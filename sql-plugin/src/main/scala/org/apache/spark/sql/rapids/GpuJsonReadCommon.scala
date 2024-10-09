/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.rapids

import java.util.Locale

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar, Schema, Table}
import com.fasterxml.jackson.core.JsonParser
import com.nvidia.spark.rapids.{ColumnCastUtil, GpuCast, GpuColumnVector, GpuScalar, GpuTextBasedPartitionReader}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.CastStrings

import org.apache.spark.sql.catalyst.json.{GpuJsonUtils, JSONOptions}
import org.apache.spark.sql.rapids.shims.GpuJsonToStructsShim
import org.apache.spark.sql.types.{DataType, _}

/**
 * This is a utility method intended to provide common functionality between JsonToStructs and
 * ScanJson
 */
object GpuJsonReadCommon {
  private def populateSchema(dt: DataType,
      name: String, builder: Schema.Builder): Unit = dt match {
    case at: ArrayType =>
      val child = builder.addColumn(DType.LIST, name)
      populateSchema(at.elementType, "element", child)
    case st: StructType =>
      val child = builder.addColumn(DType.STRUCT, name)
      for (sf <- st.fields) {
        populateSchema(sf.dataType, sf.name, child)
      }
    case _: MapType =>
      throw new IllegalArgumentException("MapType is not supported yet for schema conversion")
    case _ =>
      builder.addColumn(DType.STRING, name)
  }

  /**
   * Make a read schema given an input data type
   * @param input the input Spark schema to convert
   * @return the schema to use when reading Spark data.
   */
  def makeSchema(input: StructType): Schema = {
    val builder = Schema.builder
    input.foreach(f => populateSchema(f.dataType, f.name, builder))
    builder.build
  }

  private def isQuotedString(input: ColumnView): ColumnVector = {
    withResource(Scalar.fromString("\"")) { quote =>
      withResource(input.startsWith(quote)) { sw =>
        withResource(input.endsWith(quote)) { ew =>
          sw.binaryOp(BinaryOp.LOGICAL_AND, ew, DType.BOOL8)
        }
      }
    }
  }

  private def stripFirstAndLastChar(input: ColumnView): ColumnVector = {
    withResource(Scalar.fromInt(1)) { one =>
      val end = withResource(input.getCharLengths) { cc =>
        withResource(cc.sub(one)) { endWithNulls =>
          withResource(endWithNulls.isNull) { eIsNull =>
            eIsNull.ifElse(one, endWithNulls)
          }
        }
      }
      withResource(end) { _ =>
        withResource(ColumnVector.fromScalar(one, end.getRowCount.toInt)) { start =>
          input.substring(start, end)
        }
      }
    }
  }

  private def undoKeepQuotes(input: ColumnView): ColumnVector = {
    withResource(isQuotedString(input)) { iq =>
      withResource(stripFirstAndLastChar(input)) { stripped =>
        iq.ifElse(stripped, input)
      }
    }
  }

  private def fixupQuotedStrings(input: ColumnView): ColumnVector = {
    withResource(isQuotedString(input)) { iq =>
      withResource(stripFirstAndLastChar(input)) { stripped =>
        withResource(Scalar.fromString(null)) { ns =>
          iq.ifElse(stripped, ns)
        }
      }
    }
  }

  private lazy val specialUnquotedFloats =
    Seq("NaN", "+INF", "-INF", "+Infinity", "Infinity", "-Infinity")
  private lazy val specialQuotedFloats = specialUnquotedFloats.map(s => '"'+s+'"')

  /**
   * JSON has strict rules about valid numeric formats. See https://www.json.org/ for specification.
   *
   * Spark then has its own rules for supporting NaN and Infinity, which are not
   * valid numbers in JSON.
   */
  private def sanitizeFloats(input: ColumnView, options: JSONOptions): ColumnVector = {
    // Note that this is not 100% consistent with Spark versions prior to Spark 3.3.0
    // due to https://issues.apache.org/jira/browse/SPARK-38060
    if (options.allowNonNumericNumbers) {
      // Need to normalize the quotes to non-quoted to parse properly
      withResource(ColumnVector.fromStrings(specialQuotedFloats: _*)) { quoted =>
        withResource(ColumnVector.fromStrings(specialUnquotedFloats: _*)) { unquoted =>
          input.findAndReplaceAll(quoted, unquoted)
        }
      }
    } else {
      input.copyToColumnVector()
    }
  }

  private def sanitizeInts(input: ColumnView): ColumnVector = {
    // Integer numbers cannot look like a float, so no `.` or e The rest of the parsing should
    // handle this correctly. The rest of the validation is in CUDF itself

    val tmp = withResource(Scalar.fromString(".")) { dot =>
      withResource(input.stringContains(dot)) { hasDot =>
        withResource(Scalar.fromString("e")) { e =>
          withResource(input.stringContains(e)) { hase =>
            hasDot.or(hase)
          }
        }
      }
    }
    val invalid = withResource(tmp) { _ =>
      withResource(Scalar.fromString("E")) { E =>
        withResource(input.stringContains(E)) { hasE =>
          tmp.or(hasE)
        }
      }
    }
    withResource(invalid) { _ =>
      withResource(Scalar.fromNull(DType.STRING)) { nullString =>
        invalid.ifElse(nullString, input)
      }
    }
  }

  private def sanitizeQuotedDecimalInUSLocale(input: ColumnView): ColumnVector = {
    // The US locale is kind of special in that it will remove the , and then parse the
    // input normally
    withResource(stripFirstAndLastChar(input)) { stripped =>
      withResource(Scalar.fromString(",")) { comma =>
        withResource(Scalar.fromString("")) { empty =>
          stripped.stringReplace(comma, empty)
        }
      }
    }
  }

  private def sanitizeDecimal(input: ColumnView, options: JSONOptions): ColumnVector = {
    assert(options.locale == Locale.US)
    withResource(isQuotedString(input)) { isQuoted =>
      withResource(sanitizeQuotedDecimalInUSLocale(input)) { quoted =>
        isQuoted.ifElse(quoted, input)
      }
    }
  }

  private def castStringToFloat(input: ColumnView, dt: DType,
      options: JSONOptions): ColumnVector = {
    withResource(sanitizeFloats(input, options)) { sanitizedInput =>
      CastStrings.toFloat(sanitizedInput, false, dt)
    }
  }

  private def castStringToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {
    // TODO there is a bug here around 0 https://github.com/NVIDIA/spark-rapids/issues/10898
    CastStrings.toDecimal(input, false, false, dt.precision, -dt.scale)
  }

  private def castJsonStringToBool(input: ColumnView): ColumnVector = {
    // Sadly there is no good kernel right now to do just this check/conversion
    val isTrue = withResource(Scalar.fromString("true")) { trueStr =>
      input.equalTo(trueStr)
    }
    withResource(isTrue) { _ =>
      val isFalse = withResource(Scalar.fromString("false")) { falseStr =>
        input.equalTo(falseStr)
      }
      val falseOrNull = withResource(isFalse) { _ =>
        withResource(Scalar.fromBool(false)) { falseLit =>
          withResource(Scalar.fromNull(DType.BOOL8)) { nul =>
            isFalse.ifElse(falseLit, nul)
          }
        }
      }
      withResource(falseOrNull) { _ =>
        withResource(Scalar.fromBool(true)) { trueLit =>
          isTrue.ifElse(trueLit, falseOrNull)
        }
      }
    }
  }

  private def dateFormat(options: JSONOptions): Option[String] =
    GpuJsonUtils.optionalDateFormatInRead(options)

  private def timestampFormat(options: JSONOptions): String =
    GpuJsonUtils.timestampFormatInRead(options)

  private def throwMismatchException(cv: ColumnView,
      dt: DataType): (Option[ColumnView], Seq[AutoCloseable]) = {
    throw new IllegalStateException(s"Don't know how to transform $cv to $dt for JSON")
  }

  private def nestedColumnViewMismatchTransform(cv: ColumnView,
      dt: DataType): (Option[ColumnView], Seq[AutoCloseable]) = {
    // In the future we should be able to convert strings to maps/etc, but for
    // now we are working around issues where CUDF is not returning a STRING for nested
    // types when asked for it.
    cv.getType match {
      case DType.LIST =>
        dt match {
          case ByteType | ShortType | IntegerType | LongType |
               BooleanType | FloatType | DoubleType |
               _: DecimalType | _: StructType =>
            // This is all nulls
            val rows = cv.getRowCount().toInt
            val ret = withResource(GpuScalar.from(null, dt)) { nullScalar =>
              ColumnVector.fromScalar(nullScalar, rows)
            }
            (Some(ret.asInstanceOf[ColumnView]), Seq(ret))
          case _ =>
            throwMismatchException(cv, dt)
        }
      case DType.STRUCT =>
        dt match {
          case _: ArrayType =>
            // This is all nulls
            val rows = cv.getRowCount().toInt
            val ret = withResource(GpuScalar.from(null, dt)) { nullScalar =>
              ColumnVector.fromScalar(nullScalar, rows)
            }
            (Some(ret.asInstanceOf[ColumnView]), Seq(ret))
          case _ =>
            throwMismatchException(cv, dt)
        }
      case _ =>
        throwMismatchException(cv, dt)
    }
  }

  private def convertToDesiredType(inputCv: ColumnVector,
      topLevelType: DataType,
      options: JSONOptions): ColumnVector = {
    ColumnCastUtil.deepTransform(inputCv, Some(topLevelType),
      Some(nestedColumnViewMismatchTransform)) {
      case (cv, Some(BooleanType)) if cv.getType == DType.STRING =>
        castJsonStringToBool(cv)
      case (cv, Some(DateType)) if cv.getType == DType.STRING =>
        withResource(fixupQuotedStrings(cv)) { fixed =>
          GpuJsonToStructsShim.castJsonStringToDateFromScan(fixed, DType.TIMESTAMP_DAYS,
            dateFormat(options))
        }
      case (cv, Some(TimestampType)) if cv.getType == DType.STRING =>
        withResource(fixupQuotedStrings(cv)) { fixed =>
          GpuTextBasedPartitionReader.castStringToTimestamp(fixed, timestampFormat(options),
            DType.TIMESTAMP_MICROSECONDS)
        }
      case (cv, Some(StringType)) if cv.getType == DType.STRING =>
        undoKeepQuotes(cv)
      case (cv, Some(dt: DecimalType)) if cv.getType == DType.STRING =>
        withResource(sanitizeDecimal(cv, options)) { tmp =>
          castStringToDecimal(tmp, dt)
        }
      case (cv, Some(dt)) if (dt == DoubleType || dt == FloatType) && cv.getType == DType.STRING =>
        castStringToFloat(cv,  GpuColumnVector.getNonNestedRapidsType(dt), options)
      case (cv, Some(dt))
        if (dt == ByteType || dt == ShortType || dt == IntegerType || dt == LongType ) &&
            cv.getType == DType.STRING =>
        withResource(sanitizeInts(cv)) { tmp =>
          CastStrings.toInteger(tmp, false, GpuColumnVector.getNonNestedRapidsType(dt))
        }
      case (cv, Some(dt)) if cv.getType == DType.STRING =>
        GpuCast.doCast(cv, StringType, dt)
    }
  }


  /**
   * Convert the parsed input table to the desired output types
   * @param table the table to start with
   * @param desired the desired output data types
   * @param options the options the user provided
   * @return an array of converted column vectors in the same order as the input table.
   */
  def convertTableToDesiredType(table: Table,
      desired: StructType,
      options: JSONOptions): Array[ColumnVector] = {
    val dataTypes = desired.fields.map(_.dataType)
    dataTypes.zipWithIndex.safeMap {
      case (dt, i) =>
        convertToDesiredType(table.getColumn(i), dt, options)
    }
  }

  def cudfJsonOptions(options: JSONOptions): ai.rapids.cudf.JSONOptions = {
    // This is really ugly, but options.allowUnquotedControlChars is marked as private
    // and this is the only way I know to get it without even uglier tricks
    @scala.annotation.nowarn("msg=Java enum ALLOW_UNQUOTED_CONTROL_CHARS in " +
      "Java enum Feature is deprecated")
    val allowUnquotedControlChars =
      options.buildJsonFactory()
        .isEnabled(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
    ai.rapids.cudf.JSONOptions.builder()
    .withRecoverWithNull(true)
    .withMixedTypesAsStrings(true)
    .withNormalizeWhitespace(true)
    .withKeepQuotes(true)
    .withNormalizeSingleQuotes(options.allowSingleQuotes)
    .withStrictValidation(true)
    .withLeadingZeros(options.allowNumericLeadingZeros)
    .withNonNumericNumbers(options.allowNonNumericNumbers)
    .withUnquotedControlChars(allowUnquotedControlChars)
    .withCudfPruneSchema(true)
    .withExperimental(true)
    .build()
  }
}
