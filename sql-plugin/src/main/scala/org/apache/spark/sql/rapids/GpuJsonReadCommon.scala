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
//import ai.rapids.cudf.{ColumnVector, ColumnView, DType, NvtxColor, NvtxRange, Scalar, Schema, Table, TableDebug}
import ai.rapids.cudf.{ColumnVector, ColumnView, DType, NvtxColor, NvtxRange, Schema, Table}
import com.fasterxml.jackson.core.JsonParser
import com.nvidia.spark.rapids.{ColumnCastUtil, GpuColumnVector, GpuScalar, GpuTextBasedPartitionReader}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.{JSONUtils}

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

      //
      // DONE
      case (cv, Some(BooleanType)) if cv.getType == DType.STRING =>
        JSONUtils.castStringsToBooleans(cv)
      //
      //

      case (cv, Some(DateType)) if cv.getType == DType.STRING =>
        withResource(JSONUtils.removeQuotes(cv, true)) { fixed =>
          GpuJsonToStructsShim.castJsonStringToDateFromScan(fixed, DType.TIMESTAMP_DAYS,
            dateFormat(options))
        }
      case (cv, Some(TimestampType)) if cv.getType == DType.STRING =>
        withResource(JSONUtils.removeQuotes(cv, true)) { fixed =>
          GpuTextBasedPartitionReader.castStringToTimestamp(fixed, timestampFormat(options),
            DType.TIMESTAMP_MICROSECONDS)
        }

      //
      // Done
      case (cv, Some(StringType)) if cv.getType == DType.STRING =>
        JSONUtils.removeQuotes(cv, false)
      //
      //

      //
      // Done
      case (cv, Some(dt: DecimalType)) if cv.getType == DType.STRING =>
        JSONUtils.castStringsToDecimals(cv, GpuColumnVector.getNonNestedRapidsType(dt),
          dt.precision, -dt.scale, options.locale == Locale.US)
      //
      //

      //
      // DONE
      case (cv, Some(dt)) if (dt == DoubleType || dt == FloatType) && cv.getType == DType.STRING =>
        JSONUtils.castStringsToFloats(cv,  GpuColumnVector.getNonNestedRapidsType(dt),
          options.allowNonNumericNumbers)
      //
      //

      //
      // DONE
      case (cv, Some(dt))
        if (dt == ByteType || dt == ShortType || dt == IntegerType || dt == LongType ) &&
            cv.getType == DType.STRING =>
        JSONUtils.castStringsToIntegers(cv, GpuColumnVector.getNonNestedRapidsType(dt))
      //
      //

      case (cv, Some(dt)) if cv.getType == DType.STRING =>
        throw new JsonParsingException(s"Cannot convert string to $dt", null)
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
    withResource(new NvtxRange("convertTableToDesiredType", NvtxColor.RED)) { _ =>
      val dataTypes = desired.fields.map(_.dataType)
      dataTypes.zipWithIndex.safeMap {
        case (dt, i) =>
          convertToDesiredType(table.getColumn(i), dt, options)
      }
    }
  }

  def cudfJsonOptions(options: JSONOptions): ai.rapids.cudf.JSONOptions =
    cudfJsonOptionBuilder(options).build()

  def cudfJsonOptionBuilder(options: JSONOptions): ai.rapids.cudf.JSONOptions.Builder = {
    // This is really ugly, but options.allowUnquotedControlChars is marked as private
    // and this is the only way I know to get it without even uglier tricks
    @scala.annotation.nowarn("msg=Java enum ALLOW_UNQUOTED_CONTROL_CHARS in " +
      "Java enum Feature is deprecated")
    val allowUnquotedControlChars = options.buildJsonFactory()
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
  }
}
