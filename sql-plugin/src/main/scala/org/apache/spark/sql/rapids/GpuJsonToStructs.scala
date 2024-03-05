/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{ColumnVector, ColumnView, Cuda, DataSource, DeviceMemoryBuffer, DType, HostMemoryBuffer, Scalar, Schema}
import com.nvidia.spark.rapids.{ColumnCastUtil, GpuCast, GpuColumnVector, GpuScalar, GpuUnaryExpression, HostAlloc}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.MapUtils
import com.nvidia.spark.rapids.shims.GpuJsonToStructsShim
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.types._

class JsonDeviceDataSource(combined: ColumnVector) extends DataSource {
  lazy val data = combined.getData
  lazy val totalSize = data.getLength
  override def size(): Long = totalSize

  override def hostRead(offset: Long, length: Long): HostMemoryBuffer = {
    val realLength = math.min(totalSize - offset, length)
    withResource(data.slice(offset, realLength)) { sliced =>
      closeOnExcept(HostAlloc.alloc(realLength)) { hostMemoryBuffer =>
        hostMemoryBuffer.copyFromDeviceBuffer(sliced.asInstanceOf[DeviceMemoryBuffer])
        hostMemoryBuffer
      }
    }
  }

  override def hostRead(offset: Long, hostMemoryBuffer: HostMemoryBuffer): Long = {
    val length = math.min(totalSize - offset, hostMemoryBuffer.getLength)
    withResource(data.slice(offset, length)) { sliced =>
      hostMemoryBuffer.copyFromDeviceBuffer(sliced.asInstanceOf[DeviceMemoryBuffer])
    }
    length
  }

  override def supportsDeviceRead = true

  override def deviceRead(offset: Long, dest: DeviceMemoryBuffer, stream: Cuda.Stream): Long = {
    val length = math.min(totalSize - offset, dest.getLength)
    dest.copyFromDeviceBufferAsync(0, data, offset, length, stream)
    length
  }

  override def close(): Unit = {
    combined.close()
    super.close()
  }
}

object GpuJsonToStructs {
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

  def makeSchema(input: StructType): Schema = {
    val builder = Schema.builder
    input.foreach(f => populateSchema(f.dataType, f.name, builder))
    builder.build
  }

  private def castJsonStringToBool(input: ColumnView): ColumnVector = {
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

  private def isQuotedString(input: ColumnView): ColumnVector = {
    // TODO make this a custom kernel if we need it someplace else
    withResource(Scalar.fromString("\"")) { quote =>
      withResource(input.startsWith(quote)) { sw =>
        withResource(input.endsWith(quote)) { ew =>
          sw.binaryOp(cudf.BinaryOp.LOGICAL_AND, ew, cudf.DType.BOOL8)
        }
      }
    }
  }

  private def stripFirstAndLastChar(input: ColumnView): ColumnVector = {
    // TODO make this a custom kernel
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
    // TODO make this go away once we have decimal parsing doing the right thing for
    //  both cases
    withResource(isQuotedString(input)) { iq =>
      withResource(stripFirstAndLastChar(input)) { stripped =>
        iq.ifElse(stripped, input)
      }
    }
  }

  private def fixupQuotedStrings(input: ColumnView): ColumnVector = {
    // TODO make this a custom kernel
    withResource(isQuotedString(input)) { iq =>
      withResource(stripFirstAndLastChar(input)) { stripped =>
        withResource(Scalar.fromString(null)) { ns =>
          iq.ifElse(stripped, ns)
        }
      }
    }
  }

  private def convertToDesiredType(inputCv: ColumnVector,
      topLevelType: DataType,
      options: Map[String, String]): ColumnVector = {
    ColumnCastUtil.deepTransform(inputCv, Some(topLevelType)) {
      case (cv, Some(BooleanType)) if cv.getType == DType.STRING =>
        castJsonStringToBool(cv)
      case (cv, Some(DateType)) if cv.getType == DType.STRING =>
        withResource(fixupQuotedStrings(cv)) { fixed =>
          GpuJsonToStructsShim.castJsonStringToDate(fixed, options)
        }
      case (cv, Some(TimestampType)) if cv.getType == DType.STRING =>
        withResource(fixupQuotedStrings(cv)) { fixed =>
          GpuJsonToStructsShim.castJsonStringToTimestamp(fixed, options)
        }
      case (cv, Some(StringType)) if cv.getType == DType.STRING =>
        undoKeepQuotes(cv)
      case (cv, Some(dt: DecimalType)) if cv.getType == DType.STRING =>
        // This is not actually correct, but there are other follow on issues to fix this
        withResource(undoKeepQuotes(cv)) { undone =>
          GpuCast.doCast(undone, StringType, dt)
        }
      case (cv, Some(FloatType)) if cv.getType == DType.STRING =>
        // This is not actually correct, but there are other follow on issues to fix this
        withResource(undoKeepQuotes(cv)) { undone =>
          GpuCast.doCast(cv, StringType, FloatType)
        }
      case (cv, Some(DoubleType)) if cv.getType == DType.STRING =>
        // This is not actually correct, but there are other follow on issues to fix this
        withResource(undoKeepQuotes(cv)) { undone =>
          GpuCast.doCast(cv, StringType, DoubleType)
        }
      case(cv, Some(dt)) if cv.getType == DType.STRING =>
        GpuCast.doCast(cv, StringType, dt)
    }
  }

  def convertTableToDesiredType(table: cudf.Table,
      desired: StructType,
      options: Map[String, String]): Array[ColumnVector] = {
    val dataTypes = desired.fields.map(_.dataType)
    dataTypes.zipWithIndex.safeMap {
      case (dt, i) =>
        convertToDesiredType(table.getColumn(i), dt, options)
    }
  }
}

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    enableMixedTypesAsString: Boolean,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {
  import GpuJsonToStructs._

  lazy val emptyRowStr = constructEmptyRow(schema)

  private def constructEmptyRow(schema: DataType): String = {
    schema match {
      case struct: StructType if struct.fields.nonEmpty =>
        s"""{"${StringEscapeUtils.escapeJson(struct.head.name)}":null}"""
      case other =>
        throw new IllegalArgumentException(s"$other is not supported as a top level type")    }
  }

  private def cleanAndConcat(input: cudf.ColumnVector): (cudf.ColumnVector, cudf.ColumnVector) = {
    val stripped = if (input.getData == null) {
      input.incRefCount
    } else {
      withResource(cudf.Scalar.fromString(" ")) { space =>
        input.strip(space)
      }
    }

    withResource(stripped) { stripped =>
      val isEmpty = withResource(stripped.getByteCount) { lengths =>
        withResource(cudf.Scalar.fromInt(0)) { zero =>
          lengths.lessOrEqualTo(zero)
        }
      }
      val isNullOrEmptyInput = withResource(isEmpty) { _ =>
        withResource(input.isNull) { isNull =>
          isNull.binaryOp(cudf.BinaryOp.NULL_LOGICAL_OR, isEmpty, cudf.DType.BOOL8)
        }
      }
      closeOnExcept(isNullOrEmptyInput) { _ =>
        withResource(cudf.Scalar.fromString(emptyRowStr)) { emptyRow =>
          // TODO is it worth checking if any are empty or null and then skipping this?
          withResource(isNullOrEmptyInput.ifElse(emptyRow, stripped)) { nullsReplaced =>
            val isLiteralNull = withResource(Scalar.fromString("null")) { literalNull =>
              nullsReplaced.equalTo(literalNull)
            }
            withResource(isLiteralNull) { _ =>
              withResource(isLiteralNull.ifElse(emptyRow, nullsReplaced)) { cleaned =>
                checkForNewline(cleaned, "\n", "line separator")
                checkForNewline(cleaned, "\r", "carriage return")

                // add a newline to each JSON line
                val withNewline = withResource(cudf.Scalar.fromString("\n")) { lineSep =>
                  withResource(ColumnVector.fromScalar(lineSep, cleaned.getRowCount.toInt)) {
                    newLineCol =>
                      ColumnVector.stringConcatenate(Array[ColumnView](cleaned, newLineCol))
                  }
                }

                // We technically don't need to join the strings together as we just want the buffer
                // which should be the same either way.
                (isNullOrEmptyInput, withNewline)
              }
            }
          }
        }
      }
    }
  }

  private def checkForNewline(cleaned: ColumnVector, newlineStr: String, name: String): Unit = {
    withResource(cudf.Scalar.fromString(newlineStr)) { newline =>
      withResource(cleaned.stringContains(newline)) { hasNewline =>
        withResource(hasNewline.any()) { anyNewline =>
          if (anyNewline.isValid && anyNewline.getBoolean) {
            throw new IllegalArgumentException(
              s"We cannot currently support parsing JSON that contains a $name in it")
          }
        }
      }
    }
  }

  private lazy val jsonOptions = {
    val parsedOptions = new JSONOptions(
      options,
      timeZoneId.get,
      "")
    cudf.JSONOptions.builder()
        .withRecoverWithNull(true)
        .withMixedTypesAsStrings(enableMixedTypesAsString)
        .withKeepQuotes(true)
        .withNormalizeSingleQuotes(parsedOptions.allowSingleQuotes)
        .build()
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    schema match {
      case _: MapType =>
        MapUtils.extractRawMapFromJsonString(input.getBase)
      case struct: StructType => {
        // if we ever need to support duplicate keys we need to keep track of the duplicates
        //  and make the first one null, but I don't think this will ever happen in practice
        val cudfSchema = makeSchema(struct)

        // We cannot handle all corner cases with this right now. The parser just isn't
        // good enough, but we will try to handle a few common ones.
        val numRows = input.getRowCount.toInt

        // Step 1: verify and preprocess the data to clean it up and normalize a few things
        // Step 2: Concat the data into a single buffer
        val (isNullOrEmpty, combined) = cleanAndConcat(input.getBase)
        withResource(isNullOrEmpty) { isNullOrEmpty =>
          // Step 3: setup a datasource
          val table = withResource(new JsonDeviceDataSource(combined)) { ds =>
            // Step 4: Have cudf parse the JSON data
            cudf.Table.readJSON(cudfSchema, jsonOptions, ds)
          }

          // process duplicated field names in input struct schema

          withResource(table) { _ =>
            // Step 5: verify that the data looks correct
            if (table.getRowCount != numRows) {
              throw new IllegalStateException("The input data didn't parse correctly and we read " +
                  s"a different number of rows than was expected. Expected $numRows, " +
                  s"but got ${table.getRowCount}")
            }

            // Step 7: turn the data into a Struct
            withResource(convertTableToDesiredType(table, struct, options)) { columns =>
              withResource(cudf.ColumnVector.makeStruct(columns: _*)) { structData =>
                // Step 8: put nulls back in for nulls and empty strings
                withResource(GpuScalar.from(null, struct)) { nullVal =>
                  isNullOrEmpty.ifElse(nullVal, structData)
                }
              }
            }
          }
        }
      }
      case _ => throw new IllegalArgumentException(
        s"GpuJsonToStructs currently does not support schema of type $schema.")
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
