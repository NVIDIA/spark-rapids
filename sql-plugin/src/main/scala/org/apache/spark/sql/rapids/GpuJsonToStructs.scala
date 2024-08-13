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
import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ColumnVector, ColumnView, Cuda, DataSource, DeviceMemoryBuffer, HostMemoryBuffer, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar, GpuUnaryExpression, HostAlloc}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.MapUtils
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 *  Exception thrown when cudf cannot parse the JSON data because some Json to Struct cases are not
 *  currently supported.
 */
class JsonParsingException(s: String, cause: Throwable) extends RuntimeException(s, cause) {}

class JsonDeviceDataSource(combined: ColumnVector) extends DataSource {
  lazy val data: BaseDeviceMemoryBuffer = combined.getData
  lazy val totalSize: Long = data.getLength
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

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {
  import GpuJsonReadCommon._

  private lazy val emptyRowStr = constructEmptyRow(schema)

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

  private lazy val parsedOptions = new JSONOptions(
    options,
    timeZoneId.get,
    SQLConf.get.columnNameOfCorruptRecord)

  private lazy val jsonOptions =
    GpuJsonReadCommon.cudfJsonOptions(parsedOptions)

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
            try {
              cudf.Table.readJSON(cudfSchema, jsonOptions, ds)
            } catch {
              case e : RuntimeException =>
                throw new JsonParsingException("Currently some Json to Struct cases " +
                  "are not supported. Consider to set spark.rapids.sql.expression.JsonToStructs" +
                  "=false", e)
            }
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
            withResource(convertTableToDesiredType(table, struct, parsedOptions)) { columns =>
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
