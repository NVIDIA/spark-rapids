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
import ai.rapids.cudf.{ColumnView, Cuda, DataSource, DeviceMemoryBuffer, HostMemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression, HostAlloc}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.JSONUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 *  Exception thrown when cudf cannot parse the JSON data because some Json to Struct cases are not
 *  currently supported.
 */
class JsonParsingException(s: String, cause: Throwable) extends RuntimeException(s, cause) {}

class JsonDeviceDataSource(data: DeviceMemoryBuffer) extends DataSource {
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
}

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {
  import GpuJsonReadCommon._

  private lazy val parsedOptions = new JSONOptions(
    options,
    timeZoneId.get,
    SQLConf.get.columnNameOfCorruptRecord)

  private lazy val jsonOptionBuilder =
    GpuJsonReadCommon.cudfJsonOptionBuilder(parsedOptions)

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(new NvtxRange("GpuJsonToStructs", NvtxColor.YELLOW)) { _ =>
      schema match {
        case _: MapType => JSONUtils.extractRawMapFromJsonString(input.getBase)
        case struct: StructType =>
          // if we ever need to support duplicate keys we need to keep track of the duplicates
          //  and make the first one null, but I don't think this will ever happen in practice
          val cudfSchema = makeSchema(struct)

          // We cannot handle all corner cases with this right now. The parser just isn't
          // good enough, but we will try to handle a few common ones.
          val numRows = input.getRowCount.toInt

          // Step 1: Concat the data into a single buffer, with verifying nulls/empty strings
          val concatenated = JSONUtils.concatenateJsonStrings(input.getBase)
          withResource(concatenated) { _ =>
            // Step 2: Setup a datasource from the concatenated JSON strings
            val table = withResource(new JsonDeviceDataSource(concatenated.data)) { ds =>
              withResource(new NvtxRange("Table.readJSON", NvtxColor.RED)) { _ =>
                // Step 3: Have cudf parse the JSON data
                try {
                  cudf.Table.readJSON(cudfSchema,
                    jsonOptionBuilder.withLineDelimiter(concatenated.delimiter).build(),
                    ds,
                    numRows)
                } catch {
                  case e: RuntimeException =>
                    throw new JsonParsingException("Currently some JsonToStructs cases " +
                      "are not supported. " +
                      "Consider to set spark.rapids.sql.expression.JsonToStructs=false", e)
                }
              }
            }

            withResource(table) { _ =>
              // Step 4: Verify that the data looks correct
              if (table.getRowCount != numRows) {
                throw new IllegalStateException("The input data didn't parse correctly and " +
                  s"we read a different number of rows than was expected. Expected $numRows, " +
                  s"but got ${table.getRowCount}")
              }

              // Step 5: Convert the read table into columns of desired types.
              withResource(convertTableToDesiredType(table, struct, parsedOptions)) { columns =>
                // Step 6: Turn the data into structs.
                JSONUtils.makeStructs(columns.asInstanceOf[Array[ColumnView]],
                  concatenated.isNullOrEmpty)
              }
            }
          }
        case _ => throw new IllegalArgumentException(
          s"GpuJsonToStructs currently does not support schema of type $schema.")
      }
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
