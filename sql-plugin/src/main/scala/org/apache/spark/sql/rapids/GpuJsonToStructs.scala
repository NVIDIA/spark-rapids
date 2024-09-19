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
import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ColumnVector, Cuda, DataSource, DeviceMemoryBuffer, HostMemoryBuffer, TableDebug}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar, GpuUnaryExpression, HostAlloc}
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


  private lazy val parsedOptions = new JSONOptions(
    options,
    timeZoneId.get,
    SQLConf.get.columnNameOfCorruptRecord)


  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    schema match {
      case _: MapType =>
        JSONUtils.extractRawMapFromJsonString(input.getBase)
      case struct: StructType => {
        // if we ever need to support duplicate keys we need to keep track of the duplicates
        //  and make the first one null, but I don't think this will ever happen in practice
        val cudfSchema = makeSchema(struct)

        //       System.out.println("GpuJsonToStructs: cudfSchema.getFlattenedTypes does not contain LIST")
        val table = JSONUtils.fromJsonToStructs(input.getBase, cudfSchema,
          parsedOptions.allowNumericLeadingZeros, parsedOptions.allowNonNumericNumbers)
        TableDebug.get.debug("input.getBase", input.getBase)
        TableDebug.get.debug("table from json", table)


        val convertedStructs =
          withResource(table) { _ =>
            withResource(convertTableToDesiredType(table, struct, parsedOptions,
              removeQuotes = false)) {
              columns => cudf.ColumnVector.makeStruct(columns: _*)
            }
          }

        //          TableDebug.get.debug("convertedStructs", convertedStructs)

        withResource(convertedStructs) { converted =>
          val stripped = if (input.getBase.getData == null) {
            input.getBase.incRefCount
          } else {
            withResource(cudf.Scalar.fromString(" ")) { space =>
              input.getBase.strip(space)
            }
          }

          withResource(stripped) { stripped =>
            val isEmpty = withResource(stripped.getByteCount) { lengths =>
              withResource(cudf.Scalar.fromInt(0)) { zero =>
                lengths.lessOrEqualTo(zero)
              }
            }
            val isNullOrEmpty = withResource(isEmpty) { _ =>
              withResource(input.getBase.isNull) { isNull =>
                isNull.binaryOp(cudf.BinaryOp.NULL_LOGICAL_OR, isEmpty, cudf.DType.BOOL8)
              }
            }
            withResource(isNullOrEmpty) { nullOrEmpty =>
              withResource(GpuScalar.from(null, struct)) { nullVal =>
                val out = nullOrEmpty.ifElse(nullVal, converted)
                TableDebug.get.debug("out from json", out)
                out
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
