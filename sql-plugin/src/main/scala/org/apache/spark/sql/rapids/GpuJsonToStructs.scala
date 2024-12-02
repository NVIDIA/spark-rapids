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

import java.util.Locale

import ai.rapids.cudf
import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.JSONUtils
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerantShim {
  import GpuJsonReadCommon._

  private lazy val parsedOptions = new JSONOptions(
    options,
    timeZoneId.get,
    SQLConf.get.columnNameOfCorruptRecord)

  private lazy val cudfOptions = GpuJsonReadCommon.cudfJsonOptions(parsedOptions)

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(new NvtxRange("GpuJsonToStructs", NvtxColor.YELLOW)) { _ =>
      schema match {
        case _: MapType => JSONUtils.extractRawMapFromJsonString(input.getBase, cudfOptions)
        case struct: StructType =>
          val parsedStructs = JSONUtils.fromJSONToStructs(input.getBase, makeSchema(struct),
            cudfOptions, parsedOptions.locale == Locale.US)
          val hasDateTime = TrampolineUtil.dataTypeExistsRecursively(struct, t =>
            t.isInstanceOf[DateType] || t.isInstanceOf[TimestampType]
          )
          if (hasDateTime) {
            withResource(parsedStructs) { _ =>
              convertDateTimeType(parsedStructs, struct, parsedOptions)
            }
          } else {
            parsedStructs
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
