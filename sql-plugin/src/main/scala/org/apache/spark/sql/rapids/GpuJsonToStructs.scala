/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.jni.MapUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {
  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    MapUtils.extractRawMapFromJsonString(input.getBase)
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
