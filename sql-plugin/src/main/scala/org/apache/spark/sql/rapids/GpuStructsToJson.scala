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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{CastOptions, GpuCast, GpuColumnVector, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType, StructType}

case class GpuStructsToJson(
  options: Map[String, String],
  child: Expression,
  timeZoneId: Option[String] = None) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    GpuCast.castStructToJsonString(input.getBase, child.dataType.asInstanceOf[StructType].fields,
      new CastOptions(false, false, false, true))
  }

  override def dataType: DataType = StringType
}
