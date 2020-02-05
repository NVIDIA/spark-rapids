/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.sql.catalyst.expressions.Predicate

/*
 * IsNull and IsNotNull should eventually become CpuUnaryExpressions, with corresponding
 * UnaryOp
 */

case class GpuIsNull(child: GpuExpression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NULL)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.isNull)
}

case class GpuIsNotNull(child: GpuExpression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NOT NULL)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.isNotNull)
}

case class GpuIsNan(child: GpuExpression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NAN)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.isNan)
}
