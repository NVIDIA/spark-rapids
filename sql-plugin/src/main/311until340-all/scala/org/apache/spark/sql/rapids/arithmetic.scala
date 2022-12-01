/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType
  // arithmetic operations can overflow and throw exceptions in ANSI mode
  override def hasSideEffects: Boolean = super.hasSideEffects || SQLConf.get.ansiEnabled
}

case class GpuAdd(
    left: Expression,
    right: Expression,
    failOnError: Boolean) extends GpuAddParent(left, right, failOnError)

case class GpuSubtract(
    left: Expression,
    right: Expression,
    failOnError: Boolean) extends GpuSubtractParent(left, right, failOnError)

case class GpuIntegralDivide(
    left: Expression,
    right: Expression) extends GpuIntegralDivideParent(left, right)

case class GpuRemainder(left: Expression, right: Expression) extends GpuRemainderParent(left, right)

case class GpuPmod(left: Expression, right: Expression) extends GpuPmodParent(left, right)