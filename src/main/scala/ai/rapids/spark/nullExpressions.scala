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

import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull, IsNull}

/*
 * IsNull and IsNotNull should eventually become CpuUnaryExpressions, with corresponding
 * UnaryOp
 */

class GpuIsNull(child: Expression) extends IsNull(child) with GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    var tmp = input.getBase.isNull
    try {
      val ret = tmp
      tmp = null
      GpuColumnVector.from(ret)
    } finally {
      if (tmp != null) {
        tmp.close()
      }
    }
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuIsNull]
  }
}

class GpuIsNotNull(child: Expression) extends IsNotNull(child) with GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    var tmp = input.getBase.isNotNull
    try {
      val ret = tmp
      tmp = null
      GpuColumnVector.from(ret)
    } finally {
      if (tmp != null) {
        tmp.close()
      }
    }
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuIsNotNull]
  }
}
