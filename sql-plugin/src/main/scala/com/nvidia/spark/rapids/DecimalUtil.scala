/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType

import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}

object DecimalUtil {

  def createCudfDecimal(precision: Int, scale: Int): DType = {
    if (precision <= Decimal.MAX_INT_DIGITS) {
      DType.create(DType.DTypeEnum.DECIMAL32, -scale)
    } else {
      DType.create(DType.DTypeEnum.DECIMAL64, -scale)
    }
  }

  /**
   * Return the size in bytes of the Fixed-width data types.
   * WARNING: Do not use this method for variable-width data types
   */
  private[rapids] def getDataTypeSize(dt: DataType): Int = {
    dt match {
      case d: DecimalType if d.precision <= Decimal.MAX_INT_DIGITS => 4
      case t => t.defaultSize
    }
  }
}
