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

  def createCudfDecimal(dt: DecimalType): DType = {
    createCudfDecimal(dt.precision, dt.scale)
  }

  def createCudfDecimal(precision: Int, scale: Int): DType = {
    if (precision <= DType.DECIMAL32_MAX_PRECISION) {
      DType.create(DType.DTypeEnum.DECIMAL32, -scale)
    } else if (precision <= DType.DECIMAL64_MAX_PRECISION) {
      DType.create(DType.DTypeEnum.DECIMAL64, -scale)
    } else if (precision <= DType.DECIMAL128_MAX_PRECISION) {
      DType.create(DType.DTypeEnum.DECIMAL128, -scale)
    } else {
      throw new IllegalArgumentException(s"precision overflow: $precision")
    }
  }

    def getMaxPrecision(dt: DType): Int = dt.getTypeId match {
      case DType.DTypeEnum.DECIMAL32 => DType.DECIMAL32_MAX_PRECISION
      case DType.DTypeEnum.DECIMAL64 => DType.DECIMAL64_MAX_PRECISION
      case _ if dt.isDecimalType => DType.DECIMAL128_MAX_PRECISION
      case _ => throw new IllegalArgumentException(s"not a decimal type: $dt")
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
