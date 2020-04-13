/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}

object FloatUtils extends Arm {
  def nanToZero(cv: ColumnVector): ColumnVector = {
    if (cv.getType() != DType.FLOAT32 && cv.getType() != DType.FLOAT64) {
      throw new IllegalArgumentException("Only Floats and Doubles allowed")
    }
    withResource(cv.isNan()) { isNan =>
      withResource(if (cv.getType == DType.FLOAT64) Scalar.fromDouble(0.0d) else Scalar.fromFloat(0.0f)) { zero =>
        isNan.ifElse(zero, cv)
      }
    }
  }

  def nansToNulls(vec: ColumnVector): ColumnVector = {
    withResource(vec.isNan()) { isNan =>
      withResource(Scalar.fromNull(vec.getType)) { nullScalar =>
        isNan.ifElse(nullScalar, vec)
      }
    }
  }

  def infinityToNulls(vec: ColumnVector): ColumnVector = {
    def getInfinityVector: ColumnVector = {
      if (vec.getType == DType.FLOAT64) ColumnVector.fromDoubles(Double.PositiveInfinity, Double.NegativeInfinity)
      else ColumnVector.fromFloats(Float.PositiveInfinity, Float.NegativeInfinity)
    }

    def getNullVector: ColumnVector = {
      if (vec.getType == DType.FLOAT64) ColumnVector.fromBoxedDoubles(null, null)
      else ColumnVector.fromBoxedFloats(null, null)
    }

    withResource(getInfinityVector) { infinityVector =>
      withResource(getNullVector) { nullVector =>
        vec.findAndReplaceAll(infinityVector, nullVector)
      }
    }
  }
}
