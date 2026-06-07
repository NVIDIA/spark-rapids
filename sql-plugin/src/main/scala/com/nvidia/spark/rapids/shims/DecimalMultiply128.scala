/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnView, Table}
import com.nvidia.spark.rapids.VersionUtils
import com.nvidia.spark.rapids.jni.DecimalUtils._

object DecimalMultiply128 {
  private def useFinalPrecisionScaleMultiply: Boolean = {
    VersionUtils.cmpSparkVersion(4, 0, 0) >= 0 ||
      VersionUtils.cmpSparkVersion(3, 5, 1) >= 0 ||
      (VersionUtils.cmpSparkVersion(3, 4, 2) >= 0 &&
        VersionUtils.cmpSparkVersion(3, 5, 0) < 0) ||
      (VersionUtils.isDataBricks && VersionUtils.cmpSparkVersion(3, 5, 0) == 0)
  }

  def apply(castLhs: ColumnView, castRhs: ColumnView, scale: Int): Table = {
    if (useFinalPrecisionScaleMultiply) {
      multiply128(castLhs, castRhs, scale, false)
    } else {
      multiply128(castLhs, castRhs, scale)
    }
  }
}
