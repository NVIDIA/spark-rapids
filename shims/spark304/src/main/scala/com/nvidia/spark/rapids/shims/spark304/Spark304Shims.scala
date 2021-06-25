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

package com.nvidia.spark.rapids.shims.spark304

import com.nvidia.spark.rapids.ShimVersion
import com.nvidia.spark.rapids.shims.spark303.Spark303Shims
import com.nvidia.spark.rapids.spark303.RapidsShuffleManager

class Spark304Shims extends Spark303Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }
}
