/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark311cdh

import com.nvidia.spark.rapids.{ClouderaShimVersion, ShimVersion}

object SparkShimServiceProvider {
  val VERSION = ClouderaShimVersion(3, 1, 1, "3.1.7270")
  // cdh version can have numbers after after 7270
  val CDH_BASE_VERSION = "3.1.1.3.1.7270"
}

class SparkShimServiceProvider extends com.nvidia.spark.rapids.SparkShimServiceProvider {

  override def getShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  def matchesVersion(version: String): Boolean = {
    version.contains(SparkShimServiceProvider.CDH_BASE_VERSION)
  }
}
