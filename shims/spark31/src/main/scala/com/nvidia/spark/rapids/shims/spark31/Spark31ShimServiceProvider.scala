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

package com.nvidia.spark.rapids.shims.spark31

import com.nvidia.spark.rapids.{SparkShims, SparkShimServiceProvider}

class Spark31ShimServiceProvider extends SparkShimServiceProvider {

  val SPARK31VERSIONNAME = "3.1.0-SNAPSHOT"

  def matchesVersion(version: String): Boolean = {
    version == SPARK31VERSIONNAME
  }

  def buildShim: SparkShims = {
    new Spark31Shims()
  }
}
