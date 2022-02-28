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

package com.nvidia.spark.rapids.shims.spark312db

import com.nvidia.spark.rapids.{DatabricksShimVersion, ShimVersion}

import org.apache.spark.SparkEnv

object SparkShimServiceProvider {
  val VERSION = DatabricksShimVersion(3, 1, 2)
}

class SparkShimServiceProvider extends com.nvidia.spark.rapids.SparkShimServiceProvider {

  override def getShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  def matchesVersion(version: String): Boolean = {
    SparkEnv.get.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "").startsWith("9.1.")
  }
}
