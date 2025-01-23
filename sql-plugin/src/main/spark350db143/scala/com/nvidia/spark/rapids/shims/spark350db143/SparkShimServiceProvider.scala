/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.spark350db143

import com.nvidia.spark.rapids._

import org.apache.spark.SparkEnv

object SparkShimServiceProvider {
  val VERSION = DatabricksShimVersion(3, 5, 0, "14.3")
}

class SparkShimServiceProvider extends com.nvidia.spark.rapids.SparkShimServiceProvider {

  override def getShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  def matchesVersion(version: String): Boolean = {
    val shimEnabledProp = "spark.rapids.shims.spark350db143" + ".enabled"
    // disabled by default
    val shimEnabled = Option(SparkEnv.get)
      .flatMap(_.conf.getOption(shimEnabledProp).map(_.toBoolean))
      .getOrElse(false)

    DatabricksShimServiceProvider.matchesVersion(
      dbrVersion = "14.3.x",
      shimMatchEnabled = shimEnabled,
      // scalastyle:off line.size.limit
      disclaimer = s"""|!!!! Databricks 14.3.x support is incomplete: https://github.com/NVIDIA/spark-rapids/issues/10661
                       |!!!! It can be experimentally enabled by configuring ${shimEnabledProp}=true.""".stripMargin
      // scalastyle:on line.size.limit
    )
  }
}
