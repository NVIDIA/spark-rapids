/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import scala.util.Try

object DatabricksShimServiceProvider {
  val log = org.slf4j.LoggerFactory.getLogger(getClass().getName().stripSuffix("$"))
  def matchesVersion(dbrVersion: String): Boolean = {
    Try {
      val sparkBuildInfo = org.apache.spark.BuildInfo
      val databricksBuildInfo = com.databricks.BuildInfo
      val matchRes = sparkBuildInfo.dbrVersion.startsWith(dbrVersion)
      val matchStatus = if (matchRes) "SUCCESS" else "FAILURE"
      log.warn(s"""Databricks Runtime Build Info match: {}
                  |\tDBR_VERSION: {}
                  |\tspark.gitHash: {}
                  |\tdatabricks.gitHash: {}""".stripMargin,
        matchStatus,
        sparkBuildInfo.dbrVersion,
        sparkBuildInfo.gitHash,
        databricksBuildInfo.gitHash)
      matchRes
    }.recover {
      case x: Throwable =>
        log.warn("Databricks detection failed: " + x, x)
        false
    }.getOrElse(false)
  }
}
