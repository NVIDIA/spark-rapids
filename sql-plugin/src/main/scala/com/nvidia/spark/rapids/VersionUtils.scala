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

object VersionUtils {

  lazy val isSpark301OrLater: Boolean = cmpSparkVersion(3, 0, 1) >= 0

  lazy val isSpark311OrLater: Boolean = cmpSparkVersion(3, 1, 1) >= 0

  lazy val isSpark320OrLater: Boolean = cmpSparkVersion(3, 2, 0) >= 0

  lazy val isSpark: Boolean = {
    ShimLoader.getSparkShims.getSparkShimVersion.isInstanceOf[SparkShimVersion]
  }

  lazy val isDataBricks: Boolean = {
    ShimLoader.getSparkShims.getSparkShimVersion.isInstanceOf[DatabricksShimVersion]
  }

  lazy val isCloudera: Boolean = {
    ShimLoader.getSparkShims.getSparkShimVersion.isInstanceOf[ClouderaShimVersion]
  }

  lazy val isEMR: Boolean = {
    ShimLoader.getSparkShims.getSparkShimVersion.isInstanceOf[EMRShimVersion]
  }

  def cmpSparkVersion(major: Int, minor: Int, bugfix: Int): Int = {
    val sparkShimVersion = ShimLoader.getSparkShims.getSparkShimVersion
    val (sparkMajor, sparkMinor, sparkBugfix) = sparkShimVersion match {
      case SparkShimVersion(a, b, c) => (a, b, c)
      case DatabricksShimVersion(a, b, c) => (a, b, c)
      case ClouderaShimVersion(a, b, c, _) => (a, b, c)
      case EMRShimVersion(a, b, c) => (a, b, c)
    }
    val fullVersion = ((major.toLong * 1000) + minor) * 1000 + bugfix
    val sparkFullVersion = ((sparkMajor.toLong * 1000) + sparkMinor) * 1000 + sparkBugfix
    sparkFullVersion.compareTo(fullVersion)
  }
}
