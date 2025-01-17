/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

object HybridExecutionUtils {

  private val HYBRID_JAR_PLUGIN_CLASS_NAME = "com.nvidia.spark.rapids.hybrid.HybridPluginWrapper"

  /**
   * Check if the Hybrid jar is in the classpath,
   * report error if not
   */
  def checkHybridJarInClassPath(): Unit = {
    try {
      Class.forName(HYBRID_JAR_PLUGIN_CLASS_NAME)
    } catch {
      case e: ClassNotFoundException => throw new RuntimeException(
        "Hybrid jar is not in the classpath, Please add Hybrid jar into the class path, or " +
            "Please disable Hybrid feature by setting " +
            "spark.rapids.sql.parquet.useHybridReader=false", e)
    }
  }
}
