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

package org.apache.spark.sql.rapids.tool.profiling

import org.apache.spark.internal.config

object ToolUtils {

  def isGPUMode(properties: collection.mutable.Map[String, String]): Boolean = {
    (properties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin")
        && properties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean)
  }
}
