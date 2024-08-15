/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests

import com.nvidia.spark.rapids.ShimReflectionUtils
import org.apache.commons.lang3.reflect.MethodUtils

/**
 * Dump all spark-rapids configs with their defaults.
 */
object DumpDefaultConfigs {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(s"Usage: ${this.getClass.getCanonicalName} {format} {output_path}")
      System.exit(1)
    }
    // We use the reflection as RapidsConf should be accessed via the shim layer.
    val clazz = ShimReflectionUtils.loadClass("com.nvidia.spark.rapids.RapidsConf")
    MethodUtils.invokeStaticMethod(clazz, "dumpConfigsWithDefault", args(0), args(1))
  }
}
