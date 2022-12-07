/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

// {"spark-distros":["311","312","312db","313","314"]}
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.orc.Reader

object OrcShims extends OrcShims311until320Base {

  // the ORC Reader in non CDH Spark is closeable
  def withReader[T <: AutoCloseable, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  // the ORC Reader in non CDH Spark is closeable
  def closeReader(reader: Reader): Unit = {
    if (reader != null) {
      reader.close()
    }
  }
}
