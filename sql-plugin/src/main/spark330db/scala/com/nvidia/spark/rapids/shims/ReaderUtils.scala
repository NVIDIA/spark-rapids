/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging

object ReaderUtils extends Logging {

  private lazy val isUnityCatalogEnabled = com.databricks.unity.UnityConf.isEnabled

  /*
   * Databricks has the Unity Catalog that allows accessing files across multiple metastores and
   * catalogs. When our readers run in different threads, the credentials don't get setup
   * properly. Here we get the Hadoop configuration associated specifically with that file which
   * seems to contain the necessary credentials. This conf will be used when creating the
   * Hadoop Filesystem, which with Unity ends up being a special Credentials file system.
   */
  def getHadoopConfForReaderThread(filePath: Path, conf: Configuration): Configuration = {
    if (isUnityCatalogEnabled) {
      try {
        com.databricks.unity.ClusterDefaultSAM.createDelegateHadoopConf(filePath, conf)
      } catch {
        case a: AssertionError =>
          // ignore this and just return the regular conf, it might be a filesystem not supported
          // and I don't have a good way to check this
          logWarning("Assertion error calling createDelegateHadoopConf, skipping.", a)
          conf
      }
    } else {
      conf
    }
  }
}
