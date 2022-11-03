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

package com.nvidia.spark.rapids

import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

/*
 * Utils for the alluxio configurations.
 * If alluxio feature is disabled, we can tolerate the client jar is absent.
 * Use the following pattern to avoid `NoClassDefFoundError` if alluxio feature is disabled:
 * if(AlluxioCfgUtils.enabledAlluxio* functions) {
 *     // AlluxioCfgUtils does not import any alluxio class
 *     // Only AlluxioUtils imports alluxio classes
 *     AlluxioUtils.doSomething;
 * }
 */
object AlluxioCfgUtils {
  def checkAlluxioNotSupported(rapidsConf: RapidsConf): Unit = {
    if (rapidsConf.isParquetPerFileReadEnabled &&
        (rapidsConf.getAlluxioAutoMountEnabled || rapidsConf.getAlluxioPathsToReplace.isDefined)) {
      throw new IllegalArgumentException("Alluxio is currently not supported with the PERFILE " +
          "reader, please use one of the other reader types.")
    }
  }

  def enabledAlluxio(conf: RapidsConf): Boolean =
    conf.getAlluxioAutoMountEnabled || conf.getAlluxioPathsToReplace.isDefined

  def enabledAlluxioAutoMount(rapidsConf: RapidsConf,
      fileFormat: FileFormat): Boolean = {
    rapidsConf.getAlluxioAutoMountEnabled && fileFormat.isInstanceOf[ParquetFileFormat]
  }

  def enabledAlluxioPathsToReplace(rapidsConf: RapidsConf,
      fileFormat: FileFormat): Boolean = {
    rapidsConf.getAlluxioPathsToReplace.isDefined && fileFormat.isInstanceOf[ParquetFileFormat]
  }

  def enabledReplacementMap(conf: RapidsConf): Boolean = {
    conf.getAlluxioPathsToReplace.isDefined
  }
}
