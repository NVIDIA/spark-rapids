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

  /**
   * Returns whether alluxio convert time algorithm should be enabled
   * Note: should also check whether the auto-mount or replace path is enabled.
   *
   * @param conf the rapids conf
   * @return Returns whether alluxio convert time algorithm should be enabled
   */
  def enabledAlluxioReplacementAlgoConvertTime(conf: RapidsConf): Boolean = {
    conf.isAlluxioReplacementAlgoConvertTime &&
        (conf.getAlluxioAutoMountEnabled || conf.getAlluxioPathsToReplace.isDefined)
  }

  def enabledAlluxioReplacementAlgoTaskTime(conf: RapidsConf): Boolean = {
    conf.isAlluxioReplacementAlgoTaskTime &&
        (conf.getAlluxioAutoMountEnabled || conf.getAlluxioPathsToReplace.isDefined)
  }

  def isAlluxioAutoMountTaskTime(rapidsConf: RapidsConf,
      fileFormat: FileFormat): Boolean = {
    rapidsConf.getAlluxioAutoMountEnabled && rapidsConf.isAlluxioReplacementAlgoTaskTime &&
        fileFormat.isInstanceOf[ParquetFileFormat]
  }

  def isAlluxioPathsToReplaceTaskTime(rapidsConf: RapidsConf,
      fileFormat: FileFormat): Boolean = {
    rapidsConf.getAlluxioPathsToReplace.isDefined && rapidsConf.isAlluxioReplacementAlgoTaskTime &&
        fileFormat.isInstanceOf[ParquetFileFormat]
  }

  def isConfiguredReplacementMap(conf: RapidsConf): Boolean = {
    conf.getAlluxioPathsToReplace.isDefined
  }
}
