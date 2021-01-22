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

package org.apache.spark.sql.execution.datasources.rapids

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, PartitionSpec}
import org.apache.spark.sql.types.StructType


object GpuPartitioningUtils {

  /**
   * Mainly derived from PartitioningAwareFileIndex.inferPartitioning
   * @param sparkSession
   * @param leafDirs leaf directory paths
   * @param basePaths Contains a set of paths that are considered as the base dirs of the
   *                  input datasets. The partitioning discovery logic will make sure it
   *                  will stop when it reaches any base path
   * @param parameters a set of options to control partition discovery
   * @param userSpecifiedSchema an optional user specified schema that will be use to provide
   *                            types for the discovered partitions
   * @return
   */
  def inferPartitioning(
      sparkSession: SparkSession,
      leafDirs: Seq[Path],
      basePaths: Set[Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType]): PartitionSpec = {

    val recursiveFileLookup = parameters.getOrElse("recursiveFileLookup", "false").toBoolean

    if (recursiveFileLookup) {
      PartitionSpec.emptySpec
    } else {
      val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
      val timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

      PartitioningUtils.parsePartitions(
        leafDirs,
        typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
        basePaths = basePaths,
        userSpecifiedSchema = userSpecifiedSchema,
        caseSensitive = sparkSession.sqlContext.conf.caseSensitiveAnalysis,
        validatePartitionColumns = sparkSession.sqlContext.conf.validatePartitionColumns,
        timeZoneId = timeZoneId)
    }
  }
}
