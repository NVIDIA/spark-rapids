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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, PartitionSpec}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex.BASE_PATH_PARAM
import org.apache.spark.sql.types.StructType

object GpuPartitioningUtils {

  /**
   *
   * @param sparkSession
   * @param rootPaths the list of root input paths from which the catalog will get files
   * @param leafFiles leaf file paths
   * @param parameters a set of options to control partition discovery
   * @param userSpecifiedSchema an optional user specified schema that will be use to provide
   *                            types for the discovered partitions
   * @param replaceFunc the alluxio replace function
   * @return the specification of the partitions inferred from the data
   *
   * Mainly copied from PartitioningAwareFileIndex.inferPartitioning
   */
  def inferPartitioning(
      sparkSession: SparkSession,
      rootPaths: Seq[Path],
      leafFiles: Seq[Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      replaceFunc: Path => Path): PartitionSpec = {

    val recursiveFileLookup = parameters.getOrElse("recursiveFileLookup", "false").toBoolean

    if (recursiveFileLookup) {
      PartitionSpec.emptySpec
    } else {
      val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
      val timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

      // filter out non-data path and get unique leaf dirs of inputFiles
      val leafDirs: Seq[Path] = leafFiles.filter(isDataPath).map(_.getParent).distinct

      val basePathOption = parameters.get(BASE_PATH_PARAM).map(file => {
        // need to replace the base path
        replaceFunc(new Path(file))
      })

      val basePaths = getBasePaths(sparkSession.sessionState.newHadoopConfWithOptions(parameters),
        basePathOption, rootPaths, leafFiles)

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

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  // Copied from PartitioningAwareFileIndex.isDataPath
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }

  /**
   * Contains a set of paths that are considered as the base dirs of the input datasets.
   * The partitioning discovery logic will make sure it will stop when it reaches any
   * base path.
   *
   * By default, the paths of the dataset provided by users will be base paths.
   * Below are three typical examples,
   * Case 1) `spark.read.parquet("/path/something=true/")`: the base path will be
   * `/path/something=true/`, and the returned DataFrame will not contain a column of `something`.
   * Case 2) `spark.read.parquet("/path/something=true/a.parquet")`: the base path will be
   * still `/path/something=true/`, and the returned DataFrame will also not contain a column of
   * `something`.
   * Case 3) `spark.read.parquet("/path/")`: the base path will be `/path/`, and the returned
   * DataFrame will have the column of `something`.
   *
   * Users also can override the basePath by setting `basePath` in the options to pass the new base
   * path to the data source.
   * For example, `spark.read.option("basePath", "/path/").parquet("/path/something=true/")`,
   * and the returned DataFrame will have the column of `something`.
   *
   * mainly copied from PartitioningAwareFileIndex.basePaths
   */
  private def getBasePaths(
    hadoopConf: Configuration,
    basePathOption: Option[Path],
    rootPaths: Seq[Path],
    leafFiles: Seq[Path]): Set[Path] = {

    basePathOption match {
      case Some(userDefinedBasePath) =>
        val fs = userDefinedBasePath.getFileSystem(hadoopConf)
        if (!fs.isDirectory(userDefinedBasePath)) {
          throw new IllegalArgumentException(s"Option '$BASE_PATH_PARAM' must be a directory")
        }
        val qualifiedBasePath = fs.makeQualified(userDefinedBasePath)
        val qualifiedBasePathStr = qualifiedBasePath.toString
        rootPaths
          .find(!fs.makeQualified(_).toString.startsWith(qualifiedBasePathStr))
          .foreach { rp =>
            throw new IllegalArgumentException(
              s"Wrong basePath $userDefinedBasePath for the root path: $rp")
          }
        Set(qualifiedBasePath)

      case None =>
        rootPaths.map { path =>
          // Make the path qualified (consistent with listLeafFiles and bulkListLeafFiles).
          val qualifiedPath = path.getFileSystem(hadoopConf).makeQualified(path)
          if (leafFiles.contains(qualifiedPath)) qualifiedPath.getParent else qualifiedPath }.toSet
    }
  }

}
