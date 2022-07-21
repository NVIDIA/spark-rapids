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

import java.lang.{Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.time.ZoneId

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.{unescapePathName, DEFAULT_PARTITION_NAME}
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{PartitionPath, PartitionSpec}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex.BASE_PATH_PARAM
import org.apache.spark.sql.execution.datasources.PartitioningUtils.{parsePartition, resolvePartitions, timestampPartitionPattern, PartitionValues, TypedPartValue}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String




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

      parsePartitions(
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
        if (!fs.getFileStatus(userDefinedBasePath).isDirectory) {
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

  // To fix issue: https://github.com/NVIDIA/spark-rapids/issues/6026 and
  // https://issues.apache.org/jira/browse/SPARK-39012
  // Port latest Spark code into spark-rapids
  def castPartValueToDesiredType(
      desiredType: DataType,
      value: String,
      zoneId: ZoneId): Any = desiredType match {
    case _ if value == DEFAULT_PARTITION_NAME => null
    case NullType => null
    case StringType => UTF8String.fromString(unescapePathName(value))
    case ByteType | ShortType | IntegerType => Integer.parseInt(value)
    case LongType => JLong.parseLong(value)
    case FloatType | DoubleType => JDouble.parseDouble(value)
    case _: DecimalType => Literal(new JBigDecimal(value)).value
    case DateType =>
      Cast(Literal(value), DateType, Some(zoneId.getId)).eval()
    // Timestamp types
    case dt if AnyTimestampType.acceptsType(dt) =>
      Try {
        Cast(Literal(unescapePathName(value)), dt, Some(zoneId.getId)).eval()
      }.getOrElse {
        Cast(Cast(Literal(value), DateType, Some(zoneId.getId)), dt).eval()
      }
    case it: AnsiIntervalType =>
      Cast(Literal(unescapePathName(value)), it).eval()
    case BinaryType => value.getBytes()
    case BooleanType => value.toBoolean
    case dt => throw QueryExecutionErrors.typeUnsupportedError(dt)
  }

  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * For example, given:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14
   *   hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28
   * }}}
   * it returns:
   * {{{
   *   PartitionSpec(
   *     partitionColumns = StructType(
   *       StructField(name = "a", dataType = IntegerType, nullable = true),
   *       StructField(name = "b", dataType = StringType, nullable = true),
   *       StructField(name = "c", dataType = DoubleType, nullable = true)),
   *     partitions = Seq(
   *       Partition(
   *         values = Row(1, "hello", 3.14),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14"),
   *       Partition(
   *         values = Row(2, "world", 6.28),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28")))
   * }}}
   */
  private[datasources] def parsePartitions(
    paths: Seq[Path],
    typeInference: Boolean,
    basePaths: Set[Path],
    userSpecifiedSchema: Option[StructType],
    caseSensitive: Boolean,
    validatePartitionColumns: Boolean,
    timeZoneId: String): PartitionSpec = {
    parsePartitions(paths, typeInference, basePaths, userSpecifiedSchema, caseSensitive,
      validatePartitionColumns, DateTimeUtils.getZoneId(timeZoneId))
  }

  private[datasources] def parsePartitions(
    paths: Seq[Path],
    typeInference: Boolean,
    basePaths: Set[Path],
    userSpecifiedSchema: Option[StructType],
    caseSensitive: Boolean,
    validatePartitionColumns: Boolean,
    zoneId: ZoneId): PartitionSpec = {
    val userSpecifiedDataTypes = if (userSpecifiedSchema.isDefined) {
      val nameToDataType = userSpecifiedSchema.get.fields.map(f => f.name -> f.dataType).toMap
      if (!caseSensitive) {
        CaseInsensitiveMap(nameToDataType)
      } else {
        nameToDataType
      }
    } else {
      Map.empty[String, DataType]
    }

    // SPARK-26990: use user specified field names if case insensitive.
    val userSpecifiedNames = if (userSpecifiedSchema.isDefined && !caseSensitive) {
      CaseInsensitiveMap(userSpecifiedSchema.get.fields.map(f => f.name -> f.name).toMap)
    } else {
      Map.empty[String, String]
    }

    val dateFormatter = DateFormatter(DateFormatter.defaultPattern)
    val timestampFormatter = TimestampFormatter(
      timestampPartitionPattern,
      zoneId,
      isParsing = true)
    // First, we need to parse every partition's path and see if we can find partition values.
    val (partitionValues, optDiscoveredBasePaths) = paths.map { path =>
      parsePartition(path, typeInference, basePaths, userSpecifiedDataTypes,
        validatePartitionColumns, zoneId, dateFormatter, timestampFormatter)
    }.unzip

    // We create pairs of (path -> path's partition value) here
    // If the corresponding partition value is None, the pair will be skipped
    val pathsWithPartitionValues = paths.zip(partitionValues).flatMap(x => x._2.map(x._1 -> _))

    if (pathsWithPartitionValues.isEmpty) {
      // This dataset is not partitioned.
      PartitionSpec.emptySpec
    } else {
      // This dataset is partitioned. We need to check whether all partitions have the same
      // partition columns and resolve potential type conflicts.

      // Check if there is conflicting directory structure.
      // For the paths such as:
      // var paths = Seq(
      //   "hdfs://host:9000/invalidPath",
      //   "hdfs://host:9000/path/a=10/b=20",
      //   "hdfs://host:9000/path/a=10.5/b=hello")
      // It will be recognised as conflicting directory structure:
      //   "hdfs://host:9000/invalidPath"
      //   "hdfs://host:9000/path"
      // TODO: Selective case sensitivity.
      val discoveredBasePaths = optDiscoveredBasePaths.flatten.map(_.toString.toLowerCase())
      assert(
        discoveredBasePaths.distinct.size == 1,
        "Conflicting directory structures detected. Suspicious paths:\b" +
          discoveredBasePaths.distinct.mkString("\n\t", "\n\t", "\n\n") +
          "If provided paths are partition directories, please set " +
          "\"basePath\" in the options of the data source to specify the " +
          "root directory of the table. If there are multiple root directories, " +
          "please load them separately and then union them.")

      val resolvedPartitionValues = resolvePartitions(pathsWithPartitionValues, caseSensitive)

      // Creates the StructType which represents the partition columns.
      val fields = {
        val PartitionValues(columnNames, typedValues) = resolvedPartitionValues.head
        columnNames.zip(typedValues).map { case (name, TypedPartValue(_, dataType)) =>
          // We always assume partition columns are nullable since we've no idea whether null values
          // will be appended in the future.
          val resultName = userSpecifiedNames.getOrElse(name, name)
          val resultDataType = userSpecifiedDataTypes.getOrElse(name, dataType)
          StructField(resultName, resultDataType, nullable = true)
        }
      }

      // Finally, we create `Partition`s based on paths and resolved partition values.
      val partitions = resolvedPartitionValues.zip(pathsWithPartitionValues).map {
        case (PartitionValues(columnNames, typedValues), (path, _)) =>
          val rowValues = columnNames.zip(typedValues).map { case (columnName, typedValue) =>
            try {
              castPartValueToDesiredType(typedValue.dataType, typedValue.value, zoneId)
            } catch {
              case NonFatal(_) =>
                if (validatePartitionColumns) {
                  throw QueryExecutionErrors.failedToCastValueToDataTypeForPartitionColumnError(
                    typedValue.value, typedValue.dataType, columnName)
                } else null
            }
          }
          PartitionPath(InternalRow.fromSeq(rowValues), path)
      }

      PartitionSpec(StructType(fields), partitions)
    }
  }


}
