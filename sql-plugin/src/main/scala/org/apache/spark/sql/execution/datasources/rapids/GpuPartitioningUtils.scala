/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.AlluxioUtils.AlluxioPathReplaceConvertTime
import com.nvidia.spark.rapids.shims.FileIndexOptionsShims.BASE_PATH_PARAM
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.{unescapePathName, DEFAULT_PARTITION_NAME}
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.rapids.DateFormatter
import org.apache.spark.sql.execution.datasources.{PartitionPath, PartitionSpec}
import org.apache.spark.sql.execution.datasources.PartitioningUtils.timestampPartitionPattern
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.shims.PartitionValueCastShims
import org.apache.spark.unsafe.types.UTF8String

object GpuPartitioningUtils extends SQLConfHelper {

  case class TypedPartValue(value: String, dataType: DataType)

  case class PartitionValues(columnNames: Seq[String], typedValues: Seq[TypedPartValue])

  /**
   *
   * @param sparkSession
   * @param rootPaths the list of root input paths from which the catalog will get files
   * @param leafFiles leaf file paths
   * @param parameters a set of options to control partition discovery
   * @param userSpecifiedSchema an optional user specified schema that will be use to provide
   *                            types for the discovered partitions
   * @param replaceFunc the alluxio replace function
   * @return the specification of the partitions inferred from the data and if it was replaced the
   *         original path
   *
   * Mainly copied from PartitioningAwareFileIndex.inferPartitioning
   */
  def inferPartitioning(
      sparkSession: SparkSession,
      rootPaths: Seq[Path],
      leafFiles: Seq[Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      replaceFunc: Path => AlluxioPathReplaceConvertTime)
    : (PartitionSpec, Option[String]) = {

    val recursiveFileLookup = parameters.getOrElse("recursiveFileLookup", "false").toBoolean

    if (recursiveFileLookup) {
      (PartitionSpec.emptySpec, None)
    } else {
      val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
      val timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

      // filter out non-data path and get unique leaf dirs of inputFiles
      val leafDirs: Seq[Path] = leafFiles.filter(isDataPath).map(_.getParent).distinct

      val basePathAndAnyReplacedOption = parameters.get(BASE_PATH_PARAM).map { file =>
        // need to replace the base path
        replaceFunc(new Path(file))
      }
      val basePathOption = basePathAndAnyReplacedOption.map(_.filePath)
      val anyReplacedBase = basePathAndAnyReplacedOption.flatMap(_.origPrefix)

      val basePaths = getBasePaths(sparkSession.sessionState.newHadoopConfWithOptions(parameters),
        basePathOption, rootPaths, leafFiles)

      val parsed = parsePartitions(
        leafDirs,
        typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
        basePaths = basePaths,
        userSpecifiedSchema = userSpecifiedSchema,
        caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis,
        validatePartitionColumns = sparkSession.sessionState.conf.validatePartitionColumns,
        timeZoneId = timeZoneId)
      (parsed, anyReplacedBase)
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  // Copied from PartitioningAwareFileIndex.isDataPath
  def isDataPath(path: Path): Boolean = {
    isDataPath(path.getName)
  }

  def isDataPath(name: String): Boolean = {
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
    case ByteType => Integer.parseInt(value).toByte
    case ShortType => Integer.parseInt(value).toShort
    case IntegerType => Integer.parseInt(value)
    case LongType => JLong.parseLong(value)
    case FloatType => JDouble.parseDouble(value).toFloat
    case DoubleType => JDouble.parseDouble(value)
    case _: DecimalType => Literal(new JBigDecimal(value)).value
    case DateType =>
      Cast(Literal(value), DateType, Some(zoneId.getId)).eval()
    case BinaryType => value.getBytes()
    case BooleanType => value.toBoolean
    case t if PartitionValueCastShims.isSupportedType(t) =>
      PartitionValueCastShims.castTo(t, value, zoneId)
    case dt => throw new IllegalArgumentException(s"Unexpected type $dt")
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
                  throw new RuntimeException(s"Failed to cast value `$typedValue.value` to " +
                    s"`$typedValue.dataType` for partition column `$columnName`")
                } else null
            }
          }
          PartitionPath(InternalRow.fromSeq(rowValues), path)
      }

      PartitionSpec(StructType(fields), partitions)
    }
  }

  /**
   * Parses a single partition, returns column names and values of each partition column, also
   * the path when we stop partition discovery.  For example, given:
   * {{{
   *   path = hdfs://<host>:<port>/path/to/partition/a=42/b=hello/c=3.14
   * }}}
   * it returns the partition:
   * {{{
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, DoubleType)))
   * }}}
   * and the path when we stop the discovery is:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition
   * }}}
   */
  private[datasources] def parsePartition(
      path: Path,
      typeInference: Boolean,
      basePaths: Set[Path],
      userSpecifiedDataTypes: Map[String, DataType],
      validatePartitionColumns: Boolean,
      zoneId: ZoneId,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): (Option[PartitionValues], Option[Path]) = {
    val columns = ArrayBuffer.empty[(String, TypedPartValue)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    // currentPath is the current path that we will use to parse partition column value.
    var currentPath: Path = path

    while (!finished) {
      // Sometimes (e.g., when speculative task is enabled), temporary directories may be left
      // uncleaned. Here we simply ignore them.
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        return (None, None)
      }

      if (basePaths.contains(currentPath)) {
        // If the currentPath is one of base paths. We should stop.
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
        parsePartitionColumn(currentPath.getName, typeInference, userSpecifiedDataTypes,
          zoneId, dateFormatter, timestampFormatter)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop:
        //  - In this iteration, we could not parse the value of partition column and value,
        //    i.e. maybeColumn is None, and columns is not empty. At here we check if columns is
        //    empty to handle cases like /table/a=1/_temporary/something (we need to find a=1 in
        //    this case).
        //  - After we get the new currentPath, this new currentPath represent the top level dir
        //    i.e. currentPath.getParent == null. For the example of "/table/a=1/",
        //    the top level dir is "/table".
        finished =
          (maybeColumn.isEmpty && !columns.isEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames.toSeq, values.toSeq)), Some(currentPath))
    }
  }

  private def parsePartitionColumn(
      columnSpec: String,
      typeInference: Boolean,
      userSpecifiedDataTypes: Map[String, DataType],
      zoneId: ZoneId,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): Option[(String, TypedPartValue)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val dataType = if (userSpecifiedDataTypes.contains(columnName)) {
        // SPARK-26188: if user provides corresponding column schema, get the column value without
        //              inference, and then cast it as user specified data type.
        userSpecifiedDataTypes(columnName)
      } else {
        inferPartitionColumnValue(
          rawColumnValue,
          typeInference,
          zoneId,
          dateFormatter,
          timestampFormatter)
      }
      Some(columnName -> TypedPartValue(rawColumnValue, dataType))
    }
  }

  private[datasources] def listConflictingPartitionColumns(
      pathWithPartitionValues: Seq[(Path, PartitionValues)]): String = {
    val distinctPartColNames = pathWithPartitionValues.map(_._2.columnNames).distinct

    def groupByKey[K, V](seq: Seq[(K, V)]): Map[K, Iterable[V]] =
      seq.groupBy { case (key, _) => key }.mapValues(_.map { case (_, value) => value }).toMap

    val partColNamesToPaths = groupByKey(pathWithPartitionValues.map {
      case (path, partValues) => partValues.columnNames -> path
    })

    val distinctPartColLists = distinctPartColNames.map(_.mkString(", ")).zipWithIndex.map {
      case (names, index) =>
        s"Partition column name list #$index: $names"
    }

    // Lists out those non-leaf partition directories that also contain files
    val suspiciousPaths = distinctPartColNames.sortBy(_.length).flatMap(partColNamesToPaths)

    s"Conflicting partition column names detected:\n" +
      distinctPartColLists.mkString("\n\t", "\n\t", "\n\n") +
      "For partitioned table directories, data files should only live in leaf directories.\n" +
      "And directories at the same level should have the same partition column name.\n" +
      "Please check the following directories for unexpected files or " +
      "inconsistent partition column names:\n" +
      suspiciousPaths.map("\t" + _).mkString("\n", "\n", "")
  }

  // scalastyle:off line.size.limit
  /**
   * Converts a string to a [[Literal]] with automatic type inference. Currently only supports
   * [[NullType]], [[IntegerType]], [[LongType]], [[DoubleType]], [[DecimalType]], [[DateType]]
   * [[TimestampType]], and [[StringType]].
   *
   * When resolving conflicts, it follows the table below:
   *
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * | InputA \ InputB    | NullType          | IntegerType       | LongType          | DecimalType(38,0)* | DoubleType | DateType      | TimestampType | StringType |
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * | NullType           | NullType          | IntegerType       | LongType          | DecimalType(38,0)  | DoubleType | DateType      | TimestampType | StringType |
   * | IntegerType        | IntegerType       | IntegerType       | LongType          | DecimalType(38,0)  | DoubleType | StringType    | StringType    | StringType |
   * | LongType           | LongType          | LongType          | LongType          | DecimalType(38,0)  | StringType | StringType    | StringType    | StringType |
   * | DecimalType(38,0)* | DecimalType(38,0) | DecimalType(38,0) | DecimalType(38,0) | DecimalType(38,0)  | StringType | StringType    | StringType    | StringType |
   * | DoubleType         | DoubleType        | DoubleType        | StringType        | StringType         | DoubleType | StringType    | StringType    | StringType |
   * | DateType           | DateType          | StringType        | StringType        | StringType         | StringType | DateType      | TimestampType | StringType |
   * | TimestampType      | TimestampType     | StringType        | StringType        | StringType         | StringType | TimestampType | TimestampType | StringType |
   * | StringType         | StringType        | StringType        | StringType        | StringType         | StringType | StringType    | StringType    | StringType |
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * Note that, for DecimalType(38,0)*, the table above intentionally does not cover all other
   * combinations of scales and precisions because currently we only infer decimal type like
   * `BigInteger`/`BigInt`. For example, 1.1 is inferred as double type.
   */
  // scalastyle:on line.size.limit
  private[datasources] def inferPartitionColumnValue(
       raw: String,
       typeInference: Boolean,
       zoneId: ZoneId,
       dateFormatter: DateFormatter,
       timestampFormatter: TimestampFormatter): DataType = {
    val decimalTry = Try {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new JBigDecimal(raw)
      // It reduces the cases for decimals by disallowing values having scale (e.g. `1.1`).
      require(bigDecimal.scale <= 0)
      // `DecimalType` conversion can fail when
      //   1. The precision is bigger than 38.
      //   2. scale is bigger than precision.
      DecimalType.fromDecimal(Decimal(bigDecimal))
    }

    val dateTry = Try {
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // DateType
      dateFormatter.parse(raw)
      // SPARK-23436: Casting the string to date may still return null if a bad Date is provided.
      // This can happen since DateFormat.parse  may not use the entire text of the given string:
      // so if there are extra-characters after the date, it returns correctly.
      // We need to check that we can cast the raw string since we later can use Cast to get
      // the partition values with the right DataType (see
      // org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex.inferPartitioning)
      val dateValue = Cast(Literal(raw), DateType, Some(zoneId.getId)).eval()
      // Disallow DateType if the cast returned null
      require(dateValue != null)
      DateType
    }

    val timestampTry = Try {
      val unescapedRaw = unescapePathName(raw)
      // the inferred data type is consistent with the default timestamp type
      val timestampType = TimestampType
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // TimestampType or TimestampNTZType
      timestampType match {
        case TimestampType => timestampFormatter.parse(unescapedRaw)
        // comment the NTZ type for very limited use case of "SPARK_TESTING" to reduce duplicated
        // code copied from Spark
        // case TimestampNTZType => timestampFormatter.parseWithoutTimeZone(unescapedRaw)
      }
      // SPARK-23436: see comment for date
      val timestampValue = Cast(Literal(unescapedRaw), timestampType, Some(zoneId.getId)).eval()
      // Disallow TimestampType if the cast returned null
      require(timestampValue != null)
      timestampType
    }

    if (typeInference) {
      // First tries integral types
      Try({ Integer.parseInt(raw); IntegerType })
        .orElse(Try { JLong.parseLong(raw); LongType })
        .orElse(decimalTry)
        // Then falls back to fractional types
        .orElse(Try { JDouble.parseDouble(raw); DoubleType })
        // Then falls back to date/timestamp types
        .orElse(timestampTry)
        .orElse(dateTry)
        // Then falls back to string
        .getOrElse {
          if (raw == DEFAULT_PARTITION_NAME) NullType else StringType
        }
    } else {
      if (raw == DEFAULT_PARTITION_NAME) NullType else StringType
    }
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types using
   * [[findWiderTypeForPartitionColumn]].
   */
  def resolvePartitions(
       pathsWithPartitionValues: Seq[(Path, PartitionValues)],
       caseSensitive: Boolean): Seq[PartitionValues] = {
    if (pathsWithPartitionValues.isEmpty) {
      Seq.empty
    } else {
      val partColNames = if (caseSensitive) {
        pathsWithPartitionValues.map(_._2.columnNames)
      } else {
        pathsWithPartitionValues.map(_._2.columnNames.map(_.toLowerCase()))
      }
      assert(
        partColNames.distinct.size == 1,
        listConflictingPartitionColumns(pathsWithPartitionValues))

      // Resolves possible type conflicts for each column
      val values = pathsWithPartitionValues.map(_._2)
      val columnCount = values.head.columnNames.size
      val resolvedValues = (0 until columnCount).map { i =>
        resolveTypeConflicts(values.map(_.typedValues(i)))
      }

      // Fills resolved literals back to each partition
      values.zipWithIndex.map { case (d, index) =>
        d.copy(typedValues = resolvedValues.map(_(index)))
      }
    }
  }

  /**
   * Given a collection of [[Literal]]s, resolves possible type conflicts by
   * [[findWiderTypeForPartitionColumn]].
   */
  private def resolveTypeConflicts(typedValues: Seq[TypedPartValue]): Seq[TypedPartValue] = {
    val dataTypes = typedValues.map(_.dataType)
    val desiredType = dataTypes.reduce(findWiderTypeForPartitionColumn)

    typedValues.map(tv => tv.copy(dataType = desiredType))
  }

  /**
   * Type widening rule for partition column types. It is similar to
   * [[TypeCoercion.findWiderTypeForTwo]] but the main difference is that here we disallow
   * precision loss when widening double/long and decimal, and fall back to string.
   */
  private val findWiderTypeForPartitionColumn: (DataType, DataType) => DataType = {
    case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) => StringType
    case (DoubleType, LongType) | (LongType, DoubleType) => StringType
    case (t1, t2) => TypeCoercion.findWiderTypeForTwo(t1, t2).getOrElse(StringType)
  }

}
