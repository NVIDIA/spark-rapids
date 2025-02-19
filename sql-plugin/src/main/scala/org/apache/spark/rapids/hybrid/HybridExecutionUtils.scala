/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.hybrid

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{RapidsConf, VersionUtils}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object HybridExecutionUtils extends PredicateHelper {
  
  private val HYBRID_JAR_PLUGIN_CLASS_NAME = "com.nvidia.spark.rapids.hybrid.HybridPluginWrapper"

  /**
   * Check if the Hybrid jar is in the classpath,
   * report error if not
   */
  def checkHybridJarInClassPath(): Unit = {
    try {
      Class.forName(HYBRID_JAR_PLUGIN_CLASS_NAME)
    } catch {
      case e: ClassNotFoundException => throw new RuntimeException(
        "Hybrid jar is not in the classpath, Please add Hybrid jar into the class path, or " +
            "Please disable Hybrid feature by setting " +
            "spark.rapids.sql.parquet.useHybridReader=false", e)
    }
  }

    // Determines whether using HybridScan or GpuScan
  def useHybridScan(conf: RapidsConf, fsse: FileSourceScanExec): Boolean = {
    val isEnabled = if (conf.useHybridParquetReader) {
      require(conf.loadHybridBackend,
        "Hybrid backend was NOT loaded during the launch of spark-rapids plugin")
      true
    } else {
      false
    }
    // Currently, only support reading Parquet
    val isParquet = fsse.relation.fileFormat.getClass == classOf[ParquetFileFormat]
    // Fallback to GpuScan over the `select count(1)` cases
    val nonEmptySchema = fsse.output.nonEmpty && fsse.requiredSchema.nonEmpty
    // Check if data types of all fields are supported by HybridParquetReader
    lazy val allSupportedTypes = !fsse.requiredSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, {
        // Currently, under some circumstance, the native backend may return incorrect results
        // over MapType nested by nested types. To guarantee the correctness, disable this pattern
        // entirely.
        // TODO: figure out the root cause and support it
        case ArrayType(_: MapType, _) => true
        case MapType(_: MapType, _, _) | MapType(_, _: MapType, _) => true
        case st: StructType if st.exists(_.dataType.isInstanceOf[MapType]) => true
        // TODO: support DECIMAL with negative scale
        case dt: DecimalType if dt.scale < 0 => true
        // TODO: support DECIMAL128
        case dt: DecimalType if dt.precision > DType.DECIMAL64_MAX_PRECISION => true
        // TODO: support BinaryType
        case _: BinaryType => true
        case _ => false
      })
    }
    // TODO: supports BucketedScan
    lazy val noBucketedScan = !fsse.bucketedScan

    isEnabled && isParquet && nonEmptySchema && allSupportedTypes && noBucketedScan
  }

  /**
   * Check if runtimes are satisfied, including:
   * - Spark distribution is not CDH or Databricks
   * - Hybrid jar in the classpath
   * - Scala version is 2.12
   * - Parquet V1 data source
   */
  def checkRuntimes(v1DataSourceList: String): Unit = {
    checkNotRunningCDHorDatabricks()
    HybridExecutionUtils.checkHybridJarInClassPath()
    checkScalaVersion()
    checkV1Datasource(v1DataSourceList)
  }

  /**
   * Check Spark distribution is not CDH or Databricks,
   * report error if it is
   */
  private def checkNotRunningCDHorDatabricks(): Unit = {
    if (VersionUtils.isCloudera || VersionUtils.isDataBricks) {
      throw new RuntimeException("Hybrid feature does not support Cloudera/Databricks " +
          "Spark releases, Please disable Hybrid feature by setting " +
          "spark.rapids.sql.parquet.useHybridReader=false")
    }
  }

  /**
   * Hybrid feature only supports Scala 2.12 version,
   * report error if not
   */
  private def checkScalaVersion(): Unit = {
    val scalaVersion = scala.util.Properties.versionString
    if (!scalaVersion.startsWith("version 2.12")) {
      throw new RuntimeException(s"Hybrid feature only supports Scala 2.12 version, " +
          s"but got $scalaVersion")
    }
  }

  /**
   * Hybrid feature only supports v1 datasource,
   * report error if it's not satisfied
   */
  private def checkV1Datasource(v1SourceList: String): Unit = {
    // check spark.sql.sources.useV1SourceList contains parquet
    if(!v1SourceList.contains("parquet")) {
      throw new RuntimeException(s"Hybrid feature only supports v1 datasource, " +
        s"please set spark.sql.sources.useV1SourceList=parquet")
    }
  }

  // scalastyle:off line.size.limit
  // from https://github.com/apache/incubator-gluten/blob/branch-1.2/docs/velox-backend-support-progress.md
  // Only fully supported functions are listed here
  // scalastyle:on
  val ansiOn = Seq(
    classOf[Acos],
    classOf[Acosh],
    classOf[AddMonths],
    classOf[Alias],
    classOf[And],
    classOf[ArrayAggregate],
    classOf[ArrayContains],
    classOf[ArrayDistinct],
    classOf[ArrayExcept],
    classOf[ArrayExists],
    classOf[ArrayForAll],
    classOf[ArrayIntersect],
    classOf[ArrayJoin],
    classOf[ArrayMax],
    classOf[ArrayMin],
    classOf[ArrayPosition],
    classOf[ArrayRemove],
    classOf[ArrayRepeat],
    classOf[ArraySort],
    classOf[ArraysZip],
    classOf[Ascii],
    classOf[Asin],
    classOf[Asinh],
    classOf[Atan],
    classOf[Atan2],
    classOf[Atanh],
    classOf[AttributeReference],
    classOf[BitLength],
    classOf[BitwiseAnd],
    classOf[BitwiseOr],
    classOf[Cbrt],
    classOf[Ceil],
    classOf[Chr],
    classOf[Concat],
    classOf[Cos],
    classOf[Cosh],
    classOf[Crc32],
    classOf[CreateNamedStruct],
    classOf[DateAdd],
    classOf[DateDiff],
    classOf[DateFormatClass],
    classOf[DateFromUnixDate],
    classOf[DateSub],
    classOf[DayOfMonth],
    classOf[DayOfWeek],
    classOf[DayOfYear],
    classOf[ElementAt],
    classOf[EqualNullSafe],
    classOf[EqualTo],
    classOf[Exp],
    classOf[Expm1],
    classOf[FindInSet],
    classOf[Flatten],
    classOf[Floor],
    classOf[FromUTCTimestamp],
    classOf[FromUnixTime],
    classOf[GetJsonObject],
    classOf[GetMapValue],
    classOf[GreaterThan],
    classOf[GreaterThanOrEqual],
    classOf[Greatest],
    classOf[Hex],
    classOf[Hour],
    classOf[If],
    classOf[In],
    classOf[IsNaN],
    classOf[IsNotNull],
    classOf[IsNull],
    classOf[LastDay],
    classOf[Least],
    classOf[Left],
    classOf[Length],
    classOf[LengthOfJsonArray],
    classOf[LessThan],
    classOf[Levenshtein],
    classOf[Like],
    classOf[Literal],
    classOf[Log],
    classOf[Log10],
    classOf[Log2],
    classOf[Lower],
    classOf[MapFromArrays],
    classOf[MapKeys],
    classOf[MapValues],
    classOf[MapZipWith],
    classOf[Md5],
    classOf[MicrosToTimestamp],
    classOf[MillisToTimestamp],
    classOf[Minute],
    classOf[MonotonicallyIncreasingID],
    classOf[Month],
    classOf[NaNvl],
    classOf[NextDay],
    classOf[Not],
    classOf[Or],
    classOf[Overlay],
    classOf[Pi],
    classOf[Pow],
    classOf[Quarter],
    classOf[Rand],
    classOf[Remainder],
    classOf[Reverse],
    classOf[Rint],
    classOf[Round],
    classOf[Second],
    classOf[Sha1],
    classOf[Sha2],
    classOf[ShiftLeft],
    classOf[ShiftRight],
    classOf[Shuffle],
    classOf[Sin],
    classOf[Size],
    classOf[SortArray],
    classOf[SoundEx],
    classOf[SparkPartitionID],
    classOf[Sqrt],
    classOf[Stack],
    classOf[StringInstr],
    classOf[StringLPad],
    classOf[StringRPad],
    classOf[StringRepeat],
    classOf[StringReplace],
    classOf[StringToMap],
    classOf[StringTrim],
    classOf[StringTrimLeft],
    classOf[StringTrimRight],
    classOf[Substring],
    classOf[SubstringIndex],
    classOf[Tan],
    classOf[Tanh],
    classOf[ToDegrees],
    classOf[ToRadians],
    classOf[ToUnixTimestamp],
    classOf[UnaryPositive],
    classOf[Unhex],
    classOf[UnixMicros],
    classOf[UnixMillis],
    classOf[UnixSeconds],
    classOf[Upper],
    classOf[Uuid],
    classOf[WeekDay],
    classOf[WeekOfYear],
    classOf[WidthBucket],
    classOf[Year],
    classOf[ZipWith]
  )

  // Some functions are fully supported with ANSI mode off
  lazy val ansiOff = Seq(
    classOf[Remainder],
    classOf[Multiply],
    classOf[Add],
    classOf[Subtract],
    classOf[Divide],
    classOf[Abs],
    classOf[Pmod]
  )

  def supportedByHybridFilters(whitelistExprsName: String): Seq[Class[_]] = {
    val whitelistExprs: Seq[Class[_]] = whitelistExprsName.split(",").map { exprName =>
      try {
        Some(Class.forName("org.apache.spark.sql.catalyst.expressions." + exprName))
      } catch {
        case _: ClassNotFoundException => None
      }
    }.collect { case Some(cls) => cls }

    if (SQLConf.get.ansiEnabled) {
      ansiOn ++ whitelistExprs
    } else {
      ansiOn ++ ansiOff ++ whitelistExprs
    }
  }

  def canBePushedToHybrid(child: SparkPlan, conf: RapidsConf): String = {
    child match {
      case fsse: FileSourceScanExec if useHybridScan(conf, fsse) =>
        conf.pushDownFiltersToHybrid
      case _ => "OFF"
    }
  }

  def isTimestampCondition(expr: Expression): Boolean = {
    expr.references.exists(attr => attr.dataType == TimestampType)
  }

  def isExprSupportedByHybridScan(condition: Expression, whitelistExprsName: String): Boolean = {
    condition match {
      case filter if isTimestampCondition(filter) => false // Timestamp is not fully supported in Hybrid Filter
      case filter if HybridExecutionUtils.supportedByHybridFilters(whitelistExprsName)
          .exists(_.isInstance(filter)) =>
        val childrenSupported = filter.children.forall(
            isExprSupportedByHybridScan(_, whitelistExprsName))
        childrenSupported
      case _ => false
    }
  }

  /** 
   * Search the plan for FilterExec whose child is a FileSourceScanExec if Hybrid Scan is enabled
   * and decide whether to push down a filter to the GPU or not according to whether CPU/GPU
   * support it. After that we can remove the condition from one side to avoid duplicate execution
   * or unnecessary fallback/crash.
   */
  def hybridScanFilterSplit(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case filter: FilterExec => {
        lazy val filters = splitConjunctivePredicates(filter.condition)
        val canBePushed = HybridExecutionUtils.canBePushedToHybrid(filter.child, conf)
        (filter.child, canBePushed) match {
          case (fsse: FileSourceScanExec, "GPU") => {
            // remainingFilters shoule be empty, filter to make it more robust
            val remainingFilters = fsse.dataFilters.filterNot(filters.contains)
            val updatedFsseChild = fsse.copy(dataFilters = remainingFilters)
            val updatedFilter = FilterExec(filter.condition, updatedFsseChild)
            updatedFilter
          }
          case (fsse: FileSourceScanExec, "CPU") => {
            val (supportedConditions, notSupportedConditions) = filters.partition(
                isExprSupportedByHybridScan(_, conf.hybridExprsWhitelist))
            val updatedFsseChild = fsse.copy(dataFilters = supportedConditions)
            notSupportedConditions match {
              case Nil => updatedFsseChild
              case _ => FilterExec(notSupportedConditions.reduceLeft(And), updatedFsseChild)
            }
          }
          case _ => filter
        }
      }
    }
  }

  def tryToApplyHybridScanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (!conf.useHybridParquetReader) {
      return plan
    }
    hybridScanFilterSplit(plan, conf)
  }
}
