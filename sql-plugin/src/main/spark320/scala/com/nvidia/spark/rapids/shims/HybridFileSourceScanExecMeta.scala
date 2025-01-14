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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._

import org.apache.spark.rapids.hybrid.HybridFileSourceScanExec
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

class HybridFileSourceScanExecMeta(plan: FileSourceScanExec,
                                   conf: RapidsConf,
                                   parent: Option[RapidsMeta[_, _, _]],
                                   rule: DataFromReplacementRule)
  extends SparkPlanMeta[FileSourceScanExec](plan, conf, parent, rule) {

  // Replaces SubqueryBroadcastExec inside dynamic pruning filters with native counterpart
  // if possible. Instead regarding filters as childExprs of current Meta, we create
  // a new meta for SubqueryBroadcastExec. The reason is that the native replacement of
  // FileSourceScan is independent from the replacement of the partitionFilters.
  private lazy val partitionFilters = {
    val convertBroadcast = (bc: SubqueryBroadcastExec) => {
      val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
      meta.tagForExplain()
      meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
    }
    wrapped.partitionFilters.map { filter =>
      filter.transformDown {
        case dpe@DynamicPruningExpression(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
            case reuse@ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
            case _ =>
              dpe
          }
      }
    }
  }

  // partition filters and data filters are not run on the GPU
  override val childExprs: Seq[ExprMeta[_]] = Seq.empty

  override def tagPlanForGpu(): Unit = {
    val cls = wrapped.relation.fileFormat.getClass
    if (cls != classOf[ParquetFileFormat]) {
      willNotWorkOnGpu(s"unsupported file format: ${cls.getCanonicalName}")
    }
  }

  override def convertToGpu(): GpuExec = {
    // Modifies the original plan to support DPP
    val fixedExec = wrapped.copy(partitionFilters = partitionFilters)
    HybridFileSourceScanExec(fixedExec)(conf)
  }
}

object HybridFileSourceScanExecMeta {
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
    lazy val isParquet = fsse.relation.fileFormat.getClass == classOf[ParquetFileFormat]
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

    isEnabled && isParquet && allSupportedTypes && noBucketedScan
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
}
