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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.execution.python._

object SparkShimImpl extends Spark320PlusShims {
  override def getSparkShimVersion: ShimVersion = ShimLoader.getShimVersion

  private val spark331dbExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    Seq(
      GpuOverrides.exec[GlobalLimitExec](
        "Limiting of results across partitions",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all),
        (globalLimit, conf, p, r) =>
          new SparkPlanMeta[GlobalLimitExec](globalLimit, conf, p, r) {
            override def convertToGpu(): GpuExec =
              GpuGlobalLimitExec(
                globalLimit.limit, childPlans.head.convertIfNeeded(), globalLimit.offset)
          }),
      GpuOverrides.exec[CollectLimitExec](
        "Reduce to single partition and apply limit",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all),
        (collectLimit, conf, p, r) => new GpuCollectLimitMeta(collectLimit, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(collectLimit.limit,
              GpuShuffleExchangeExec(
                GpuSinglePartitioning,
                GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded()),
                ENSURE_REQUIREMENTS
              )(SinglePartition), collectLimit.offset)
        }
      ).disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
        "of rows in a batch it could help by limiting the number of rows transferred from " +
        "GPU to CPU")
    )
    .map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r))
    .toMap

  private val spark330PlusExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    Seq(
      GpuOverrides.exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.DECIMAL_128 + TypeSig.BINARY +
              GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r)),
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (fsse, conf, p, r) => new FileSourceScanExecMeta(fsse, conf, p, r)),
      GpuOverrides.exec[PythonMapInArrowExec](
        "The backend for Map Arrow Iterator UDF. Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (mapPy, conf, p, r) => new GpuPythonMapInArrowExecMeta(mapPy, conf, p, r))
    )
    .map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r))
    .toMap

  private val spark320PlusExec = super.getExecs

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    spark320PlusExec ++ spark330PlusExecs ++ spark331dbExecs

  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  def getWindowExpressions(winPy: org.apache.spark.sql.execution.python.WindowInPandasExec): Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression] = ???

  // Members declared in com.nvidia.spark.rapids.SparkShims
  def broadcastModeTransform(mode: org.apache.spark.sql.catalyst.plans.physical.BroadcastMode,toArray: Array[org.apache.spark.sql.catalyst.InternalRow]): Any = ???
  def filesFromFileIndex(fileCatalog: org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex): Seq[org.apache.hadoop.fs.FileStatus] = ???
  def getFileScanRDD(sparkSession: org.apache.spark.sql.SparkSession,readFunction: org.apache.spark.sql.execution.datasources.PartitionedFile => Iterator[org.apache.spark.sql.catalyst.InternalRow],filePartitions: Seq[org.apache.spark.sql.execution.datasources.FilePartition],readDataSchema: org.apache.spark.sql.types.StructType,metadataColumns: Seq[org.apache.spark.sql.catalyst.expressions.AttributeReference]): org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = ???
  def getParquetFilters(schema: org.apache.parquet.schema.MessageType,pushDownDate: Boolean,pushDownTimestamp: Boolean,pushDownDecimal: Boolean,pushDownStartWith: Boolean,pushDownInFilterThreshold: Int,caseSensitive: Boolean,lookupFileMeta: String => String,dateTimeRebaseModeFromConf: String): org.apache.spark.sql.execution.datasources.parquet.ParquetFilters = ???
  def neverReplaceShowCurrentNamespaceCommand: com.nvidia.spark.rapids.ExecRule[_ <: org.apache.spark.sql.execution.SparkPlan] = ???
  def newBroadcastQueryStageExec(old: org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec,newPlan: org.apache.spark.sql.execution.SparkPlan): org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec = ???
  def reusedExchangeExecPfn: PartialFunction[org.apache.spark.sql.execution.SparkPlan,org.apache.spark.sql.execution.exchange.ReusedExchangeExec] = ???
}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
