/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.velox

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuExec, GpuMetric, RapidsConf, TargetSize}
import org.apache.gluten.execution.{FileSourceScanExecTransformer, WholeStageTransformer}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.rapids.GpuDataSourceScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

case class VeloxFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    originalOutput: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false,
    queryUsesInputFile: Boolean = false,
    alluxioPathsMap: Option[Map[String, String]],
    filteredOutput: Option[Seq[Attribute]] = None)(@transient val rapidsConf: RapidsConf)
  extends GpuDataSourceScanExec with GpuExec {
  import GpuMetric._

  // All expressions are on the CPU.
  override def gpuExpressions: Seq[Expression] = Nil

  private val glutenScan: FileSourceScanExecTransformer = {
    FileSourceScanExecTransformer(
      relation,
      output,
      requiredSchema,
      partitionFilters,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      dataFilters,
      tableIdentifier,
      disableBucketedScan)
  }

  private val coalesceSizeGoal = rapidsConf.gpuTargetBatchSizeBytes

  override def output: Seq[Attribute] = filteredOutput.getOrElse(originalOutput)

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")

    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName +
        GpuDataSourceScanExec.buildLocationMetadata(location.rootPaths, maxMetadataValueLength)
    Map(
      "Format" -> relation.fileFormat.toString,
      "ReadSchema" -> requiredSchema.catalogString,
      "Batched" -> supportsColumnar.toString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "PushedFilters" -> seqToString(glutenScan.filterExprs()),
      "DataFilters" -> seqToString(dataFilters),
      "Location" -> locationDesc)
  }

  lazy val inputRDD: RDD[InternalRow] = {
    // invoke a whole stage transformer
    val glutenPipeline = WholeStageTransformer(glutenScan, materializeInput = false)(1)
    logInfo(s"Using Velox to read parquet: ${metadata.toString()}")
    logInfo(s"SubstraitPlan: ${glutenPipeline.substraitPlanJson}")
    logInfo(s"NativePlan for VeloxParquetScan: ${glutenPipeline.nativePlanString(true)}")

    val glutenScanRDD = glutenPipeline.doExecuteColumnar()

    // Narrow down all used metrics on the Executor side, in case transferring unnecessary metrics
    val embeddedMetrics = {
      val mapBuilder = mutable.Map.empty[String, GpuMetric]
      if (rapidsConf.enableNativeVeloxConverter) {
        commonMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
        nativeMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
        if (rapidsConf.parquetVeloxPreloadCapacity > 0) {
          preloadingMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
        }
      } else {
        commonMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
        roundTripMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
      }
      mapBuilder.toMap
    }

    new VeloxParquetScanRDD(glutenScanRDD,
      output,
      requiredSchema,
      TargetSize(coalesceSizeGoal),
      embeddedMetrics,
      useNativeConverter = rapidsConf.enableNativeVeloxConverter,
      preloadedCapacity = rapidsConf.parquetVeloxPreloadCapacity)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  private val commonMetrics = Map[String, () => GpuMetric](
    "VeloxScanTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "VeloxScanTime")),
    "GpuAcquireTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "GpuAcquireTime")),
  )
  private val nativeMetrics = Map[String, () => GpuMetric](
    "C2COutputBatches" -> (() => createMetric(MODERATE_LEVEL, "C2COutputBatches")),
    "VeloxOutputBatches" -> (() => createMetric(MODERATE_LEVEL, "VeloxOutputBatches")),
    "C2COutputSize" -> (() => createSizeMetric(MODERATE_LEVEL, "C2COutputSize")),
    "C2CTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "C2CTime")),
    "C2CStreamTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "C2CStreamTime")),
    "PageableH2DTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "PageableH2DTime")),
    "PinnedH2DTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "PinnedH2DTime")),
    "PageableH2DSize" -> (() => createSizeMetric(MODERATE_LEVEL, "PageableH2DSize")),
    "PinnedH2DSize" -> (() => createSizeMetric(MODERATE_LEVEL, "PinnedH2DSize")),
  )
  private val preloadingMetrics = Map[String, () => GpuMetric](
    "preloadWaitTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "preloadWaitTime"))
  )
  private val roundTripMetrics = Map[String, () => GpuMetric](
    "C2ROutputRows" -> (() => createMetric(MODERATE_LEVEL, "C2ROutputRows")),
    "C2ROutputBatches" -> (() => createMetric(MODERATE_LEVEL, "C2ROutputBatches")),
    "R2CInputRows" -> (() => createMetric(MODERATE_LEVEL, "R2CInputRows")),
    "R2COutputRows" -> (() => createMetric(MODERATE_LEVEL, "R2COutputRows")),
    "R2COutputBatches" -> (() => createMetric(MODERATE_LEVEL, "R2COutputBatches")),
    "VeloxC2RTime" -> (() => createTimingMetric(MODERATE_LEVEL, "VeloxC2RTime")),
    "R2CTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "R2CTime")),
    "R2CStreamTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "R2CStreamTime")),
  )

  override lazy val allMetrics: Map[String, GpuMetric] = {
    val mapBuilder = Map.newBuilder[String, GpuMetric]
    mapBuilder += "scanTime" -> createTimingMetric(ESSENTIAL_LEVEL, "TotalTime")
    // Add common embedded metrics
    commonMetrics.foreach { case (key, generator) =>
      mapBuilder += key -> generator()
    }
    // NativeConverter and RoundTripConverter uses different metrics
    if (rapidsConf.enableNativeVeloxConverter) {
      nativeMetrics.foreach { case (key, generator) =>
        mapBuilder += key -> generator()
      }
      if (rapidsConf.parquetVeloxPreloadCapacity > 0) {
        preloadingMetrics.foreach { case (key, generator) =>
          mapBuilder += key -> generator()
        }
      }
    } else {
      roundTripMetrics.foreach { case (key, generator) =>
        mapBuilder += key -> generator()
      }
    }
    // Expose all metrics of the underlying GlutenScanExec
    glutenScan.metrics.foreach { case (key, metric) =>
      mapBuilder += s"GLUTEN_$key" -> GpuMetric.wrap(metric)
    }
    mapBuilder.result()
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val scanTime = gpuLongMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          val startNs = System.nanoTime()
          val hasNext = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          hasNext
        }

        override def next(): ColumnarBatch = {
          val startNs = System.nanoTime()
          val batch = batches.next()
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          batch
        }
      }
    }
  }

  override def doCanonicalize(): VeloxFileSourceScanExec = {
    VeloxFileSourceScanExec(
      relation,
      originalOutput.map(QueryPlan.normalizeExpressions(_, originalOutput)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), originalOutput),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, originalOutput),
      None,
      queryUsesInputFile,
      alluxioPathsMap = alluxioPathsMap)(rapidsConf)
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  private def filterUnusedDynamicPruningExpressions(predicates: Seq[Expression]) = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  override def otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)

}
