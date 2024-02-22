/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuSubqueryBroadcastExec}

class FileSourceScanExecMeta(plan: FileSourceScanExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[FileSourceScanExec](plan, conf, parent, rule) with Logging {

  // Replaces SubqueryBroadcastExec inside dynamic pruning filters with GPU counterpart
  // if possible. Instead regarding filters as childExprs of current Meta, we create
  // a new meta for SubqueryBroadcastExec. The reason is that the GPU replacement of
  // FileSourceScan is independent from the replacement of the partitionFilters. It is
  // possible that the FileSourceScan is on the CPU, while the dynamic partitionFilters
  // are on the GPU. And vice versa. The same applies for dataFilters in the case of
  // Dynamic File Pruning
  private def convertBroadcast(bc: SubqueryBroadcastExec): BaseSubqueryExec = {
    val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
    meta.tagForExplain()
    if (conf.shouldExplain) {
      val explain = meta.explain(conf.shouldExplainAll)
      if (explain.nonEmpty) {
        logWarning(s"\n$explain")
      }
    }
    val converted = meta.convertIfNeeded()
    // Because the PlanSubqueries rule is not called (and does not work as expected),
    // we might actually have to fully convert the subquery plan as the plugin would
    // intend (in this case calling GpuTransitionOverrides to insert GpuCoalesceBatches,
    // etc.) to match the other side of the join to reuse the BroadcastExchange.
    // This happens when SubqueryBroadcast has the original (Gpu)BroadcastExchangeExec
    converted match {
      case e: GpuSubqueryBroadcastExec => e.child match {
        // If the GpuBroadcastExchange is here, then we will need to run the transition
        // overrides here
        case _: GpuBroadcastExchangeExec =>
          var updated = ApplyColumnarRulesAndInsertTransitions(Seq(), true)
              .apply(converted)
          updated = (new GpuTransitionOverrides()).apply(updated)
          updated match {
            case h: GpuBringBackToHost =>
              h.child.asInstanceOf[BaseSubqueryExec]
            case c2r: GpuColumnarToRowExec =>
              c2r.child.asInstanceOf[BaseSubqueryExec]
            case _: GpuSubqueryBroadcastExec =>
              updated.asInstanceOf[BaseSubqueryExec]
          }
        // Otherwise, if this SubqueryBroadcast is using a ReusedExchange, then we don't
        // do anything further
        case _: ReusedExchangeExec =>
          converted.asInstanceOf[BaseSubqueryExec]
      }
      case _ =>
        converted.asInstanceOf[BaseSubqueryExec]
    }
  }

  private def convertDynamicPruningFilters(filters: Seq[Expression]): Seq[Expression] = {
    filters.map { filter =>
      filter.transformDown {
        case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
            case reuse @ ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
            case _ =>
              dpe
          }
      }
    }
  }

  // Support partitionFilters in Dynamic Partition Pruning
  private lazy val partitionFilters = convertDynamicPruningFilters(wrapped.partitionFilters)

  // Support dataFilters in Dynamic File Pruning
  private lazy val dataFilters = convertDynamicPruningFilters(wrapped.dataFilters)

  // partition filters and data filters are not run on the GPU
  override val childExprs: Seq[ExprMeta[_]] = Seq.empty

  override def tagPlanForGpu(): Unit = {
    // this is very specific check to have any of the Delta log metadata queries
    // fallback and run on the CPU since there is some incompatibilities in
    // Databricks Spark and Apache Spark.
    if (wrapped.relation.fileFormat.isInstanceOf[JsonFileFormat] &&
        wrapped.relation.location.getClass.getCanonicalName() ==
            "com.databricks.sql.transaction.tahoe.DeltaLogFileIndex") {
      this.entirePlanWillNotWork("Plans that read Delta Index JSON files can not run " +
          "any part of the plan on the GPU!")
    }
    ScanExecShims.tagGpuFileSourceScanExecSupport(this)
  }

  override def convertToCpu(): SparkPlan = {
    val cpu = wrapped.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
    cpu.copyTagsFrom(wrapped)
    cpu
  }

  override def convertToGpu(): GpuExec = {
    val sparkSession = wrapped.relation.sparkSession
    val options = wrapped.relation.options
    val (location, alluxioPathsToReplaceMap) =
      if (AlluxioCfgUtils.enabledAlluxioReplacementAlgoConvertTime(conf)) {
        val shouldReadFromS3 = wrapped.relation.location match {
          // Only handle InMemoryFileIndex
          //
          // skip handle `MetadataLogFileIndex`, from the description of this class:
          // it's about the files generated by the `FileStreamSink`.
          // The streaming data source is not in our scope.
          //
          // For CatalogFileIndex and FileIndex of `delta` data source,
          // need more investigation.
          case inMemory: InMemoryFileIndex =>
            // List all the partitions to reduce overhead, pass in 2 empty filters.
            // Subsequent process will do the right partition pruning.
            // This operation is fast, because it lists files from the caches and the caches
            // already exist in the `InMemoryFileIndex`.
            val pds = inMemory.listFiles(Seq.empty, Seq.empty)
            AlluxioUtils.shouldReadDirectlyFromS3(conf, pds)
          case _ =>
            false
        }

        if (!shouldReadFromS3) {
          // it's convert time algorithm and some paths are not large tables
          AlluxioUtils.replacePathIfNeeded(
            conf,
            wrapped.relation,
            partitionFilters,
            dataFilters)
        } else {
          // convert time algorithm and read large files
          (wrapped.relation.location, None)
        }
      } else {
        // it's not convert time algorithm or read large files, do not replace
        (wrapped.relation.location, None)
      }

    val newRelation = HadoopFsRelation(
      location,
      wrapped.relation.partitionSchema,
      wrapped.relation.dataSchema,
      wrapped.relation.bucketSpec,
      GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
      options)(sparkSession)

    GpuFileSourceScanExec(
      newRelation,
      wrapped.output,
      wrapped.requiredSchema,
      partitionFilters,
      wrapped.optionalBucketSet,
      // TODO: Does Databricks have coalesced bucketing implemented?
      None,
      dataFilters,
      wrapped.tableIdentifier,
      wrapped.disableBucketedScan,
      queryUsesInputFile = false,
      alluxioPathsToReplaceMap)(conf)
  }
}
