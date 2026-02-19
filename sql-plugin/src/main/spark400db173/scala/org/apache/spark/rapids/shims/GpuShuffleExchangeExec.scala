/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuMetric, GpuPartitioning}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveRepartitioningStatus
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.createAdditionalExchangeMetrics
import org.apache.spark.sql.rapids.execution.ShuffledBatchRDD

case class GpuShuffleExchangeExec(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin)(
    cpuOutputPartitioning: Partitioning)
  extends GpuDatabricksShuffleExchangeExecBase(gpuOutputPartitioning, child, shuffleOrigin)(
    cpuOutputPartitioning) {

  // DB SPECIFIC: ShuffleExchangeLike in DBR 17.3 extends several Databricks-specific traits
  // (ShuffleSkewnessMetrics, AdaptiveRepartitioningMetrics, EnsureRequirementsDPMetrics,
  // AdaptiveSpillFallbackMetrics, AutoOptimizedShuffleMetrics) that define metrics accessed
  // via SparkPlan.metrics. Since GpuExec.metrics is final and backed by allMetrics, we must
  // include these DB-specific metrics in additionalMetrics so they appear in the metrics map.
  override lazy val additionalMetrics: Map[String, GpuMetric] = {
    // Base metrics from GpuShuffleExchangeExecBase.additionalMetrics
    // (cannot use super on lazy val in Scala)
    createAdditionalExchangeMetrics(this) ++
      GpuMetric.wrap(readMetrics) ++
      GpuMetric.wrap(
        SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)) ++
      // DBR 17.3 specific metrics from ShuffleExchangeLike's parent traits
      GpuMetric.wrap(skewMetrics) ++
      GpuMetric.wrap(spillFallbackMetrics) ++
      GpuMetric.wrap(ensReqDPMetrics) ++
      GpuMetric.wrap(adpMetrics) ++
      GpuMetric.wrap(aosMetrics)
  }

  // Databricks 17.3: Added stageShuffleCount parameter
  override def getShuffleRDD(
      partitionSpecs: Array[ShufflePartitionSpec],
      lazyFetching: Boolean,
      stageShuffleCount: Int): RDD[_] = {
    new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics, partitionSpecs)
  }

  // DB SPECIFIC: In Databricks ShuffleExchangeExec, targetOutputPartitioning is the first
  // constructor parameter (the CPU partitioning). Our GPU version stores this as
  // cpuOutputPartitioning.
  override def targetOutputPartitioning: Partitioning = cpuOutputPartitioning

  // DB SPECIFIC: In Databricks ShuffleExchangeExec, adaptiveRepartitioningStatus is a case class
  // parameter defaulting to AdaptiveRepartitioningStatus.DEFAULT_STATUS. AQE calls this to check
  // the current repartitioning state. Returning DEFAULT_STATUS indicates no repartitioning has
  // occurred.
  def adaptiveRepartitioningStatus(): AdaptiveRepartitioningStatus = {
    AdaptiveRepartitioningStatus.DEFAULT_STATUS
  }

  // DB SPECIFIC: In Databricks ShuffleExchangeExec, withNewNumPartitions creates a copy with
  // updated partitioning and copies tags from the original.
  override def withNewNumPartitions(numPartitions: Int): ShuffleExchangeLike = {
    val newCpuPartitioning = cpuOutputPartitioning.withNewNumPartitions(numPartitions)
    val newExec = copy(gpuOutputPartitioning, child, shuffleOrigin)(newCpuPartitioning)
    newExec.copyTagsFrom(this)
    newExec
  }

  // DB SPECIFIC: In Databricks ShuffleExchangeExec, repartition creates a copy with new
  // partition count and updated AdaptiveRepartitioningStatus. Since our GPU version doesn't
  // store the status as a field, we create a new instance with the updated partitioning.
  // The status is not persisted but AQE should still function correctly.
  def repartition(numPartitions: Int, updatedRepartitioningStatus: AdaptiveRepartitioningStatus):
     ShuffleExchangeLike = {
    val newCpuPartitioning = cpuOutputPartitioning.withNewNumPartitions(numPartitions)
    copy(gpuOutputPartitioning, child, shuffleOrigin)(newCpuPartitioning)
  }

  // DB SPECIFIC - not sure how it is used, so try to return one at first.
  // For more details, refer to https://github.com/NVIDIA/spark-rapids/issues/13242.
  override val ensReqDPMetricTag: TreeNodeTag[Int] = TreeNodeTag[Int]("GpuShuffleExchangeExec")
}
