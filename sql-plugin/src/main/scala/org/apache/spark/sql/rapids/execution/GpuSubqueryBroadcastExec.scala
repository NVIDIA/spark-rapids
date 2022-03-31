/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuColumnarToRowExec, GpuExec, GpuMetric, RapidsConf, RapidsMeta, SparkPlanMeta, TargetSize}
import com.nvidia.spark.rapids.GpuMetric.{COLLECT_TIME, DESCRIPTION_COLLECT_TIME, ESSENTIAL_LEVEL}
import com.nvidia.spark.rapids.shims.{ShimUnaryExecNode, SparkShimImpl}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Cast, Expression, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{BaseSubqueryExec, SparkPlan, SQLExecution, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.util.ThreadUtils


class GpuSubqueryBroadcastMeta(
    s: SubqueryBroadcastExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends
    SparkPlanMeta[SubqueryBroadcastExec](s, conf, p, r) {

  private var broadcastBuilder: () => SparkPlan = _

  override val childExprs: Seq[BaseExprMeta[_]] = Nil

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Nil

  override def tagPlanForGpu(): Unit = s.child match {

    // For AQE off:
    //
    // The rule PlanDynamicPruningFilters will insert SubqueryBroadcast if there exists
    // available broadcast exchange for reuse. The plan stack of SubqueryBroadcast:
    //
    // +- SubqueryBroadcast
    //    +- BroadcastExchange (can be reused)
    //       +- [executed subquery...]
    //
    // Since the GPU overrides rule has been applied on executedSubQuery, if the
    // executedSubQuery can be replaced by GPU overrides, the plan stack becomes:
    //
    // +- SubqueryBroadcast
    //    +- BroadcastExchange
    //       +- GpuColumnarToRow
    //          +- [GPU overrides of executed subquery...]
    //
    // To reuse BroadcastExchange on the GPU, we shall transform above pattern into:
    //
    // +- GpuSubqueryBroadcast
    //    +- GpuBroadcastExchange (can be reused)
    //       +- [GPU overrides of executed subquery...]
    //
    case ex @ BroadcastExchangeExec(_, c2r: GpuColumnarToRowExec) =>
      val exMeta = new GpuBroadcastMeta(ex.copy(child = c2r.child), conf, p, r)
      exMeta.tagForGpu()
      if (exMeta.canThisBeReplaced) {
        broadcastBuilder = () => exMeta.convertToGpu()
      } else {
        willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
      }

    // For AQE on:
    //
    // In Spark 320+, DPP can cooperate with AQE. The insertion of SubqueryBroadcast is
    // similar with non-AQE circumstance. During the creation of AdaptiveSparkPlan, the
    // rule PlanAdaptiveSubqueries insert an intermediate plan SubqueryAdaptiveBroadcast to
    // preserve the initial physical plan of DPP subquery filters. During the optimization,
    // the rule PlanAdaptiveDynamicPruningFilters inserts the SubqueryBroadcast as the parent
    // of adaptive subqueries:
    //
    // +- SubqueryBroadcast
    //    +- AdaptiveSparkPlan (supportColumnar=false)
    //    +- == Initial Plan ==
    //       BroadcastExchange
    //       +- [executed subquery...]
    //
    // Since AdaptiveSparkPlan can be explicitly set as a columnar plan from Spark 320+,
    // we can simply build GpuSubqueryBroadcast on the base of columnar adaptive plans
    // whose root plan are GpuBroadcastExchange:
    //
    // +- GpuSubqueryBroadcast
    //    +- AdaptiveSparkPlan (supportColumnar=true)
    //    +- == Final Plan ==
    //       BroadcastQueryStage
    //       +- GpuBroadcastExchange (can be reused)
    //          +- [GPU overrides of executed subquery...]
    //
    case a: AdaptiveSparkPlanExec =>
      SparkShimImpl.getAdaptiveInputPlan(a) match {
        case ex: BroadcastExchangeExec =>
          val exMeta = new GpuBroadcastMeta(ex, conf, p, r)
          exMeta.tagForGpu()
          if (exMeta.canThisBeReplaced) {
            broadcastBuilder = () =>
              SparkShimImpl.columnarAdaptivePlan(
                a, TargetSize(conf.gpuTargetBatchSizeBytes))
          } else {
            willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
          }
        case _ =>
          throw new AssertionError("should not reach here")
      }

    case _ =>
      willNotWorkOnGpu("the subquery to broadcast can not entirely run in the GPU.")
  }

  /**
   * Simply returns the original plan. Because its only child, BroadcastExchange, doesn't
   * need to change if SubqueryBroadcastExec falls back to the CPU.
   */
  override def convertToCpu(): SparkPlan = s

  override def convertToGpu(): GpuExec = {
    GpuSubqueryBroadcastExec(s.name, s.index, s.buildKeys, broadcastBuilder())
  }
}


case class GpuSubqueryBroadcastExec(
    name: String,
    index: Int,
    buildKeys: Seq[Expression],
    child: SparkPlan) extends BaseSubqueryExec with GpuExec with ShimUnaryExecNode {

  // As `SubqueryBroadcastExec`, `GpuSubqueryBroadcastExec` is only used with `InSubqueryExec`.
  // No one would reference this output, so the exprId doesn't matter here. But it's important to
  // correctly report the output length, so that `InSubqueryExec` can know it's the single-column
  // execution mode, not multi-column.
  override def output: Seq[Attribute] = {
    val key = buildKeys(index)
    val name = key match {
      case n: NamedExpression =>
        n.name
      case cast: Cast if cast.child.isInstanceOf[NamedExpression] =>
        cast.child.asInstanceOf[NamedExpression].name
      case _ =>
        "key"
    }
    Seq(AttributeReference(name, key.dataType, key.nullable)())
  }

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    COLLECT_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_COLLECT_TIME))

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    GpuSubqueryBroadcastExec("dpp", index, keys, child.canonicalized)
  }

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkSession, executionId) {
        val beforeCollect = System.nanoTime()

        val serBatch = child.executeBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

        // Creates projection to extract target field from Row, as what Spark does.
        val rowProject = {
          val extractKey = BoundReference(
            index, buildKeys(index).dataType, buildKeys(index).nullable)
          UnsafeProjection.create(extractKey)
        }

        // Deserializes the batch on the host. Then, transforms it to rows and performs row-wise
        // projection. We should NOT run any device operation on the driver node.
        val result = withResource(serBatch.value.hostBatches) { hostBatches =>
          hostBatches.flatMap { cb =>
            cb.rowIterator().asScala
              .map(rowProject(_).copy().asInstanceOf[InternalRow])
          }
        }

        gpuLongMetric("dataSize") += serBatch.value.dataSize
        gpuLongMetric(COLLECT_TIME) += System.nanoTime() - beforeCollect

        result
      }
    }(GpuSubqueryBroadcastExec.executionContext)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuSubqueryBroadcastExec does not support the execute() code path.")
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }
}

object GpuSubqueryBroadcastExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("dynamicpruning", 16))
}
