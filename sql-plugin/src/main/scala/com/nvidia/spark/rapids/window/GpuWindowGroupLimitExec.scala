package com.nvidia.spark.rapids.window

import ai.rapids.cudf
import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuBindReferences, GpuColumnVector, GpuExec, GpuExpression, GpuOverrides, GpuProjectExec, RapidsConf, RapidsMeta, SparkPlanMeta}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, RowNumber, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.{WindowGroupLimitExec, WindowGroupLimitMode}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuWindowGroupLimitExecMeta(limitExec: WindowGroupLimitExec,
                                  conf: RapidsConf,
                                  parent:Option[RapidsMeta[_, _, _]],
                                  rule: DataFromReplacementRule)
    extends SparkPlanMeta[WindowGroupLimitExec](limitExec, conf, parent, rule) {

  private val partitionSpec: Seq[BaseExprMeta[Expression]] =
    limitExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    limitExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val rankFunction: BaseExprMeta[Expression] =
    GpuOverrides.wrapExpr(limitExec.rankLikeFunction, conf, Some(this))

  override def tagPlanForGpu(): Unit = {
    wrapped.rankLikeFunction match {
      // case DenseRank(_) =>
      // case Rank(_) =>
      case RowNumber() =>
      case _ => willNotWorkOnGpu("Only RowNumber is currently supported for group limits")
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuWindowGroupLimitExec(partitionSpec.map(_.convertToGpu()),
                            orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
                            rankFunction.convertToGpu(),
                            limitExec.limit,
                            limitExec.mode,
                            childPlans.head.convertIfNeeded())
  }
}

class GpuWindowGroupLimitingIterator(input: Iterator[ColumnarBatch],
                                     override val boundPartitionSpec: Seq[GpuExpression],
                                     override val boundOrderSpec: Seq[SortOrder],
                                     boundRankFunction: GpuExpression)
  extends Iterator[ColumnarBatch]
  with BasicWindowCalc
  with Logging {

  override val boundWindowOps: Seq[GpuExpression] = Seq(boundRankFunction)
  override def hasNext: Boolean = input.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }

    println(s"CALEB: Number of boundOrderSpec: ${boundOrderSpec.length}")

    // TODO: Make spillable, again.
    withResource(input.next()) { cb =>
      withResource(GpuProjectExec.project(cb, boundPartitionSpec)) { partsCol =>
        GpuColumnVector.debug("Here's the extracted parts column: ", partsCol)
        // TODO: opTime NvtxWithMetrics.
        withResource(computeBasicWindow(cb)) { outputCols =>
          withResource(new cudf.Table(outputCols: _*)) { outputTable =>
            cudf.TableDebug.get().debug("OutputTable: ", outputTable)
          }
        }
      }
    }

    throw new UnsupportedOperationException("TODO: Implement GpuWindowGroupLimitIterator!")
  }

  // To calculate per-ColumnarBatch limits, we don't need running-window optimization.
  override def isRunningBatched: Boolean = false
}

case class GpuWindowGroupLimitExec(
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output

  // Bindings.

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // TODO: Populate metrics.
    //    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    //    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    //    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, output)
    val boundRankFunction = GpuBindReferences.bindGpuReference(rankLikeFunction, output)

    child.executeColumnar().mapPartitions { input =>
      new GpuWindowGroupLimitingIterator(input,
                                         boundPartitionSpec,
                                         boundOrderSpec,
                                         boundRankFunction)
    }

  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("Row-wise execution unsupported!")
}
