/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Scalar

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, AttributeSet, CurrentRow, Expression, NamedExpression, RowFrame, SortOrder, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Base class for GPU Execs that implement window functions. This abstracts the method
 * by which the window function's input expressions, partition specs, order-by specs, etc.
 * are extracted from the specific WindowExecType.
 *
 * @tparam WindowExecType The Exec class that implements window functions
 *                        (E.g. o.a.s.sql.execution.window.WindowExec.)
 */
abstract class GpuBaseWindowExecMeta[WindowExecType <: SparkPlan] (windowExec: WindowExecType,
                        conf: RapidsConf,
                        parent: Option[RapidsMeta[_, _, _]],
                        rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowExecType](windowExec, conf, parent, rule) with Logging {

  /**
   * Extracts window-expression from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getInputWindowExpressions: Seq[NamedExpression]

  /**
   * Extracts partition-spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getPartitionSpecs: Seq[Expression]

  /**
   * Extracts order-by spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getOrderSpecs: Seq[SortOrder]

  /**
   * Indicates the output column semantics for the WindowExecType,
   * i.e. whether to only return the window-expression result columns (as in some Spark
   * distributions) or also include the input columns (as in Apache Spark).
   */
  def getResultColumnsOnly: Boolean

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    getInputWindowExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    getPartitionSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    getOrderSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  lazy val inputFields: Seq[BaseExprMeta[Attribute]] =
    windowExec.children.head.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
        "cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  override def convertToGpu(): GpuExec = {
    val resultColumnsOnly = getResultColumnsOnly
    val gpuWindowExpressions = if (resultColumnsOnly) {
      windowExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression])
    } else {
      (inputFields ++ windowExpressions).map(_.convertToGpu().asInstanceOf[NamedExpression])
    }

    val (pre, windowOps, post) = GpuWindowExec.splitAndDedup(gpuWindowExpressions)
    // Order is not important for pre. It is unbound and we are inserting it in.
    val isPreNeeded =
      (AttributeSet(pre.map(_.toAttribute)) -- windowExec.children.head.output).nonEmpty
    // To check if post is needed we first have to remove a layer of indirection that
    // might not be needed. Here we want to maintain order, just to match Spark as closely
    // as possible
    val remappedWindowOps = GpuWindowExec.remapAttributes(windowOps, post)
    val isPostNeeded = remappedWindowOps.length != post.length ||
        remappedWindowOps.zip(post).exists {
          case (w, p) => w.exprId != p.exprId
        }
    val fixedUpWindowOps = if(isPostNeeded) {
      windowOps
    } else {
      remappedWindowOps
    }

    // When we support multiple ways to avoid batching the input data like with
    // https://github.com/NVIDIA/spark-rapids/issues/1860 we should check if all of
    // the operations fit into one of the supported groups and then split them up into
    // multiple execs if they do, so that we can avoid batching on all of them.
    val allBatchedRunning = fixedUpWindowOps.forall {
      case GpuAlias(GpuWindowExpression(func, spec), _) =>
        val isRunningFunc = func match {
          case _: GpuBatchedRunningWindowFunction[_] => true
          case GpuAggregateExpression(_: GpuBatchedRunningWindowFunction[_], _, _, _ , _) => true
          case _ => false
        }
        // Running windows are limited to row based queries with a few changes we could make this
        // work for range based queries too https://github.com/NVIDIA/spark-rapids/issues/2708
        isRunningFunc && GpuWindowExec.isRunningWindow(spec)
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference =>
        // We allow pure result columns for running windows
        true
      case other =>
        // This should only happen if we did something wrong in splitting/deduping
        // the window expressions.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    }

    val input = if (isPreNeeded) {
      GpuProjectExec(pre.toList, childPlans.head.convertIfNeeded())
    } else {
      childPlans.head.convertIfNeeded()
    }

    val windowExpr = if (allBatchedRunning) {
      GpuRunningWindowExec(
        fixedUpWindowOps,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input)
    } else {
      GpuWindowExec(
        fixedUpWindowOps,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input)
    }

    if (isPostNeeded) {
      GpuProjectExec(post.toList, windowExpr)
    } else {
      windowExpr
    }
  }
}

/**
 * Specialization of GpuBaseWindowExecMeta for org.apache.spark.sql.window.WindowExec.
 * This class implements methods to extract the window-expressions, partition columns,
 * order-by columns, etc. from WindowExec.
 */
class GpuWindowExecMeta(windowExec: WindowExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseWindowExecMeta[WindowExec](windowExec, conf, parent, rule) {

  /**
   * Fetches WindowExpressions in input `windowExec`, via reflection.
   * As a byproduct, determines whether to return the original input columns,
   * as part of the output.
   *
   * (Spark versions that use `projectList` expect result columns
   * *not* to include the input columns.
   * Apache Spark expects the input columns, before the aggregation output columns.)
   *
   * @return WindowExpressions within windowExec,
   *         and a boolean, indicating the result column semantics
   *         (i.e. whether result columns should be returned *without* including the
   *         input columns).
   */
  def getWindowExpression: (Seq[NamedExpression], Boolean) = {
    var resultColumnsOnly : Boolean = false
    val expr = try {
      val resultMethod = windowExec.getClass.getMethod("windowExpression")
      resultMethod.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        resultColumnsOnly = true
        val winExpr = windowExec.getClass.getMethod("projectList")
        winExpr.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    }
    (expr, resultColumnsOnly)
  }

  private lazy val (inputWindowExpressions, resultColumnsOnly) = getWindowExpression

  override def getInputWindowExpressions: Seq[NamedExpression] = inputWindowExpressions
  override def getPartitionSpecs: Seq[Expression] = windowExec.partitionSpec
  override def getOrderSpecs: Seq[SortOrder] = windowExec.orderSpec
  override def getResultColumnsOnly: Boolean = resultColumnsOnly
}

object GpuWindowExec extends Arm {
  /**
   * As a part of `splitAndDedup` the dedup part adds a layer of indirection. This attempts to
   * remove that layer of indirection.
   * @param windowOps the windowOps output of splitAndDedup
   * @param post the post output of splitAndDedup
   * @return a version of windowOps that has removed as many un-needed temp aliases as possible.
   */
  def remapAttributes(windowOps: Seq[NamedExpression],
      post: Seq[NamedExpression]): Seq[NamedExpression] = {
    val postRemapping = post.flatMap {
      case a @ GpuAlias(attr: AttributeReference, _) => Some((attr.exprId, a))
      case _ => None
    }.groupBy(_._1)
    windowOps.map {
      case a @ GpuAlias(child, _)
        // We can only replace the mapping if there is one thing to map it to.
        if postRemapping.get(a.exprId).exists(_.length == 1) =>
        val attr = postRemapping(a.exprId).head._2
        GpuAlias(child, attr.name)(attr.exprId, attr.qualifier)
      case other => other
    }
  }

  private def hasGpuWindowFunction(expr: Expression): Boolean =
    expr.find(_.isInstanceOf[GpuWindowExpression]).isDefined

  private def extractAndSave(expr: Expression,
      saved: ArrayBuffer[NamedExpression],
      deduped: mutable.HashMap[Expression, Attribute]): Expression =
    expr match {
      // Don't rename an already named expression
      case ne: NamedExpression =>
        if (!saved.exists(_.exprId == ne.exprId)) {
          saved += ne
        }
        ne.toAttribute
      case e: Expression if e.foldable =>
        e // No need to create an attribute reference if it will be evaluated as a Literal.
      case e: Expression =>
        // For other expressions, we extract it and replace it with an AttributeReference (with
        // an internal column name, e.g. "_gpu_w0"). Deduping it as we go.
        deduped.getOrElseUpdate(e, {
          val withName = GpuAlias(e, s"_gpu_w${saved.length}")()
          saved += withName
          withName.toAttribute
        })
    }

  /**
   * In some distributions expressions passed into WindowExec can have more operations
   * in them than just a WindowExpression wrapped in an GpuAlias. This is a problem if we
   * want to try and do multiple window operations in a single pass to speed things up
   * or if we need to add new transitive window functions when we are doing some memory
   * optimizations, like running window. This will split the input expressions
   * into three sets of expressions. The first set is a project with no window expressions in it at
   * all. The second takes the first as input and will only have aliases to columns in the first or
   * named expressions wrapping a single window function in it. The third uses the second as
   * input and will do any final steps to combine window functions together.
   *
   * For example `SUM(a) - SUM(b + c) over (PARTITION BY d ORDER BY e) as result` would be
   * transformed into
   * <pre>
   * Phase 1 (Pre project):
   * a, b + c as _tmp0, d, e
   *
   * Phase 2 (Window Operations):
   * SUM(a) over (PARTITION BY d ORDER BY e) as _tmp1,
   * SUM(_tmp0) over (PARTITION BY d ORDER BY e) as _tmp2
   *
   * Phase 3 (Post Project):
   * (_tmp1 - _tmp2) as result
   * </pre>
   *
   * This assumes that there is not a window function of another window function, like
   * `LAG(SUM(a), 2)` which appears to be something all distros split apart into separate
   * window operations, so we are good.
   * @param exprs the input expressions to a GpuWindowExec
   */
  def splitAndDedup(exprs: Seq[NamedExpression]):
  (Seq[NamedExpression], Seq[NamedExpression], Seq[NamedExpression]) = {
    // This is based off of similar code in Apache Spark's `ExtractWindowExpressions.extract` but
    // has been highly modified
    val preProject = ArrayBuffer[NamedExpression]()
    val preDedupe = mutable.HashMap[Expression, Attribute]()
    val windowOps = ArrayBuffer[NamedExpression]()
    val windowDedupe = mutable.HashMap[Expression, Attribute]()
    val postProject = ArrayBuffer[NamedExpression]()

    val shims = ShimLoader.getSparkShims

    exprs.foreach { expr =>
      if (hasGpuWindowFunction(expr)) {
        // First pass looks for GpuWindowFunctions and GpuWindowSpecDefinitions to build up
        // the preProject phase
        val firstPass = expr.transformDown {
          case wf: GpuWindowFunction =>
            // All window functions, including those that are also aggregation functions, are
            // wrapped in a GpuWindowExpression, so dedup and save their children into the pre
            // stage, replacing them with aliases.
            val newChildren = wf.children.map(extractAndSave(_, preProject, preDedupe))
            wf.withNewChildren(newChildren)
          case wsc @ GpuWindowSpecDefinition(partitionSpec, orderSpec, _) =>
            // Extracts expressions from the partition spec and order spec to be sure that they
            // show up in the pre stage.  Because map is lazy we are going to force it to be
            // materialized, by forcing it to go through an array that cannot be lazily created
            val newPartitionSpec = partitionSpec.map(
              extractAndSave(_, preProject, preDedupe)).toArray.toSeq
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractAndSave(so.child, preProject, preDedupe)
              shims.sortOrder(newChild, so.direction, so.nullOrdering)
            }.toArray.toSeq
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)
        }
        val secondPass = firstPass.transformDown {
          case we: GpuWindowExpression =>
            // A window Expression holds a window function or an aggregate function, so put it into
            // the windowOps phase, and create a new alias for it for the post phase
            extractAndSave(we, windowOps, windowDedupe)
        }.asInstanceOf[NamedExpression]

        postProject += secondPass
      } else {
        // There is no window function so pass the result through all of the phases (with deduping)
        postProject += extractAndSave(
          extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
            .asInstanceOf[NamedExpression]
      }
    }
    (preProject, windowOps, postProject)
  }

  def isRunningWindow(spec: GpuWindowSpecDefinition): Boolean = spec match {
    case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(RowFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding), GpuSpecialFrameBoundary(CurrentRow))) => true
    case _ => false
  }

  def fixerIndexMap(windowExpressionAliases: Seq[Expression]): Map[Int, BatchedRunningWindowFixer] =
    windowExpressionAliases.zipWithIndex.flatMap {
      case (GpuAlias(GpuWindowExpression(func, _), _), index) =>
        func match {
          case f: GpuBatchedRunningWindowFunction[_] =>
            Some((index, f.newFixer()))
          case GpuAggregateExpression(f: GpuBatchedRunningWindowFunction[_], _, _, _, _) =>
            Some((index, f.newFixer()))
          case _ => None
        }
      case _ => None
    }.toMap

  def computeRunningNoPartitioning(
      iter: Iterator[ColumnarBatch],
      boundWindowOps: Seq[GpuExpression],
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    val fixers = fixerIndexMap(boundWindowOps)
    TaskContext.get().addTaskCompletionListener[Unit](_ => fixers.values.foreach(_.close()))

    iter.flatMap { cb =>
      val numRows = cb.numRows
      numOutputBatches += 1
      numOutputRows += numRows
      withResource(new MetricRange(opTime)) { _ =>
        if (numRows > 0) {
          withResource(GpuProjectExec.projectAndClose(cb, boundWindowOps, NoopMetric)) { full =>
            closeOnExcept(ArrayBuffer[ColumnVector]()) { newColumns =>
              boundWindowOps.indices.foreach { idx =>
                val column = full.column(idx).asInstanceOf[GpuColumnVector]
                fixers.get(idx) match {
                  case Some(fixer) =>
                    closeOnExcept(fixer.fixUp(scala.util.Right(true), column)) { finalOutput =>
                      fixer.updateState(finalOutput)
                      newColumns += finalOutput
                    }
                  case None =>
                    newColumns += column.incRefCount()
                }
              }
              Some(new ColumnarBatch(newColumns.toArray, full.numRows()))
            }
          }
        } else {
          // Now rows so just filter it out
          cb.close()
          None
        }
      }
    }
  }

  private def cudfAnd(lhs: ai.rapids.cudf.ColumnVector,
      rhs: ai.rapids.cudf.ColumnVector): ai.rapids.cudf.ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        lhs.and(rhs)
      }
    }
  }

  private def arePartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[ai.rapids.cudf.ColumnVector]): Either[GpuColumnVector, Boolean] = {
    if (scalars.isEmpty) {
      scala.util.Right(false)
    } else {
      val ret = scalars.zip(columns).map {
        case (scalar, column) => scalar.equalToNullAware(column)
      }.reduce(cudfAnd)
      scala.util.Left(GpuColumnVector.from(ret, BooleanType))
    }
  }

  private def getScalarRow(index: Int, columns: Seq[ai.rapids.cudf.ColumnVector]): Array[Scalar] =
    columns.map(_.getScalarElement(index)).toArray

  def computeRunning(
      iter: Iterator[ColumnarBatch],
      boundWindowOps: Seq[GpuExpression],
      boundPartitionSpec: Seq[Expression],
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    var lastParts: Array[Scalar] = Array.empty
    val fixers = fixerIndexMap(boundWindowOps)

    def saveLastParts(newLastParts: Array[Scalar]): Unit = {
      lastParts.foreach(_.close())
      lastParts = newLastParts
    }

    def closeState(): Unit = {
      saveLastParts(Array.empty)
      fixers.values.foreach(_.close())
    }

    TaskContext.get().addTaskCompletionListener[Unit](_ => closeState())

    iter.map { cb =>
      val numRows = cb.numRows
      numOutputBatches += 1
      numOutputRows += numRows
      withResource(new MetricRange(opTime)) { _ =>
        val fullWindowProjectList = boundWindowOps ++ boundPartitionSpec
        withResource(
          GpuProjectExec.projectAndClose(cb, fullWindowProjectList, NoopMetric)) { full =>
          // part columns are owned by full and do not need to be closed, but should not be used
          // if full is closed
          val partColumns = boundPartitionSpec.indices.map { idx =>
            full.column(idx + boundWindowOps.length).asInstanceOf[GpuColumnVector].getBase
          }

          // We need to fix up the rows that are part of the same batch as the end of the
          // last batch
          val partsEqual = arePartsEqual(lastParts, partColumns)
          try {
            closeOnExcept(ArrayBuffer[ColumnVector]()) { newColumns =>
              boundWindowOps.indices.foreach { idx =>
                val column = full.column(idx).asInstanceOf[GpuColumnVector]
                val fixer = fixers.get(idx)
                if (fixer.isDefined) {
                  val f = fixer.get
                  closeOnExcept(f.fixUp(partsEqual, column)) { finalOutput =>
                    f.updateState(finalOutput)
                    newColumns += finalOutput
                  }
                } else {
                  newColumns += column.incRefCount()
                }
              }
              saveLastParts(getScalarRow(numRows - 1, partColumns))

              new ColumnarBatch(newColumns.toArray, numRows)
            }
          } finally {
            partsEqual match {
              case scala.util.Left(cv) => cv.close()
              case _ => // Nothing
            }
          }
        }
      }
    }
  }
}

trait GpuWindowBaseExec extends UnaryExecNode with GpuExec {
  val windowOps: Seq[NamedExpression]
  val partitionSpec: Seq[Expression]
  val orderSpec: Seq[SortOrder]

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, OP_TIME)
  )

  override def output: Seq[Attribute] = windowOps.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  lazy val partitionOrdering: Seq[SortOrder] = {
    val shims = ShimLoader.getSparkShims
    partitionSpec.map(shims.sortOrder(_, Ascending))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionOrdering ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")
}

case class GpuRunningWindowExec(
    windowOps: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan
) extends GpuWindowBaseExec {

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps =
      GpuBindReferences.bindGpuReferences(windowOps, child.output)

    val boundPartitionSpec =
      GpuBindReferences.bindGpuReferences(partitionSpec, child.output)

    if (partitionSpec.isEmpty) {
      child.executeColumnar().mapPartitions {
        iter => GpuWindowExec.computeRunningNoPartitioning(iter,
          boundWindowOps, numOutputBatches, numOutputRows, opTime)
      }
    } else {
      child.executeColumnar().mapPartitions {
        iter => GpuWindowExec.computeRunning(iter,
          boundWindowOps, boundPartitionSpec, numOutputBatches,
          numOutputRows, opTime)
      }
    }
  }
}

case class GpuWindowExec(
    windowOps: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan
  ) extends GpuWindowBaseExec {

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(outputBatching)

  override def outputBatching: CoalesceGoal = if (partitionSpec.isEmpty) {
    RequireSingleBatch
  } else {
    BatchedByKey(partitionOrdering)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps =
      GpuBindReferences.bindGpuReferences(windowOps, child.output)

    child.executeColumnar().map { cb =>
      numOutputBatches += 1
      numOutputRows += cb.numRows
      GpuProjectExec.projectAndClose(cb, boundWindowOps, opTime)
    }
  }
}
