/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf._
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRestoreOnRetry, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM
import com.nvidia.spark.rapids.shims._

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SampleExec, SparkPlan}
import org.apache.spark.sql.rapids.{GpuPartitionwiseSampledRDD, GpuPoissonSampler}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.random.BernoulliCellSampler

class GpuProjectExecMeta(
    proj: ProjectExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[ProjectExec](proj, conf, p, r)
    with Logging {
  override def convertToGpu(): GpuExec = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    val gpuExprs = childExprs.map(_.convertToGpu().asInstanceOf[NamedExpression]).toList
    val gpuChild = childPlans.head.convertIfNeeded()
    if (conf.isProjectAstEnabled) {
      if (childExprs.forall(_.canThisBeAst)) {
        return GpuProjectAstExec(gpuExprs, gpuChild)
      }
      // explain AST because this is optional and it is sometimes hard to debug
      if (conf.shouldExplain) {
        val explain = childExprs.map(_.explainAst(conf.shouldExplainAll))
            .filter(_.nonEmpty)
        if (explain.nonEmpty) {
          logWarning(s"AST PROJECT\n$explain")
        }
      }
    }
    GpuProjectExec(gpuExprs, gpuChild)
  }
}

object GpuProjectExec {
  def projectAndClose[A <: Expression](cb: ColumnarBatch, boundExprs: Seq[A],
      opTime: GpuMetric): ColumnarBatch = {
    val nvtxRange = new NvtxWithMetrics("ProjectExec", NvtxColor.CYAN, opTime)
    try {
      project(cb, boundExprs)
    } finally {
      cb.close()
      nvtxRange.close()
    }
  }

  @tailrec
  private def extractSingleBoundIndex(expr: Expression): Option[Int] = expr match {
    case ga: GpuAlias => extractSingleBoundIndex(ga.child)
    case br: GpuBoundReference => Some(br.ordinal)
    case _ => None
  }

  def extractSingleBoundIndex(boundExprs: Seq[Expression]): Seq[Option[Int]] =
    boundExprs.map(extractSingleBoundIndex)

  def isNoopProject(cb: ColumnarBatch, boundExprs: Seq[Expression]): Boolean = {
    if (boundExprs.length == cb.numCols()) {
      extractSingleBoundIndex(boundExprs).zip(0 until cb.numCols()).forall {
        case (Some(foundIndex), expectedIndex) => foundIndex == expectedIndex
        case _ => false
      }
    } else {
      false
    }
  }

  def project(cb: ColumnarBatch, boundExprs: Seq[Expression]): ColumnarBatch = {
    if (isNoopProject(cb, boundExprs)) {
      // This can help avoid contiguous splits in some cases when the input data is also contiguous
      GpuColumnVector.incRefCounts(cb)
    } else {
      val newColumns = boundExprs.safeMap(_.columnarEval(cb)).toArray[ColumnVector]
      new ColumnarBatch(newColumns, cb.numRows())
    }
  }

  /**
   * Similar to project, but it will try and retry the operations if it can. It also will close
   * the input SpillableColumnarBatch.
   * @param sb the input batch
   * @param boundExprs the expressions to run
   * @return the resulting batch
   */
  def projectAndCloseWithRetrySingleBatch(sb: SpillableColumnarBatch,
      boundExprs: Seq[Expression]): ColumnarBatch = {
    withResource(sb) { _ =>
      projectWithRetrySingleBatch(sb, boundExprs)
    }
  }

  /**
   * Similar to project, but it will try and retry the operations if it can.  The caller is
   * responsible for closing the input batch.
   * @param sb the input batch
   * @param boundExprs the expressions to run
   * @return the resulting batch
   */
  def projectWithRetrySingleBatch(sb: SpillableColumnarBatch,
      boundExprs: Seq[Expression]): ColumnarBatch = {

    // First off we want to find/run all of the expressions that are not retryable,
    // These cannot be retried.
    val (retryableExprs, notRetryableExprs) = boundExprs.partition(
      _.asInstanceOf[GpuExpression].retryable)
    val retryables = GpuExpressionsUtils.collectRetryables(retryableExprs)

    val snd = if (notRetryableExprs.nonEmpty) {
      withResource(sb.getColumnarBatch()) { cb =>
        Some(SpillableColumnarBatch(project(cb, notRetryableExprs),
          SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      }
    } else {
      None
    }

    withResource(snd) { snd =>
      retryables.foreach(_.checkpoint())
      RmmRapidsRetryIterator.withRetryNoSplit {
        val deterministicResults = withResource(sb.getColumnarBatch()) { cb =>
          withRestoreOnRetry(retryables) {
            // For now we are just going to run all of these and deal with losing work...
            project(cb, retryableExprs)
          }
        }
        if (snd.isEmpty) {
          // We are done and the order should be the same so we don't need to do anything...
          deterministicResults
        } else {
          // There was a mix of deterministic and non-deterministic...
          withResource(deterministicResults) { _ =>
            withResource(snd.get.getColumnarBatch()) { nd =>
              var ndAt = 0
              var detAt = 0
              val outputColumns = ArrayBuffer[ColumnVector]()
              boundExprs.foreach { expr =>
                if (expr.deterministic) {
                  outputColumns += deterministicResults.column(detAt)
                  detAt += 1
                } else {
                  outputColumns += nd.column(ndAt)
                  ndAt += 1
                }
              }
              GpuColumnVector.incRefCounts(new ColumnarBatch(outputColumns.toArray, sb.numRows()))
            }
          }
        }
      }
    }
  }
}

object GpuProjectExecLike {
  def unapply(plan: SparkPlan): Option[(Seq[Expression], SparkPlan)] = plan match {
    case gpuProjectLike: GpuProjectExecLike =>
      Some((gpuProjectLike.projectList, gpuProjectLike.child))
    case _ => None
  }
}

trait GpuProjectExecLike extends ShimUnaryExecNode with GpuExec {

  def projectList: Seq[Expression]

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)
}

/**
 * An iterator that is intended to split the input to or output of a project on rows.
 * In practice this is only used for splitting the input prior to a project in some
 * very special cases. If the projected size of the output is so large that it would
 * risk us not being able to split it later on if we ran into trouble.
 * @param iter the input iterator of columnar batches.
 * @param schema the schema of that input so we can make things spillable if needed
 * @param opTime metric for how long this took
 * @param numSplitsMetric metric for the number of splits that happened.
 */
abstract class AbstractProjectSplitIterator(iter: Iterator[ColumnarBatch],
    schema: Array[DataType],
    opTime: GpuMetric,
    numSplitsMetric: GpuMetric) extends Iterator[ColumnarBatch] {
  private[this] val pending = new scala.collection.mutable.Queue[SpillableColumnarBatch]()

  override def hasNext: Boolean = pending.nonEmpty || iter.hasNext

  protected def calcNumSplits(cb: ColumnarBatch): Int

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    } else if (pending.nonEmpty) {
      opTime.ns {
        withRetryNoSplit(pending.dequeue()) { sb =>
          sb.getColumnarBatch()
        }
      }
    } else {
      val cb = iter.next()
      opTime.ns {
        val numSplits = closeOnExcept(cb) { cb =>
          calcNumSplits(cb)
        }
        if (numSplits <= 1) {
          cb
        } else {
          // this should never happen but it is here just in case
          require(cb.numCols() > 0,
            "About to perform cuDF table operations with a rows-only batch.")
          numSplitsMetric += numSplits - 1
          val sb = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          val tables = withRetryNoSplit(sb) { sb =>
            withResource(sb.getColumnarBatch()) { cb =>
              withResource(GpuColumnVector.from(cb)) { table =>
                val rows = table.getRowCount.toInt
                val rowsPerSplit = math.ceil(rows.toDouble / numSplits).toInt
                val splitIndexes = rowsPerSplit until rows by rowsPerSplit
                table.contiguousSplit(splitIndexes: _*)
              }
            }
          }
          withResource(tables) { tables =>
            (1 until tables.length).foreach { ix =>
              val tbl = tables(ix)
              tables(ix) = null // everything but the head table will be nulled, queued
                                // as spillable batches in `pending`
              pending.enqueue(
                SpillableColumnarBatch(tbl, schema, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
            }
            GpuColumnVector.from(tables.head.getTable, schema)
          }
        }
      }
    }
  }
}

object PreProjectSplitIterator {
  def calcMinOutputSize(cb: ColumnarBatch, boundExprs: GpuTieredProject): Long = {
    val numRows = cb.numRows()
    boundExprs.outputTypes.zipWithIndex.map {
      case (dataType, index) =>
        if (GpuBatchUtils.isFixedWidth(dataType)) {
          GpuBatchUtils.minGpuMemory(dataType, true, numRows)
        } else {
          boundExprs.getPassThroughIndex(index).map { inputIndex =>
            cb.column(inputIndex).asInstanceOf[GpuColumnVector].getBase.getDeviceMemorySize
          }.getOrElse {
            GpuBatchUtils.minGpuMemory(dataType, true, numRows)
          }
        }
    }.sum
  }
}

/**
 * An iterator that can be used to split the input of a project before it happens to prevent
 * situations where the output could not be split later on. In testing we tried to see what
 * would happen if we split it to the target batch size, but there was a very significant
 * performance degradation when that happened. For now this is only used in a few specific
 * places and not everywhere.  In the future this could be extended, but if we do that there
 * are some places where we don't want a split, like a project before a window operation.
 * @param iter the input iterator of columnar batches.
 * @param schema the schema of that input so we can make things spillable if needed
 * @param boundExprs the bound project so we can get a good idea of the output size.
 * @param opTime metric for how long this took
 * @param numSplits the number of splits that happened.
 */
class PreProjectSplitIterator(
    iter: Iterator[ColumnarBatch],
    schema: Array[DataType],
    boundExprs: GpuTieredProject,
    opTime: GpuMetric,
    numSplits: GpuMetric) extends AbstractProjectSplitIterator(iter, schema, opTime, numSplits) {

  // We memoize this parameter here as the value doesn't change during the execution
  // of a SQL query. This is the highest level we can cache at without getting it
  // passed in from the Exec that instantiates this split iterator.
  // NOTE: this is overwritten by tests to trigger various corner cases
  private lazy val splitUntilSize: Double = GpuDeviceManager.getSplitUntilSize.toDouble

  /**
   * calcNumSplit will return the number of splits that we need for the input, in the case
   * that we can detect that a projection using `boundExprs` would expand the output above
   * `GpuDeviceManager.getSplitUntilSize`.
   *
   * @note In the corner case that `cb` is rows-only (no columns), this function returns 0 and the
   * caller must be prepared to handle that case.
   */
  override def calcNumSplits(cb: ColumnarBatch): Int = {
    if (cb.numCols() == 0) {
      0 // rows-only batches should not be split
    } else {
      val minOutputSize = PreProjectSplitIterator.calcMinOutputSize(cb, boundExprs)
      // If the minimum size is too large we will split before doing the project, to help avoid
      // extreme cases where the output size is so large that we cannot split it afterwards.
      math.max(1, math.ceil(minOutputSize / splitUntilSize).toInt)
    }
  }
}

case class GpuProjectExec(
   // NOTE for Scala 2.12.x and below we enforce usage of (eager) List to prevent running
   // into a deep recursion during serde of lazy lists. See
   // https://github.com/NVIDIA/spark-rapids/issues/2036
   //
   // Whereas a similar issue https://issues.apache.org/jira/browse/SPARK-27100 is resolved
   // using an Array, we opt in for List because it implements Seq while having non-recursive
   // serde: https://github.com/scala/scala/blob/2.12.x/src/library/scala/collection/
   //   immutable/List.scala#L516
   projectList: List[NamedExpression],
   child: SparkPlan) extends GpuProjectExecLike {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def internalDoExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val boundProjectList = GpuBindReferences.bindGpuReferencesTiered(projectList, child.output,
      conf)

    val rdd = child.executeColumnar()
    rdd.map { cb =>
      val ret = withResource(new NvtxWithMetrics("ProjectExec", NvtxColor.CYAN, opTime)) { _ =>
        val sb = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        //Note if this ever changes to include splitting the output we need to have an option to not
        // do this for window to work properly.
        boundProjectList.projectAndCloseWithRetrySingleBatch(sb)
      }
      numOutputBatches += 1
      numOutputRows += ret.numRows()
      ret
    }
  }
}

/** Use cudf AST expressions to project columnar batches */
case class GpuProjectAstExec(
    // NOTE for Scala 2.12.x and below we enforce usage of (eager) List to prevent running
    // into a deep recursion during serde of lazy lists. See
    // https://github.com/NVIDIA/spark-rapids/issues/2036
    //
    // Whereas a similar issue https://issues.apache.org/jira/browse/SPARK-27100 is resolved
    // using an Array, we opt in for List because it implements Seq while having non-recursive
    // serde: https://github.com/scala/scala/blob/2.12.x/src/library/scala/collection/
    //   immutable/List.scala#L516
    projectList: List[Expression],
    child: SparkPlan
) extends GpuProjectExecLike {

  override def output: Seq[Attribute] = {
    projectList.collect { case ne: NamedExpression => ne.toAttribute }
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions(buildRetryableAstIterator)
  }

  def buildRetryableAstIterator(
      input: Iterator[ColumnarBatch]): GpuColumnarBatchIterator = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val boundProjectList = GpuBindReferences.bindGpuReferences(projectList, child.output)
    val outputTypes = output.map(_.dataType).toArray
    new GpuColumnarBatchIterator(true) {
      private[this] var maybeSplittedItr: Iterator[ColumnarBatch] = Iterator.empty
      private[this] var compiledAstExprs =
        withResource(new NvtxWithMetrics("Compile ASTs", NvtxColor.ORANGE, opTime)) { _ =>
          boundProjectList.safeMap { expr =>
            // Use intmax for the left table column count since there's only one input table here.
            expr.convertToAst(Int.MaxValue).compile()
          }
        }

      override def hasNext: Boolean = maybeSplittedItr.hasNext || {
        if (input.hasNext) {
          true
        } else {
          close()
          false
        }
      }

      override def next(): ColumnarBatch = {
        if (!maybeSplittedItr.hasNext) {
          val spillable = SpillableColumnarBatch(
            input.next(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          // AST currently doesn't support non-deterministic expressions so it's not needed
          // to check whether compiled expressions are retryable.
          maybeSplittedItr = withRetry(spillable, splitSpillableInHalfByRows) { spillable =>
            withResource(new NvtxWithMetrics("Project AST", NvtxColor.CYAN, opTime)) { _ =>
              withResource(spillable.getColumnarBatch()) { cb =>
                val projectedTable = withResource(tableFromBatch(cb)) { table =>
                  withResource(
                    compiledAstExprs.safeMap(_.computeColumn(table))) { projectedColumns =>
                    new Table(projectedColumns: _*)
                  }
                }
                withResource(projectedTable) { _ =>
                  GpuColumnVector.from(projectedTable, outputTypes)
                }
              }
            }
          }
        }

        val ret = maybeSplittedItr.next()
        numOutputBatches += 1
        numOutputRows += ret.numRows()
        ret
      }

      override def doClose(): Unit = {
        compiledAstExprs.safeClose()
        compiledAstExprs = Nil
      }

      private def tableFromBatch(cb: ColumnarBatch): Table = {
        if (cb.numCols != 0) {
          GpuColumnVector.from(cb)
        } else {
          // Count-only batch but cudf Table cannot be created with no columns.
          // Create the cheapest table we can to evaluate the AST expression.
          withResource(Scalar.fromBool(false)) { falseScalar =>
            withResource(cudf.ColumnVector.fromScalar(falseScalar, cb.numRows())) { falseColumn =>
              new Table(falseColumn)
            }
          }
        }
      }
    }
  }
}

/**
 * Do projections in a tiered fashion, where earlier tiers contain sub-expressions that are
 * referenced in later tiers.  Each tier adds columns to the original batch corresponding
 * to the output of the sub-expressions.  It also removes columns that are no longer needed,
 * based on inputAttrTiers for the current tier and the next tier.
 * Example of how this is processed:
 *   Original projection expressions:
 *   (((a + b) + c) * e), (((a + b) + d) * f), (a + e), (c + f)
 *   Input columns for tier 1: a, b, c, d, e, f  (original projection inputs)
 *   Tier 1: (a + b) as ref1
 *   Input columns for tier 2: a, c, d, e, f, ref1
 *   Tier 2: (ref1 + c) as ref2, (ref1 + d) as ref3
 *   Input columns for tier 3: a, c, e, f, ref2, ref3
 *   Tier 3: (ref2 * e), (ref3 * f), (a + e), (c + f)
 */
 case class GpuTieredProject(exprTiers: Seq[Seq[GpuExpression]]) {
  /**
   * Is everything retryable. This can help with reliability in the common case.
   */
  lazy val areAllRetryable = !exprTiers.exists { tier =>
    tier.exists { expr =>
      !expr.retryable
    }
  }

  lazy val retryables: Seq[Retryable] = exprTiers.flatMap(GpuExpressionsUtils.collectRetryables)

  lazy val outputTypes = exprTiers.last.map(_.dataType).toArray

  private[this] def getPassThroughIndex(tierIndex: Int,
      expr: Expression,
      exprIndex: Int): Option[Int] = expr match {
    case GpuAlias(child, _) =>
      getPassThroughIndex(tierIndex, child, exprIndex)
    case GpuBoundReference(index, _, _) =>
      if (tierIndex <= 0) {
        // We are at the input tier so the bound attribute is good!!!
        Some(index)
      } else {
        // Not at the input yet
        val newTier = tierIndex - 1
        val newExpr = exprTiers(newTier)(index)
        getPassThroughIndex(newTier, newExpr, index)
      }
    case _ =>
      None
  }

  /**
   * Given an output index check to see if this is just going to be a pass through to a
   * specific input column index.
   * @param index the output column index to check
   * @return the index of the input column that it passes through to or else None
   */
  def getPassThroughIndex(index: Int): Option[Int] = {
    val startTier = exprTiers.length - 1
    getPassThroughIndex(startTier, exprTiers.last(index), index)
  }

  private [this] def projectWithRetrySingleBatchInternal(sb: SpillableColumnarBatch,
      closeInputBatch: Boolean): ColumnarBatch = {
    if (areAllRetryable) {
      // If all of the expressions are retryable we can just run everything and retry it
      // at the top level. If some things are not retryable we need to split them up and
      // do the processing in a way that makes it so retries are more likely to succeed.
      val sbToClose = if (closeInputBatch) {
        Some(sb)
      } else {
        None
      }
      withResource(sbToClose) { _ =>
        retryables.foreach(_.checkpoint())
        RmmRapidsRetryIterator.withRetryNoSplit {
          withResource(sb.getColumnarBatch()) { cb =>
            withRestoreOnRetry(retryables) {
              project(cb)
            }
          }
        }
      }
    } else {
      @tailrec
      def recurse(boundExprs: Seq[Seq[GpuExpression]],
          sb: SpillableColumnarBatch,
          recurseCloseInputBatch: Boolean): SpillableColumnarBatch = boundExprs match {
        case Nil => sb
        case exprSet :: tail =>
          val projectSb = withResource(new NvtxRange("project tier", NvtxColor.ORANGE)) { _ =>
            val projectResult = if (recurseCloseInputBatch) {
              GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, exprSet)
            } else {
              GpuProjectExec.projectWithRetrySingleBatch(sb, exprSet)
            }
            SpillableColumnarBatch(projectResult, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          }
          // We always want to close the temp batches that we make...
          recurse(tail, projectSb, recurseCloseInputBatch = true)
      }
      // Process tiers sequentially. The input batch is closed by recurse if requested.
      withResource(recurse(exprTiers, sb, closeInputBatch)) { ret =>
        ret.getColumnarBatch()
      }
    }
  }

  /**
   * Do a project with retry and close the input batch when done.
   */
  def projectAndCloseWithRetrySingleBatch(sb: SpillableColumnarBatch): ColumnarBatch =
    projectWithRetrySingleBatchInternal(sb, closeInputBatch = true)

  /**
   * Do a project with retry, but don't close the input batch.
   */
  def projectWithRetrySingleBatch(sb: SpillableColumnarBatch): ColumnarBatch =
    projectWithRetrySingleBatchInternal(sb, closeInputBatch = false)

  def project(batch: ColumnarBatch): ColumnarBatch = {
    @tailrec
    def recurse(boundExprs: Seq[Seq[GpuExpression]],
        cb: ColumnarBatch,
        isFirst: Boolean): ColumnarBatch = {
      boundExprs match {
        case Nil => cb
        case exprSet :: tail =>
          val projectCb = try {
            withResource(new NvtxRange("project tier", NvtxColor.ORANGE)) { _ =>
              GpuProjectExec.project(cb, exprSet)
            }
          } finally {
            // Close intermediate batches
            if (!isFirst) {
              cb.close()
            }
          }
          recurse(tail, projectCb, false)
      }
    }
    // Process tiers sequentially
    recurse(exprTiers, batch, true)
  }
}

/**
 * Run a filter on a batch.  The batch will be consumed.
 */
object GpuFilter {

  def apply(
      batch: ColumnarBatch,
      boundCondition: Expression,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("filter batch", NvtxColor.YELLOW, filterTime)) { _ =>
      val filteredBatch = GpuFilter(batch, boundCondition)
      numOutputBatches += 1
      numOutputRows += filteredBatch.numRows()
      filteredBatch
    }
  }

  def filterAndClose(batch: ColumnarBatch,
      boundCondition: GpuTieredProject,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric): Iterator[ColumnarBatch] = {
    if (boundCondition.areAllRetryable) {
      val sb = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      filterAndCloseWithRetry(sb, boundCondition, numOutputRows, numOutputBatches, filterTime)
    } else {
      filterAndCloseNoRetry(batch, boundCondition, numOutputRows, numOutputBatches,
        filterTime)
    }
  }

  private def filterAndCloseNoRetry(batch: ColumnarBatch,
      boundCondition: GpuTieredProject,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric): Iterator[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("filter batch", NvtxColor.YELLOW, filterTime)) { _ =>
      val filteredBatch = withResource(batch) { batch =>
        GpuFilter(batch, boundCondition)
      }
      numOutputBatches += 1
      numOutputRows += filteredBatch.numRows()
      Seq(filteredBatch).toIterator
    }
  }

  private def filterAndCloseWithRetry(input: SpillableColumnarBatch,
      boundCondition: GpuTieredProject,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    boundCondition.retryables.foreach(_.checkpoint())
    val ret = withRetry(input, splitSpillableInHalfByRows) { sb =>
      withResource(sb.getColumnarBatch()) { cb =>
        withRestoreOnRetry(boundCondition.retryables) {
          withResource(new NvtxWithMetrics("filter batch", NvtxColor.YELLOW, opTime)) { _ =>
            GpuFilter(cb, boundCondition)
          }
        }
      }
    }
    ret.map { cb =>
      numOutputRows += cb.numRows()
      numOutputBatches += 1
      cb
    }
  }

  private def allEntriesAreTrue(mask: GpuColumnVector): Boolean = {
    if (mask.hasNull) {
      false
    } else {
      withResource(mask.getBase.all()) { all =>
        all.getBoolean
      }
    }
  }

  private def doFilter(checkedFilterMask: Option[cudf.ColumnVector],
      cb: ColumnarBatch): ColumnarBatch = {
    checkedFilterMask.map { checkedFilterMask =>
      withResource(checkedFilterMask) { checkedFilterMask =>
        if (cb.numCols() <= 0) {
          val rowCount = withResource(checkedFilterMask.sum(DType.INT32)) { sum =>
            if (sum.isValid) {
              sum.getInt
            } else {
              0
            }
          }
          new ColumnarBatch(Array(), rowCount)
        } else {
          val colTypes = GpuColumnVector.extractTypes(cb)
          withResource(GpuColumnVector.from(cb)) { tbl =>
            withResource(tbl.filter(checkedFilterMask)) { filteredData =>
              GpuColumnVector.from(filteredData, colTypes)
            }
          }
        }
      }
    }.getOrElse {
      // Nothing to filter so it is a NOOP
      GpuColumnVector.incRefCounts(cb)
    }
  }

  private def computeCheckedFilterMask(boundCondition: Expression,
      cb: ColumnarBatch): Option[cudf.ColumnVector] = {
    withResource(boundCondition.columnarEval(cb)) { filterMask =>
      // If  filter is a noop then return a None for the mask
      if (allEntriesAreTrue(filterMask)) {
        None
      } else {
        Some(filterMask.getBase.incRefCount())
      }
    }
  }

  private def computeCheckedFilterMask(boundCondition: GpuTieredProject,
      cb: ColumnarBatch): Option[cudf.ColumnVector] = {
    withResource(boundCondition.project(cb)) { filterBatch =>
      val filterMask = filterBatch.column(0).asInstanceOf[GpuColumnVector]
      // If  filter is a noop then return a None for the mask
      if (allEntriesAreTrue(filterMask)) {
        None
      } else {
        Some(filterMask.getBase.incRefCount())
      }
    }
  }

  private[rapids] def apply(batch: ColumnarBatch,
      boundCondition: Expression) : ColumnarBatch = {
    val checkedFilterMask = computeCheckedFilterMask(boundCondition, batch)
    doFilter(checkedFilterMask, batch)
  }


  def apply(
      batch: ColumnarBatch,
      boundCondition: GpuTieredProject): ColumnarBatch = {
    val checkedFilterMask = computeCheckedFilterMask(boundCondition, batch)
    doFilter(checkedFilterMask, batch)
  }
}

case class GpuFilterExecMeta(
  filter: FilterExec,
  override val conf: RapidsConf,
  parentMetaOpt: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule
) extends SparkPlanMeta[FilterExec](filter, conf, parentMetaOpt, rule) {
  override def convertToGpu(): GpuExec = {
    GpuFilterExec(childExprs.head.convertToGpu(),
      childPlans.head.convertIfNeeded())()
  }
}

case class GpuFilterExec(
    condition: Expression,
    child: SparkPlan)(
    override val coalesceAfter: Boolean = true)
    extends ShimUnaryExecNode with ShimPredicateHelper with GpuExec {

  override def otherCopyArgs: Seq[AnyRef] =
    Seq[AnyRef](coalesceAfter.asInstanceOf[java.lang.Boolean])

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case GpuIsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val rdd = child.executeColumnar()
    val boundCondition = GpuBindReferences.bindGpuReferencesTiered(Seq(condition), child.output,
      conf)
    rdd.flatMap { batch =>
      GpuFilter.filterAndClose(batch, boundCondition, numOutputRows,
        numOutputBatches, opTime)
    }
  }
}

class GpuSampleExecMeta(
    sample: SampleExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[SampleExec](sample, conf, p, r)
    with Logging {
  override def convertToGpu(): GpuExec = {
    val gpuChild = childPlans.head.convertIfNeeded()
    if (conf.isFastSampleEnabled) {
      // Use GPU sample JNI, this is faster, but the output is not the same as CPU produces
      GpuFastSampleExec(sample.lowerBound, sample.upperBound, sample.withReplacement,
        sample.seed, gpuChild)
    } else {
      // The output is the same as CPU produces
      // First generates row indexes by CPU sampler, then use GPU to gathers
      GpuSampleExec(sample.lowerBound, sample.upperBound, sample.withReplacement,
        sample.seed, gpuChild)
    }
  }
}

case class GpuSampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long, child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def output: Seq[Attribute] = {
    child.output
  }

  // add one coalesce exec to avoid empty batch and small batch,
  // because sample will shrink the batch
  override val coalesceAfter: Boolean = true

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)

    val rdd = child.executeColumnar()
    // CPU consistent, first generates sample row indexes by CPU, then gathers by GPU
    if (withReplacement) {
      new GpuPartitionwiseSampledRDD(
        rdd,
        new GpuPoissonSampler(upperBound - lowerBound, useGapSamplingIfPossible = false,
          numOutputRows, numOutputBatches, opTime),
        preservesPartitioning = true,
        seed)
    } else {
      rdd.mapPartitionsWithIndex(
        (index, iterator) => {
          // use CPU sampler generate row indexes
          val sampler = new BernoulliCellSampler(lowerBound, upperBound)
          sampler.setSeed(seed + index)
          iterator.map[ColumnarBatch] { columnarBatch =>
            // collect sampled row idx
            // samples idx in batch one by one, so it's same as CPU execution
            withResource(new NvtxWithMetrics("Sample Exec", NvtxColor.YELLOW, opTime)) { _ =>
              withResource(columnarBatch) { cb =>
                // generate sampled row indexes by CPU
                val sampledRows = new ArrayBuffer[Int]
                var rowIndex = 0
                while (rowIndex < cb.numRows()) {
                  if (sampler.sample() > 0) {
                    sampledRows += rowIndex
                  }
                  rowIndex += 1
                }
                numOutputBatches += 1
                numOutputRows += sampledRows.length
                // gather by row indexes
                GatherUtils.gather(cb, sampledRows)
              }
            }
          }
        }
        , preservesPartitioning = true
      )
    }
  }
}

case class GpuFastSampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def output: Seq[Attribute] = {
    child.output
  }

  // add one coalesce exec to avoid empty batch and small batch,
  // because sample will shrink the batch
  override val coalesceAfter: Boolean = true

  // Note GPU sample does not preserve the ordering
  override def outputOrdering: Seq[SortOrder] = Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val rdd = child.executeColumnar()

    // CPU inconsistent, uses GPU sample JNI
    rdd.mapPartitionsWithIndex(
      (index, iterator) => {
        iterator.map[ColumnarBatch] { columnarBatch =>
          withResource(new NvtxWithMetrics("Fast Sample Exec", NvtxColor.YELLOW, opTime)) { _ =>
            withResource(columnarBatch) { cb =>
              numOutputBatches += 1
              val numSampleRows = (cb.numRows() * (upperBound - lowerBound)).toLong

              val colTypes = GpuColumnVector.extractTypes(cb)
              if (numSampleRows == 0L) {
                GpuColumnVector.emptyBatchFromTypes(colTypes)
              } else if (cb.numCols() == 0) {
                // for count agg, num of cols is 0
                val c = GpuColumnVector.emptyBatchFromTypes(colTypes)
                c.setNumRows(numSampleRows.toInt)
                c
              } else {
                withResource(GpuColumnVector.from(cb)) { table =>
                  // GPU sample
                  withResource(table.sample(numSampleRows, withReplacement, seed + index)) {
                    sampled =>
                      val cb = GpuColumnVector.from(sampled, colTypes)
                      numOutputRows += cb.numRows()
                      cb
                  }
                }
              }
            }
          }
        }
      }
      , preservesPartitioning = true
    )
  }
}

private[rapids] class GpuRangeIterator(
    partitionStart: BigInt,
    partitionEnd: BigInt,
    step: Long,
    maxRowCountPerBatch: Long,
    taskContext: TaskContext,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with Logging {

  // This iterator is designed for GpuRangeExec, so it has the requirement for the inputs.
  assert((partitionEnd - partitionStart) % step == 0)

  private def getSafeMargin(bi: BigInt): Long = {
    if (bi.isValidLong) {
      bi.toLong
    } else if (bi > 0) {
      Long.MaxValue
    } else {
      Long.MinValue
    }
  }

  private val safePartitionStart = getSafeMargin(partitionStart) // inclusive
  private val safePartitionEnd = getSafeMargin(partitionEnd) // exclusive, unless start == this
  private[this] var currentPosition: Long = safePartitionStart
  private[this] var done: Boolean = false

  override def hasNext: Boolean = {
    if (!done) {
      if (step > 0) {
        currentPosition < safePartitionEnd
      } else {
        currentPosition > safePartitionEnd
      }
    } else false
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    GpuSemaphore.acquireIfNecessary(taskContext)
    withResource(new NvtxWithMetrics("GpuRange", NvtxColor.DARK_GREEN, opTime)) { _ =>
      val start = currentPosition
      val remainingRows = (safePartitionEnd - start) / step
      // Start is inclusive so we need to produce at least one row
      val rowsExpected = Math.max(1, Math.min(remainingRows, maxRowCountPerBatch))
      val iter = withRetry(AutoCloseableLong(rowsExpected), reduceRowsNumberByHalf) { rows =>
        withResource(Scalar.fromLong(start)) { startScalar =>
          withResource(Scalar.fromLong(step)) { stepScalar =>
            withResource(
                cudf.ColumnVector.sequence(startScalar, stepScalar, rows.value.toInt)) { vec =>
              withResource(new Table(vec)) { tab =>
                GpuColumnVector.from(tab, Array[DataType](LongType))
              }
            }
          }
        }
      }
      assert(iter.hasNext)
      closeOnExcept(iter.next()) { batch =>
        // This "iter" returned from the "withRetry" block above has only one batch,
        // because the split function "reduceRowsNumberByHalf" returns a Seq with a single
        // element inside.
        // By doing this, we can pull out this single batch directly without maintaining
        // this extra `iter` for the next loop.
        assert(iter.isEmpty)
        val endInclusive = start + ((batch.numRows() - 1) * step)
        currentPosition = endInclusive + step
        if (currentPosition < endInclusive ^ step < 0) {
          // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
          // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a
          // step back, we are pretty sure that we have an overflow.
          done = true
        }
        if (batch.numRows() < rowsExpected) {
          logDebug(s"Retried with ${batch.numRows()} rows when expected $rowsExpected rows")
        }
        batch
      }
    }
  }

  /**
   * Reduce the input rows number by half, and it returns a Seq with only one element,
   * that is the half value.
   * This will be used with the split_retry block to generate a single batch a with smaller
   * size when getting relevant OOMs.
   */
  private def reduceRowsNumberByHalf: AutoCloseableLong => Seq[AutoCloseableLong] =
    (rowsNumber) => {
      withResource(rowsNumber) { _ =>
        if (rowsNumber.value < 10) {
          throw new GpuSplitAndRetryOOM(s"GPU OutOfMemory: the number of rows generated is" +
            s" too small to be split ${rowsNumber.value}!")
        }
        Seq(AutoCloseableLong(rowsNumber.value / 2))
      }
    }

  /** A bridge class between Long and AutoCloseable for retry */
  case class AutoCloseableLong(value: Long) extends AutoCloseable {
    override def close(): Unit = { /* Nothing to be closed */ }
  }
}

/**
 * Physical plan for range (generating a range of 64 bit numbers).
 */
case class GpuRangeExec(
    start: Long,
    end: Long,
    step: Long,
    numSlices: Int,
    output: Seq[Attribute],
    targetSizeBytes: Long)
  extends ShimLeafExecNode with GpuExec {

  val numElements: BigInt = {
    val safeStart = BigInt(start)
    val safeEnd = BigInt(end)
    if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
      (safeEnd - safeStart) / step
    } else {
      // the remainder has the same sign with range, could add 1 more
      (safeEnd - safeStart) / step + 1
    }
  }

  val isEmptyRange: Boolean = start == end || (start < end ^ 0 < step)

  override protected val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override protected val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME)
  )

  override def outputOrdering: Seq[SortOrder] = {
    val order = if (step > 0) {
      Ascending
    } else {
      Descending
    }
    output.map(a => SortOrder(a, order))
  }

  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override def outputBatching: CoalesceGoal = TargetSize(targetSizeBytes)

  protected override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val maxRowCountPerBatch = Math.min(targetSizeBytes/8, Int.MaxValue)

    if (isEmptyRange) {
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      sparkSession
        .sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex { (i, _) =>
          val partitionStart = (i * numElements) / numSlices * step + start
          val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
          val taskContext = TaskContext.get()
          val inputMetrics = taskContext.taskMetrics().inputMetrics

          val rangeIter = new GpuRangeIterator(partitionStart, partitionEnd, step,
              maxRowCountPerBatch, taskContext, opTime).map { batch =>
            numOutputRows += batch.numRows()
            TrampolineUtil.incInputRecordsRows(inputMetrics, batch.numRows())
            numOutputBatches += 1
            batch
          }
          new InterruptibleIterator(taskContext, rangeIter)
        }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"GpuRange ($start, $end, step=$step, splits=$numSlices)"
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
}


case class GpuUnionExec(children: Seq[SparkPlan]) extends ShimSparkPlan with GpuExec {

  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(TrampolineUtil.unionLikeMerge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  // The smallest of our children
  override def outputBatching: CoalesceGoal =
    children.map(GpuExec.outputBatching).reduce(CoalesceGoal.minProvided)

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)

    sparkContext.union(children.map(_.executeColumnar())).map { batch =>
      numOutputBatches += 1
      numOutputRows += batch.numRows
      batch
    }
  }
}

case class GpuCoalesceExec(numPartitions: Int, child: SparkPlan)
    extends ShimUnaryExecNode with GpuExec {

  // This operator does not record any metrics
  override lazy val allMetrics: Map[String, GpuMetric] = Map.empty

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  protected override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar()
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions, shuffle = false)
    }
  }

  override val coalesceAfter: Boolean = true
}

object GpuCoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[ColumnarBatch](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}
