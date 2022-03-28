/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{ShimSparkPlan, ShimUnaryExecNode}

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Descending, Expression, NamedExpression, NullIntolerant, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{LeafExecNode, ProjectExec, SampleExec, SparkPlan}
import org.apache.spark.sql.rapids.{GpuPartitionwiseSampledRDD, GpuPoissonSampler, GpuPredicateHelper}
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

object GpuProjectExec extends Arm {
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

  private def extractSingleBoundIndex(boundExprs: Seq[Expression]): Seq[Option[Int]] =
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

  def projectSingle(cb: ColumnarBatch, boundExpr: Expression): GpuColumnVector =
    GpuExpressionsUtils.columnarEvalToColumn(boundExpr, cb)

  def project(cb: ColumnarBatch, boundExprs: Seq[Expression]): ColumnarBatch = {
    if (isNoopProject(cb, boundExprs)) {
      // This can help avoid contiguous splits in some cases when the input data is also contiguous
      GpuColumnVector.incRefCounts(cb)
    } else {
      val newColumns = boundExprs.safeMap(expr => projectSingle(cb, expr)).toArray[ColumnVector]
      new ColumnarBatch(newColumns, cb.numRows())
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
   child: SparkPlan
 ) extends ShimUnaryExecNode with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val boundProjectList = GpuBindReferences.bindGpuReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    rdd.map { cb =>
      numOutputBatches += 1
      numOutputRows += cb.numRows()
      GpuProjectExec.projectAndClose(cb, boundProjectList, opTime)
    }
  }

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)
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
) extends ShimUnaryExecNode with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def output: Seq[Attribute] = {
    projectList.collect { case ne: NamedExpression => ne.toAttribute }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val boundProjectList = GpuBindReferences.bindGpuReferences(projectList, child.output)
    val outputTypes = output.map(_.dataType).toArray
    val rdd = child.executeColumnar()
    rdd.mapPartitions { cbIter =>
      new Iterator[ColumnarBatch] with AutoCloseable {
        private[this] var compiledAstExprs =
          withResource(new NvtxWithMetrics("Compile ASTs", NvtxColor.ORANGE, opTime)) { _ =>
            boundProjectList.safeMap { expr =>
              // Use intmax for the left table column count since there's only one input table here.
              expr.convertToAst(Int.MaxValue).compile()
            }
          }

        Option(TaskContext.get).foreach(_.addTaskCompletionListener[Unit](_ => close()))

        override def hasNext: Boolean = {
          if (cbIter.hasNext) {
            true
          } else {
            close()
            false
          }
        }

        override def next(): ColumnarBatch = {
          withResource(cbIter.next()) { cb =>
            withResource(new NvtxWithMetrics("Project AST", NvtxColor.CYAN, opTime)) { _ =>
              numOutputBatches += 1
              numOutputRows += cb.numRows()
              val projectedTable = withResource(tableFromBatch(cb)) { table =>
                withResource(compiledAstExprs.safeMap(_.computeColumn(table))) { projectedColumns =>
                  new Table(projectedColumns:_*)
                }
              }
              withResource(projectedTable) { _ =>
                GpuColumnVector.from(projectedTable, outputTypes)
              }
            }
          }
        }

        override def close(): Unit = {
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

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)
}

/**
 * Run a filter on a batch.  The batch will be consumed.
 */
object GpuFilter extends Arm {
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

  private def allEntriesAreTrue(mask: GpuColumnVector): Boolean = {
    if (mask.hasNull) {
      false
    } else {
      withResource(mask.getBase.all()) { all =>
        all.getBoolean
      }
    }
  }

  def apply(batch: ColumnarBatch,
      boundCondition: Expression) : ColumnarBatch = {
    withResource(batch) { batch =>
      val checkedFilterMask = withResource(
        GpuProjectExec.projectSingle(batch, boundCondition)) { filterMask =>
        // If  filter is a noop then return a None for the mask
        if (allEntriesAreTrue(filterMask)) {
          None
        } else {
          Some(filterMask.getBase.incRefCount())
        }
      }
      checkedFilterMask.map { checkedFilterMask =>
        withResource(checkedFilterMask) { checkedFilterMask =>
          val colTypes = GpuColumnVector.extractTypes(batch)
          withResource(GpuColumnVector.from(batch)) { tbl =>
            withResource(tbl.filter(checkedFilterMask)) { filteredData =>
              GpuColumnVector.from(filteredData, colTypes)
            }
          }
        }
      }.getOrElse {
        // Nothing to filter so it is a NOOP
        GpuColumnVector.incRefCounts(batch)
      }
    }
  }
}

case class GpuFilterExec(
    condition: Expression,
    child: SparkPlan,
    override val coalesceAfter: Boolean = true)
    extends ShimUnaryExecNode with GpuPredicateHelper with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case GpuIsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val boundCondition = GpuBindReferences.bindReference(condition, child.output)
    val rdd = child.executeColumnar()
    rdd.map { batch =>
      GpuFilter(batch, boundCondition, numOutputRows, numOutputBatches, opTime)
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
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
    extends LeafExecNode with GpuExec {

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
  ) ++ semaphoreMetrics

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

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val semTime = gpuLongMetric(SEMAPHORE_WAIT_TIME)
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

            def getSafeMargin(bi: BigInt): Long =
              if (bi.isValidLong) {
                bi.toLong
              } else if (bi > 0) {
                Long.MaxValue
              } else {
                Long.MinValue
              }

            val safePartitionStart = getSafeMargin(partitionStart) // inclusive
            val safePartitionEnd = getSafeMargin(partitionEnd) // exclusive, unless start == this
            val taskContext = TaskContext.get()

            val iter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
              private[this] var number: Long = safePartitionStart
              private[this] var done: Boolean = false
              private[this] val inputMetrics = taskContext.taskMetrics().inputMetrics

              override def hasNext: Boolean =
                if (!done) {
                  if (step > 0) {
                    number < safePartitionEnd
                  } else {
                    number > safePartitionEnd
                  }
                } else false

              override def next(): ColumnarBatch = {
                GpuSemaphore.acquireIfNecessary(taskContext, semTime)
                withResource(
                  new NvtxWithMetrics("GpuRange", NvtxColor.DARK_GREEN, opTime)) { _ =>
                    val start = number
                    val remainingSteps = (safePartitionEnd - start) / step
                    // Start is inclusive so we need to produce at least one row
                    val rowsThisBatch = Math.max(1, Math.min(remainingSteps, maxRowCountPerBatch))
                    val endInclusive = start + ((rowsThisBatch - 1) * step)
                    number = endInclusive + step
                    if (number < endInclusive ^ step < 0) {
                      // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
                      // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a
                      // step back, we are pretty sure that we have an overflow.
                      done = true
                    }

                    val ret = withResource(Scalar.fromLong(start)) { startScalar =>
                      withResource(Scalar.fromLong(step)) { stepScalar =>
                        withResource(
                          cudf.ColumnVector.sequence(
                            startScalar, stepScalar, rowsThisBatch.toInt)) { vec =>
                          withResource(new Table(vec)) { tab =>
                            GpuColumnVector.from(tab, Array[DataType](LongType))
                          }
                        }
                      }
                    }

                    assert(rowsThisBatch == ret.numRows())
                    numOutputRows += rowsThisBatch
                    TrampolineUtil.incInputRecordsRows(inputMetrics, rowsThisBatch)
                    numOutputBatches += 1
                    ret
                }
              }
            }
            new InterruptibleIterator(taskContext, iter)
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
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

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar()
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions, shuffle = false)
    }
  }
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
