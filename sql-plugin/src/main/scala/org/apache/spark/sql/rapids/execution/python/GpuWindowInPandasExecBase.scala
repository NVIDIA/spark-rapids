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

package org.apache.spark.sql.rapids.execution.python

import ai.rapids.cudf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuWindowInPandasExecMetaBase(
    winPandas: WindowInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowInPandasExec](winPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    winPandas.windowExpression.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    winPandas.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    winPandas.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  // Same check with that in GpuWindowExecMeta
  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing" +
        " Pandas UDF; cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))

    // Early check for the frame type, only supporting RowFrame for now, which is different from
    // the node GpuWindowExec.
    windowExpressions
      .flatMap(meta => meta.wrapped.collect { case e: SpecifiedWindowFrame => e })
      .filter(swf => swf.frameType.equals(RangeFrame))
      .foreach(rf => willNotWorkOnGpu(because = s"Only support RowFrame for now," +
        s" but found ${rf.frameType}"))
  }
}

/**
 * This iterator will build the additional info columns on the incoming batches for windowing
 * things before sending batches to the Python side, now including only window bounds.
 * @param wrapped the incoming ColumnarBatch iterator.
 * @param partitionRefs the column references used by the window partition spec.
 * @param isSplitGroups whether to split the groups into batches respectively
 * @param windowFrames the window frames for the windowing
 * @param inputRows metric for rows read
 * @param inputBatches metric for batches read
 */
class WindowingIterator(
    wrapped: Iterator[ColumnarBatch],
    partitionRefs: Seq[Expression],
    isSplitGroups: Boolean,
    windowFrames: Seq[GpuSpecifiedWindowFrame],
    inputRows: GpuMetric,
    inputBatches: GpuMetric,
    spillCallback: RapidsBuffer.SpillCallback)
  extends Iterator[(ColumnarBatch, ColumnarBatch)] with Arm {

  private val queueBatches: mutable.Queue[(SpillableColumnarBatch, Seq[Int])] =
    mutable.Queue.empty

  TaskContext.get().addTaskCompletionListener[Unit]{ _ =>
    queueBatches.foreach(_._1.close())
    queueBatches.clear()
  }

  override def hasNext(): Boolean = queueBatches.nonEmpty || wrapped.hasNext

  // Return both the original batch and the additional info batch.
  // The info batch will be used by the Python side.
  override def next(): (ColumnarBatch, ColumnarBatch) = {
    val (nextBatch, gpLens) = if (queueBatches.nonEmpty) {
      val (sBatch, lens) = queueBatches.dequeue()
      (sBatch.getColumnarBatch(), lens)
    } else {
      val batch = wrapped.next()
      inputBatches += 1
      inputRows += batch.numRows()
      val groupLens = genGroupInfoFromBatch(batch)
      if (isSplitGroups && groupLens.size > 1) {
        // Split the groups when being required and there are more than one group.
        val groupBatchesAndSizes = splitBatchPerGroups(batch, groupLens)
          .zip(groupLens.map(Seq(_)))
        val tails = groupBatchesAndSizes.tail.map {
          case (sb, gpInfo) =>
            (SpillableColumnarBatch(sb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback),
              gpInfo)
        }
        // Return the first one and enqueue others
        queueBatches.enqueue(tails: _*)
        groupBatchesAndSizes.head
      } else {
        (batch, groupLens)
      }
    }
    (nextBatch, buildInfoBatchForPython(nextBatch, gpLens))
  }

  private[this] def splitBatchPerGroups(
      batch: ColumnarBatch,
      gpLens: Seq[Int]): Seq[ColumnarBatch] = {
    val splitIndices = gpLens.dropRight(1).scan(0)(_+_).tail
    // Close the original big batch
    withResource(batch) { cb =>
      withResource(GpuColumnVector.from(cb)) { table =>
        withResource(table.contiguousSplit(splitIndices: _*)) { tables =>
          val dataTypes = GpuColumnVector.extractTypes(batch)
          tables.safeMap(t => GpuColumnVectorFromBuffer.from(t, dataTypes))
        }
      }
    }
  }

  private[this] def buildInfoBatchForPython(
      batch: ColumnarBatch,
      gpLens: Seq[_]): ColumnarBatch = {

    // The rolling size may need tuning.
    val rollingSize = 20000
    val gpInfo: Seq[_] = if (gpLens.length <= rollingSize) {
      gpLens
    } else {
      // Two levels concatenation to avoid too many small column vectors on GPU and
      // exceeding GC overhead limit.
      gpLens.sliding(rollingSize, rollingSize).toSeq
    }

    // Build the window bounds for bounded frames
    val boundsCVs = windowFrames.filterNot(_.isUnbounded).flatMap { frame =>
      frame.frameType match {
        case RowFrame =>
          val (lowerCV, upperCV, _) = genColumnsForWindowBounds(frame, gpInfo, 0)
          Seq(lowerCV, upperCV)
        case f => throw new UnsupportedOperationException(s"Unsupported window frame $f")
      }
    }.map(GpuColumnVector.from(_, IntegerType))
    new ColumnarBatch(boundsCVs.toArray, batch.numRows())
  }

  /*
   * (Currently do it be leveraging the existing APIs in cuDF Java.
   *  But still need a dedicated API for better performance.)
   */
  private[this] def genGroupInfoFromBatch(batch: ColumnarBatch): Seq[Int] = {
    assert(batch != null, "batch is null.")
    if (batch.numRows() > 0 && batch.numCols() > 0 && partitionRefs.nonEmpty) {
      //  a) Run count aggregation on each group in the batch, including null values.
      //  b) Restore the original order (Ascending with NullsFirst) defined in window plan,
      //     since "cudf.Table.groupBy" will break the order.
      //  c) Copy the 'count' column to host and it is the group info.
      val partitionIndices = partitionRefs.indices
      val cntTable = withResource(GpuProjectExec.project(batch, partitionRefs)) { projected =>
        withResource(GpuColumnVector.from(projected)) { table =>
          table
            .groupBy(partitionIndices:_*)
            .aggregate(cudf.Aggregation.count(cudf.Aggregation.NullPolicy.INCLUDE).onColumn(0))
        }
      }
      val orderedTable = withResource(cntTable) { table =>
        table.orderBy(partitionIndices.map(id => cudf.Table.asc(id, true)): _*)
      }
      val (countHostCol, numRows) = withResource(orderedTable) { table =>
        // Yes copying the data to host, it would be OK since just copying the aggregated
        // count column.
        (table.getColumn(table.getNumberOfColumns - 1).copyToHost(), table.getRowCount)
      }
      withResource(countHostCol) { cntCol =>
        // Verified the type of Count column is integer, so use getInt
        (0L until numRows).map(id => cntCol.getInt(id))
      }
    } else {
      // Empty batch or No partition defined for Window operation, the whole batch is a group.
      Seq(batch.numRows())
    }
  }

  /*
   * Build a single column vector from the column vectors collected so far.
   * If seq is empty this will likely blow up.
   */
  private[this] def buildNonEmptyCV(cvs: Seq[cudf.ColumnVector]): cudf.ColumnVector = {
    if (cvs.size == 1) {
      cvs.head
    } else if (cvs.size > 1) {
      withResource(cvs) { seqCV =>
        cudf.ColumnVector.concatenate(seqCV: _*)
      }
    } else {
      throw new IllegalArgumentException("Empty sequence is not allowed.")
    }
  }

  // Build column vectors for lower bound and upper bound.
  private[this] def genColumnsForWindowBounds(
      frame: GpuSpecifiedWindowFrame,
      gpLens: Seq[_],
      groupOffset: Int): (cudf.ColumnVector, cudf.ColumnVector, Int) = {
    assert(gpLens != null)
    var gpOffset = groupOffset
    val bufferLowerCV = mutable.ArrayBuffer[cudf.ColumnVector]()
    val bufferUpperCV = mutable.ArrayBuffer[cudf.ColumnVector]()
    gpLens.foreach {
      case gpInfo: Seq[_] =>
        val (lowerCV, upperCV, offset) = genColumnsForWindowBounds(frame, gpInfo, gpOffset)
        bufferLowerCV.append(lowerCV)
        bufferUpperCV.append(upperCV)
        // Update offset to next group slide
        gpOffset = offset
      case gpLen: Int =>
        bufferLowerCV.append(genLowerCV(frame.lower, gpOffset, gpLen))
        bufferUpperCV.append(genUpperCV(frame.upper, gpOffset, gpLen))
        // Update offset to next group
        gpOffset += gpLen
    }
    (buildNonEmptyCV(bufferLowerCV), buildNonEmptyCV(bufferUpperCV), gpOffset)
  }

  /*
   * Build the lower bounds as a column vector per group per frame.
   *
   * (Currently do it be leveraging the existing APIs in cuDF Java.
   *  But still need a dedicated API for better performance.)
   */
  private[this] def genLowerCV(
      lower: Expression,
      groupOffset: Int,
      groupLen: Int): cudf.ColumnVector = {
    lower match {
      case GpuSpecialFrameBoundary(UnboundedPreceding) =>
        // bound is always the group offset in a batch
        withResource(cudf.Scalar.fromInt(groupOffset)) { offsetVal =>
          cudf.ColumnVector.fromScalar(offsetVal, groupLen)
        }
      case GpuSpecialFrameBoundary(CurrentRow) =>
        // Simply create a integer sequence starting with group offset
        withResource(cudf.Scalar.fromInt(groupOffset)) { offsetVal =>
          cudf.ColumnVector.sequence(offsetVal, groupLen)
        }
      case GpuLiteral(offset, _) =>
        // 1) Create a integer sequence starting with (groupOffset + offset)
        // 2) Replace the values less than groupOffset with groupOffset
        //    (offset is negative for lower bound)
        val startIndex = groupOffset + offset.asInstanceOf[Int]
        withResource(cudf.Scalar.fromInt(startIndex)) { startVal =>
          withResource(cudf.ColumnVector.sequence(startVal, groupLen)) { offsetCV =>
            val loAndNullHi = Seq(cudf.Scalar.fromInt(groupOffset),
              cudf.Scalar.fromNull(cudf.DType.INT32))
            withResource(loAndNullHi) { loNullHi =>
              offsetCV.clamp(loNullHi.head, loNullHi.last)
            }
          }
        }
      case what =>
        throw new UnsupportedOperationException(s"Unsupported lower bound: ${what.prettyName}")
    }
  }

  /*
   * Build the upper bounds as a column vector per group per frame.
   *
   * (Currently do it be leveraging the existing APIs in cuDF Java.
   *  But still need a dedicated API for better performance.)
   */
  private[this] def genUpperCV(
      upper: Expression,
      groupOffset: Int,
      groupLen: Int): cudf.ColumnVector = {
    val groupEnd = groupOffset + groupLen
    upper match {
      case GpuSpecialFrameBoundary(UnboundedFollowing) =>
        // bound is always the group offset in a batch plus group length
        withResource(cudf.Scalar.fromInt(groupEnd)) { groupEndVal =>
          cudf.ColumnVector.fromScalar(groupEndVal, groupLen)
        }
      case GpuSpecialFrameBoundary(CurrentRow) =>
        // Simply create a integer sequence starting with (groupOffset + 1)
        withResource(cudf.Scalar.fromInt(groupOffset + 1)) { offsetOneVal =>
          cudf.ColumnVector.sequence(offsetOneVal, groupLen)
        }
      case GpuLiteral(offset, _) =>
        // 1) Create a integer sequence starting with (groupOffset + 1 + offset)
        // 2) Replace the values larger than groupEnd with groupEnd
        //    (offset is positive for upper bound)
        val startIndex = groupOffset + 1 + offset.asInstanceOf[Int]
        withResource(cudf.Scalar.fromInt(startIndex)) { startVal =>
          withResource(cudf.ColumnVector.sequence(startVal, groupLen)) { offsetCV =>
            val nullLoAndHi = Seq(cudf.Scalar.fromNull(cudf.DType.INT32),
              cudf.Scalar.fromInt(groupEnd))
            withResource(nullLoAndHi) { nullLoHi =>
              offsetCV.clamp(nullLoHi.head, nullLoHi.last)
            }
          }
        }
      case what =>
        throw new UnsupportedOperationException(s"Unsupported upper bound: ${what.prettyName}")
    }
  }

}

/*
 * The base class of GpuWindowInPandasExec in different shim layers
 *
 */
trait GpuWindowInPandasExecBase extends UnaryExecNode with GpuExec {

  def windowExpression: Seq[Expression]
  def partitionSpec: Seq[Expression]
  def orderSpec: Seq[SortOrder]

  // Used to choose the default python modules
  // please refer to `GpuPythonHelper` for details.
  def pythonModuleKey: String

  // project the output from joined batch, you should close the joined batch if no longer needed.
  def projectResult(joinedBatch: ColumnarBatch): ColumnarBatch

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionSpec) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(ShimLoader.getSparkShims.sortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(RequireSingleBatch)

  private val windowBoundTypeConf = "pandas_window_bound_types"

  private val UnboundedWindow ="unbounded"
  private val BoundedWindow ="bounded"

  /*
   * Helper functions and data structures for window bounds
   *
   * This tuple contains:
   * (1) Function from frame index to its lower bound column index in the python input row
   * (2) Function from frame index to its upper bound column index in the python input row
   * (3) Seq from frame index to its window bound type
   * (4) Seq of all the window frames
   */
  private type WindowMetaHelpers = (
      Int => Int,
      Int => Int,
      Seq[String],
      Seq[GpuSpecifiedWindowFrame])

  private def computeWindowMetaHelpers: WindowMetaHelpers = {
    val windowFrames =  windowFramesWithExpressions.map(_._1)

    val windowBoundTypes = windowFrames.map { frame =>
      if (frame.isUnbounded) {
        UnboundedWindow
      } else {
        BoundedWindow
      }
    }
    val requiredIndices = windowBoundTypes.map {
      case UnboundedWindow => 0
      case _ => 2
    }

    val upperBoundIndices = requiredIndices.scan(0)(_ + _).tail
    val boundIndices = requiredIndices.zip(upperBoundIndices).map { case (num, upperBoundIndex) =>
      if (num == 0) {
        // Sentinel values for unbounded window
        (-1, -1)
      } else {
        (upperBoundIndex - 2, upperBoundIndex - 1)
      }
    }

    def lowerBoundIndex(frameIndex: Int) = boundIndices(frameIndex)._1
    def upperBoundIndex(frameIndex: Int) = boundIndices(frameIndex)._2

    (lowerBoundIndex, upperBoundIndex, windowBoundTypes, windowFrames)
  }

  private def collectFunctions(udf: GpuPythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: GpuPythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[GpuPythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  // Similar with WindowExecBase.windowFrameExpressionFactoryPairs
  // but the functions are not needed here.
  protected lazy val windowFramesWithExpressions = {
    type FrameKey = (String, GpuSpecifiedWindowFrame)
    type ExpressionBuffer = mutable.Buffer[Expression]
    val framedExpressions = mutable.Map.empty[FrameKey, ExpressionBuffer]

    // Add expressions to the map for a given frame.
    def collect(tpe: String, fr: GpuSpecifiedWindowFrame, e: Expression): Unit = {
      val key = (tpe, fr)
      val es = framedExpressions.getOrElseUpdate(key, mutable.ArrayBuffer.empty[Expression])
      es += e
    }

    // Collect all valid window expressions and group them by their frame.
    windowExpression.foreach { e =>
      e.foreach {
        case e @ GpuWindowExpression(function, spec) =>
          val frame = spec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame]
          function match {
            case GpuAggregateExpression(_, _, _, _, _) => collect("AGGREGATE", frame, e)
            // GpuPythonUDF is a GpuAggregateWindowFunction, so it is covered here.
            case _: GpuAggregateWindowFunction => collect("AGGREGATE", frame, e)
            // OffsetWindowFunction is not supported yet, no harm to keep it here
            case _: OffsetWindowFunction => collect("OFFSET", frame, e)
            case f => sys.error(s"Unsupported window function: $f")
          }
        case _ =>
      }
    }

    framedExpressions.toSeq.map {
      // Remove the function type string
      case ((_, frame), es) => (frame, es)
    }
  }

  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES)
  ) ++ spillMetrics

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
    val sessionLocalTimeZone = conf.sessionLocalTimeZone

    // 1) Unwrap the expressions and build some info data:
    //    - Map from expression index to frame index
    //    - Helper functions
    //    - isBounded by given a frame index
    val expressionsWithFrameIndex = windowFramesWithExpressions.map(_._2).zipWithIndex.flatMap {
      case (bufferEs, frameIndex) => bufferEs.map(expr => (expr, frameIndex))
    }
    val exprIndex2FrameIndex = expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap
    val (lowerBoundIndex,
         upperBoundIndex,
         frameBoundTypes,
         windowFrames) = computeWindowMetaHelpers
    val isBounded = { frameIndex: Int => lowerBoundIndex(frameIndex) >= 0 }

    // 2) Extract window functions, here should be Python (Pandas) UDFs
    val allWindowExpressions = expressionsWithFrameIndex.map(_._1)
    val udfExpressions = allWindowExpressions.map {
      case e: GpuWindowExpression => e.windowFunction.asInstanceOf[GpuPythonUDF]
    }
    // We shouldn't be chaining anything here.
    // All chained python functions should only contain one function.
    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip
    require(pyFuncs.length == allWindowExpressions.length)

    // 3) Build the python runner configs
    val udfWindowBoundTypesStr = pyFuncs.indices
      .map(expId => frameBoundTypes(exprIndex2FrameIndex(expId)))
      .mkString(",")
    val pythonRunnerConf: Map[String, String] = ArrowUtils.getPythonRunnerConfMap(conf) +
      (windowBoundTypeConf -> udfWindowBoundTypesStr)

    // 4) Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs. This is similar to how other Python UDF node
    // handles UDF inputs.
    val dataInputs = new ArrayBuffer[Expression]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (dataInputs.exists(_.semanticEquals(e))) {
          dataInputs.indexWhere(_.semanticEquals(e))
        } else {
          dataInputs += e
          dataInputs.length - 1
        }
      }.toArray
    }.toArray

    // In addition to UDF inputs, we will prepend window bounds for each UDFs.
    // For bounded windows, we prepend lower bound and upper bound. For unbounded windows,
    // we no not add window bounds. (strictly speaking, we only need to lower or upper bound
    // if the window is bounded only on one side, this can be improved in the future)

    // 5) Setting window bounds for each window frames. Each window frame has different bounds so
    // each has its own window bound columns.
    val windowBoundsInput = windowFrames.indices.flatMap { frameIndex =>
      if (isBounded(frameIndex)) {
        Seq(
          BoundReference(lowerBoundIndex(frameIndex), IntegerType, nullable = false),
          BoundReference(upperBoundIndex(frameIndex), IntegerType, nullable = false)
        )
      } else {
        Seq.empty
      }
    }

    // 6) Setting the window bounds argOffset for each UDF. For UDFs with bounded window, argOffset
    // for the UDF is (lowerBoundOffset, upperBoundOffset, inputOffset1, inputOffset2, ...)
    // For UDFs with unbounded window, argOffset is (inputOffset1, inputOffset2, ...)
    pyFuncs.indices.foreach { exprIndex =>
      val frameIndex = exprIndex2FrameIndex(exprIndex)
      if (isBounded(frameIndex)) {
        argOffsets(exprIndex) = Array(lowerBoundIndex(frameIndex), upperBoundIndex(frameIndex)) ++
            argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      } else {
        argOffsets(exprIndex) = argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      }
    }

    // 7) Building the final input and schema
    val allInputs = windowBoundsInput ++ dataInputs
    val allInputTypes = allInputs.map(_.dataType)
    val pythonInputSchema = StructType(
      allInputTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }
    )

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf, pythonModuleKey)
    // cache in a local to avoid serializing the plan

    // Build the Python output schema from UDF expressions instead of the 'windowExpression',
    // because the 'windowExpression' does NOT always represent the Python output schema.
    // For example, on Databricks when projecting only one column from a Python UDF output
    // where containing multiple result columns, there will be only one item in the
    // 'windowExpression' for the projecting output, but the output schema for this Python
    // UDF contains multiple columns.
    val pythonOutputSchema = StructType.fromAttributes(udfExpressions.map(_.resultAttribute))
    // Split the input batch per group when
    //   there are unbounded frames, or
    //   batching groups is disabled.
    lazy val isSplitGroups = frameBoundTypes.contains(UnboundedWindow) ||
      (!new RapidsConf(conf).isWindowBatchingGroupsToPythonEnabled)
    val boundPartitionRefs = GpuBindReferences.bindGpuReferences(partitionSpec, child.output)
    val boundPythonInputRefs = GpuBindReferences.bindGpuReferences(dataInputs, child.output)

    // 8) Start processing.
    child.executeColumnar().mapPartitions { inputIter =>
      val context = TaskContext.get()
      val queue: BatchQueue = new BatchQueue()
      context.addTaskCompletionListener[Unit](_ => queue.close())
      // Do required data transformations by windowing iterator
      val pyInputIterator = new WindowingIterator(inputIter, boundPartitionRefs, isSplitGroups,
        windowFrames, numInputRows, numInputBatches, spillCallback).map {
          case (originBatch, infoBatch) =>
            // We have to do the project before adding the batch because the batch might be closed
            // when it is added.
            val projectBatch = GpuProjectExec.project(originBatch, boundPythonInputRefs)
            queue.add(originBatch, spillCallback)
            withResource(projectBatch) { proBatch =>
              withResource(infoBatch) { inBatch =>
                val infoCVs = GpuColumnVector.extractColumns(inBatch).map(_.incRefCount())
                val pythonInputCVs = GpuColumnVector.extractColumns(proBatch).map(_.incRefCount())
                new ColumnarBatch(infoCVs ++ pythonInputCVs, proBatch.numRows())
              }
            }
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      }

      if (pyInputIterator.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          pyFuncs,
          PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
          argOffsets,
          pythonInputSchema,
          sessionLocalTimeZone,
          pythonRunnerConf,
          /* The whole group data should be written in a single call, so here is unlimited */
          Int.MaxValue,
          () => queue.finish(),
          pythonOutputSchema)

        val outputBatchIterator = pyRunner.compute(pyInputIterator, context.partitionId(), context)
        new Iterator[ColumnarBatch] {
          // for hasNext we are waiting on the queue to have something inserted into it
          // instead of waiting for a result to be ready from python. The reason for this
          // is to let us know the target number of rows in the batch that we want when reading.
          // It is a bit hacked up but it works. In the future when we support spilling we should
          // store the number of rows separate from the batch. That way we can get the target batch
          // size out without needing to grab the GpuSemaphore which we cannot do if we might block
          // on a read operation.
          // Besides, when the queue is empty, need to call the `hasNext` of the out iterator to
          // trigger reading and handling the control data followed with the stream data.
          override def hasNext: Boolean = queue.hasNext || outputBatchIterator.hasNext

          private [this] def combine(
                                      origBatch: ColumnarBatch,
                                      retBatch: ColumnarBatch): ColumnarBatch = {
            val lColumns = GpuColumnVector.extractColumns(origBatch)
            val rColumns = GpuColumnVector.extractColumns(retBatch)
            new ColumnarBatch(lColumns.map(_.incRefCount()) ++ rColumns.map(_.incRefCount()),
              origBatch.numRows())
          }

          override def next(): ColumnarBatch = {
            val numRows = queue.peekBatchSize
            // Update the expected batch size for next read
            pyRunner.minReadTargetBatchSize = numRows
            withResource(outputBatchIterator.next()) { cbFromPython =>
              assert(cbFromPython.numRows() == numRows)
              withResource(queue.remove()) { origBatch =>
                numOutputBatches += 1
                numOutputRows += numRows
                projectResult(combine(origBatch, cbFromPython))
              }
            }
          }
        } // End of new Iterator
      } else {
        // Empty partition, return the input iterator directly
        inputIter
      }

    } // End of mapPartitions
  } // End of doExecuteColumnar

}
