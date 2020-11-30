/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import ai.rapids.cudf.{Aggregation, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetricNames._
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
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuWindowInPandasExecMetaBase(
    winPandas: WindowInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[WindowInPandasExec](winPandas, conf, parent, rule) {

  override def couldReplaceMessage: String = "could partially run on GPU"
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

  override def isSupportedType(t: DataType): Boolean =
    GpuOverrides.isSupportedType(t, allowArray = true)
}

/**
 * This iterator will group the rows in the incoming batches per the window
 * "partitionBy" specification to make sure each group goes into only one batch, and
 * each batch contains only one group data.
 * @param wrapped the incoming ColumnarBatch iterator.
 * @param partitionSpec the partition specification of the window expression for this node.
 */
class GroupingIterator(
    wrapped: Iterator[ColumnarBatch],
    partitionSpec: Seq[Expression],
    inputRows: SQLMetric,
    inputBatches: SQLMetric) extends Iterator[ColumnarBatch] with Arm {

  // Currently do it in a somewhat ugly way. In the future cuDF will provide a dedicated API.
  // Current solution assumes one group data exists in only one batch, so just split the
  // batch into multiple batches to make sure one batch contains only one group.
  private val groupBatches: mutable.Queue[SpillableColumnarBatch] = mutable.Queue.empty

  TaskContext.get().addTaskCompletionListener[Unit]{ _ =>
    groupBatches.foreach(_.close())
    groupBatches.clear()
  }

  override def hasNext(): Boolean = groupBatches.nonEmpty || wrapped.hasNext

  override def next(): ColumnarBatch = {
    if (groupBatches.nonEmpty) {
      groupBatches.dequeue().getColumnarBatch()
    } else {
      val batch = wrapped.next()
      inputBatches += 1
      inputRows += batch.numRows()
      if (batch.numRows() > 0 && batch.numCols() > 0 && partitionSpec.nonEmpty) {
        val partitionIndices = partitionSpec.indices
        // 1) Calculate the split indices via cudf.Table.groupBy and aggregation Count.
        //   a) Compute the count number for each group in a batch, including null values.
        //   b) Restore the original order (Ascending with NullsFirst) defined in the plan,
        //      since cudf.Table.groupBy will break the original order.
        //   c) The 'count' column can be used to calculate the indices for split.
        val cntTable = withResource(GpuProjectExec.project(batch, partitionSpec)) { projected =>
          withResource(GpuColumnVector.from(projected)) { table =>
            table
              .groupBy(partitionIndices:_*)
              .aggregate(Aggregation.count(true).onColumn(0))
          }
        }
        val orderedTable = withResource(cntTable) { table =>
          table.orderBy(partitionIndices.map(id => Table.asc(id, true)): _*)
        }
        val (countHostCol, numRows) = withResource(orderedTable) { table =>
          // Yes copying the data to host, it would be OK since just copying the aggregated
          // count column.
          (table.getColumn(table.getNumberOfColumns - 1).copyToHost(), table.getRowCount)
        }
        val splitIndices = withResource(countHostCol) { cntCol =>
          // Verified the type of Count column is integer, so use getInt
          // Also drop the last count value due to the split API definition.
          (0L until (numRows - 1)).map(id => cntCol.getInt(id))
        }.scan(0)(_+_).tail

        // 2) Split the table when needed
        val resultBatch = if (splitIndices.nonEmpty) {
          // More than one group, split it and close this incoming batch
          withResource(batch) { cb =>
            withResource(GpuColumnVector.from(cb)) { table =>
              withResource(table.contiguousSplit(splitIndices:_*)) { tables =>
                // Return the first one and enqueue others
                val splitBatches = tables.safeMap(table =>
                  GpuColumnVectorFromBuffer.from(table, GpuColumnVector.extractTypes(batch)))
                groupBatches.enqueue(splitBatches.tail.map(sb =>
                  SpillableColumnarBatch(sb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)): _*)
                splitBatches.head
              }
            }
          }
        } else {
          // Only one group, return it directly
          batch
        }

        resultBatch
      } else {
        // Empty batch, or No partition defined for Window operation, return it directly.
        // When no partition is defined in window, there will be only one partition with one batch,
        // meaning one group.
        batch
      }
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
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(RequireSingleBatch)

  /*
   * Helper functions and data structures for window bounds
   *
   * It contains:
   * (1) Function from frame index to its lower bound column index in the python input row
   * (2) Function from frame index to its upper bound column index in the python input row
   * (3) Seq from frame index to its window bound type
   */
  private type WindowBoundHelpers = (Int => Int, Int => Int, Seq[WindowBoundType])

  /*
   * Enum for window bound types. Used only inside this class.
   */
  private sealed case class WindowBoundType(value: String)
  private object UnboundedWindow extends WindowBoundType("unbounded")
  private object BoundedWindow extends WindowBoundType("bounded")

  private val windowBoundTypeConf = "pandas_window_bound_types"

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

  /*
   * See [[WindowBoundHelpers]] for details.
   */
  private def computeWindowBoundHelpers: WindowBoundHelpers = {

    val windowBoundTypes = windowFramesWithExpressions.map(_._1).map { frame =>
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

    (lowerBoundIndex, upperBoundIndex, windowBoundTypes)
  }

  private def insertWindowBounds(batch: ColumnarBatch): ColumnarBatch = {
    val numRows = batch.numRows()
    def buildLowerCV(lower: Expression): GpuColumnVector = {
      lower match {
        case GpuSpecialFrameBoundary(UnboundedPreceding) =>
          // lower bound is always 0
          withResource(cudf.Scalar.fromInt(0)) { zeroVal =>
            GpuColumnVector.from(cudf.ColumnVector.fromScalar(zeroVal, numRows), IntegerType)
          }
        case GpuSpecialFrameBoundary(CurrentRow) =>
          // offset is 0, simply create a integer sequence starting with 0
          withResource(cudf.Scalar.fromInt(0)) { zeroVal =>
            GpuColumnVector.from(cudf.ColumnVector.sequence(zeroVal, numRows), IntegerType)
          }
        case GpuLiteral(offset, _) =>
          // 1) Create a integer sequence starting with (0 + offset)
          // 2) Replace the values less than 0 with 0 (offset is negative for lower bound)
          val startIndex = 0 + offset.asInstanceOf[Int]
          withResource(cudf.Scalar.fromInt(startIndex)) { startVal =>
            withResource(cudf.ColumnVector.sequence(startVal, numRows)) { offsetCV =>
              val loAndNullHi = Seq(cudf.Scalar.fromInt(0),
                cudf.Scalar.fromNull(cudf.DType.INT32))
              withResource(loAndNullHi) { loNullHi =>
                val lowerCV = offsetCV.clamp(loNullHi.head, loNullHi.last)
                GpuColumnVector.from(lowerCV, IntegerType)
              }
            }
          }
        case what =>
          throw new UnsupportedOperationException(s"Unsupported lower bound: ${what.prettyName}")
      }
    }

    def buildUpperCV(upper: Expression): GpuColumnVector = {
      upper match {
        case GpuSpecialFrameBoundary(UnboundedFollowing) =>
          // bound is always the length of the group, equal to numRows
          withResource(cudf.Scalar.fromInt(numRows)) { numRowsVal =>
            GpuColumnVector.from(cudf.ColumnVector.fromScalar(numRowsVal, numRows), IntegerType)
          }
        case GpuSpecialFrameBoundary(CurrentRow) =>
          // offset is 0, simply create a integer sequence starting with 1 for upper bound
          withResource(cudf.Scalar.fromInt(1)) { oneVal =>
            GpuColumnVector.from(cudf.ColumnVector.sequence(oneVal, numRows), IntegerType)
          }
        case GpuLiteral(offset, _) =>
          // 1) Create a integer sequence starting with (1 + offset)
          // 2) Replace the values larger than numRows with numRows
          //    (offset is positive for upper bound)
          val startIndex = 1 + offset.asInstanceOf[Int]
          withResource(cudf.Scalar.fromInt(startIndex)) { startVal =>
            withResource(cudf.ColumnVector.sequence(startVal, numRows)) { offsetCV =>
              val nullLoAndHi = Seq(cudf.Scalar.fromNull(cudf.DType.INT32),
                cudf.Scalar.fromInt(numRows))
              withResource(nullLoAndHi) { nullLoHi =>
                val upperCV = offsetCV.clamp(nullLoHi.head, nullLoHi.last)
                GpuColumnVector.from(upperCV, IntegerType)
              }
            }
          }
        case what =>
          throw new UnsupportedOperationException(s"Unsupported upper bound: ${what.prettyName}")
      }
    }

    val boundsCVs = windowFramesWithExpressions.map(_._1).flatMap { frame =>
      (frame.isUnbounded, frame.frameType) match {
        // Skip unbound window frames
        case (true, _) => Seq.empty
        case (false, RowFrame) => Seq(buildLowerCV(frame.lower), buildUpperCV(frame.upper))
        // Only support RowFrame here, should check it when replacing this node.
        case (false, RangeFrame) => throw new UnsupportedOperationException("Range frame" +
          " is not supported yet!")
        case (false, f) => throw new UnsupportedOperationException(s"Unsupported window frame $f")
      }
    }.toArray
    val dataCVs = GpuColumnVector.extractColumns(batch)
    new ColumnarBatch(boundsCVs ++ dataCVs.map(_.incRefCount()), numRows)
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_BATCHES),
    NUM_INPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_BATCHES)
  )

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric(NUM_INPUT_ROWS)
    val numInputBatches = longMetric(NUM_INPUT_BATCHES)
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val sessionLocalTimeZone = conf.sessionLocalTimeZone

    // 1) Unwrap the expressions and build some info data:
    //    - Map from expression index to frame index
    //    - Helper functions
    //    - isBounded by given a frame index
    val expressionsWithFrameIndex = windowFramesWithExpressions.map(_._2).zipWithIndex.flatMap {
      case (bufferEs, frameIndex) => bufferEs.map(expr => (expr, frameIndex))
    }
    val exprIndex2FrameIndex = expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap
    val (lowerBoundIndex, upperBoundIndex, frameWindowBoundTypes) =
      computeWindowBoundHelpers
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
      .map(expId => frameWindowBoundTypes(exprIndex2FrameIndex(expId)).value)
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
    val windowBoundsInput = windowFramesWithExpressions.indices.flatMap { frameIndex =>
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
    val retAttributes = windowExpression.map(_.asInstanceOf[NamedExpression].toAttribute)
    val pythonOutputSchema = StructType.fromAttributes(retAttributes)
    val childOutput = child.output

    // 8) Start processing.
    child.executeColumnar().mapPartitions { inputIter =>
      val context = TaskContext.get()
      val queue: BatchQueue = new BatchQueue()
      context.addTaskCompletionListener[Unit](_ => queue.close())

      val boundDataRefs = GpuBindReferences.bindGpuReferences(dataInputs, childOutput)
      // Re-batching the input data by GroupingIterator
      val boundPartitionRefs = GpuBindReferences.bindGpuReferences(partitionSpec, childOutput)
      val groupedIterator = new GroupingIterator(inputIter, boundPartitionRefs,
        numInputRows, numInputBatches)
      val pyInputIterator = groupedIterator.map { batch =>
        // We have to do the project before we add the batch because the batch might be closed
        // when it is added
        val projectedBatch = GpuProjectExec.project(batch, boundDataRefs)
        // Compute the window bounds and insert to the head of each row for one batch
        val inputBatch = withResource(projectedBatch) { projectedCb =>
          insertWindowBounds(projectedCb)
        }
        queue.add(batch)
        inputBatch
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      }

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
        override def hasNext: Boolean = queue.hasNext

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

    } // End of mapPartitions
  } // End of doExecuteColumnar

}
