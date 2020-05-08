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

package ai.rapids.spark

import ai.rapids.cudf.{DType, Table, WindowAggregate, WindowOptions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, FrameType, NamedExpression, RangeFrame, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.rapids.{GpuAggregateExpression, GpuCount, GpuMax, GpuMin, GpuSum}
import org.apache.spark.sql.types.{CalendarIntervalType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

class GpuWindowExecMeta(windowExec: WindowExec,
                        conf: RapidsConf,
                        parent: Option[RapidsMeta[_, _, _]],
                        rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[WindowExec](windowExec, conf, parent, rule) {

  val windowExpressions: Seq[ExprMeta[NamedExpression]] =
    windowExec.windowExpression.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {

    // Implementation depends on receiving an `Alias` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[Alias])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
        "cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `Alias`.)"))

  }

  override def convertToGpu(): GpuExec = {
    GpuWindowExec(
      windowExpressions.map(_.convertToGpu().asInstanceOf[GpuAlias]),
      childPlans.head.convertIfNeeded()
    )
  }
}

case class GpuWindowExec(windowExpressionAliases: Seq[GpuAlias],
                         child: SparkPlan
                        ) extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = {
    child.output ++ windowExpressionAliases.map(_.toAttribute)
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  val windowExpressions: Seq[GpuWindowExpression] = windowExpressionAliases.map(_.child.asInstanceOf[GpuWindowExpression])
  val windowFunctions: Seq[GpuExpression] = windowExpressions.map(_.windowFunction)     // One per window expression.
  val unboundWindowFunctionExpressions: Seq[GpuExpression] = windowFunctions.map {
    case aggExpression: GpuAggregateExpression => aggExpression.aggregateFunction.inputProjection.head
    case _: GpuRowNumber => GpuLiteral(1, IntegerType) // row_number has no arguments. Placeholder for input.
    case anythingElse => throw new IllegalStateException(s"Unexpected window operation ${anythingElse.prettyName}")
  } // One per window expression.
  val windowFrameTypes: Seq[FrameType] = windowExpressions.map(_.windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame].frameType)
  val boundWindowAggregations: Seq[GpuExpression] = GpuBindReferences.bindReferences(unboundWindowFunctionExpressions, child.output)  // One per window expression.
  val boundWindowPartKeys : Seq[Seq[GpuExpression]] = windowExpressions.map(_.windowSpec.partitionSpec).map(GpuBindReferences.bindReferences(_, child.output)) // 1 set of part-keys per window-expression.
  val boundWindowSortKeys : Seq[Seq[GpuExpression]] = windowExpressions.map(_.windowSpec.orderSpec).map(_.map(_.child)).map(GpuBindReferences.bindReferences(_, child.output))
  val windowSortOrderIsAscending : Seq[Seq[Boolean]] = windowExpressions.map(_.windowSpec.orderSpec).map(_.map(_.isAscending))

  override protected def doExecute(): RDD[InternalRow]
  = throw new IllegalStateException(s"Row-based execution should not happen, in $this.")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val input = child.executeColumnar()
    input.map {
      cb => {

        var originalCols: Array[GpuColumnVector] = null
        var aggCols     : Array[GpuColumnVector] = null

        try {
          originalCols = GpuColumnVector.extractColumns(cb)
          originalCols.foreach(_.incRefCount())
          aggCols      = windowExpressions.indices.toArray.map(evaluateWindowExpression(cb, _))

          new ColumnarBatch( originalCols ++ aggCols, cb.numRows() )
        } finally {
          cb.close()
        }
      }
    }
  }

  private def evaluateWindowExpression(cb : ColumnarBatch, i : Int) : GpuColumnVector = {
    if (windowFrameTypes(i).equals(RangeFrame)) {
      evaluateRangeBasedWindowExpression(cb, i)
    }
    else {
      evaluateRowBasedWindowExpression(cb, i)
    }
  }

  private def evaluateRowBasedWindowExpression(cb : ColumnarBatch, i : Int) : GpuColumnVector = {

    var groupingColsCB : ColumnarBatch = null
    var aggregationColsCB : ColumnarBatch = null
    var groupingCols : Array[GpuColumnVector] = null
    var aggregationCols : Array[GpuColumnVector] = null
    var inputTable : Table = null
    var aggResultTable : Table = null

    try {
      // Project required column batches.
      groupingColsCB    = GpuProjectExec.project(cb, boundWindowPartKeys(i))
      aggregationColsCB = GpuProjectExec.project(cb, Seq(boundWindowAggregations(i)))
      // Extract required columns columns.
      groupingCols = GpuColumnVector.extractColumns(groupingColsCB)
      aggregationCols = GpuColumnVector.extractColumns(aggregationColsCB)

      inputTable = new Table( ( groupingCols ++ aggregationCols ).map(_.getBase) : _* )

      aggResultTable = inputTable.groupBy(0 until groupingColsCB.numCols(): _*)
        .aggregateWindows(
          GpuWindowExec.getRowBasedWindowFrame(
            groupingColsCB.numCols(),
            windowFunctions(i),
            windowExpressions(i).windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame]
          )
        )

      val aggColumn = windowFunctions(i) match {

        // Special-case handling for COUNT(1)/COUNT(*):
        // GpuCount aggregation expects to return LongType (INT64), but CUDF returns IntType (INT32) for COUNT() window
        // function. Must cast back up to INT64.
        case GpuAggregateExpression(GpuCount(_), _, _, _) =>
          aggResultTable.getColumn(0).castTo(DType.INT64) // Aggregation column is at index `0`.

        case _ =>
          val origAggColumn = aggResultTable.getColumn(0) // Aggregation column is at index `0`.
          origAggColumn.incRefCount()
          origAggColumn
      }

      GpuColumnVector.from(aggColumn)
    } finally {
      if (groupingColsCB != null) groupingColsCB.close()
      if (aggregationColsCB != null) aggregationColsCB.close()
      if (inputTable != null) inputTable.close()
      if (aggResultTable != null) aggResultTable.close()
    }
  }

  private def evaluateRangeBasedWindowExpression(cb : ColumnarBatch, i : Int) : GpuColumnVector = {

    var groupingColsCB : ColumnarBatch = null
    var sortColsCB : ColumnarBatch = null
    var aggregationColsCB : ColumnarBatch = null
    var groupingCols : Array[GpuColumnVector] = null
    var sortCols : Array[GpuColumnVector] = null
    var aggregationCols : Array[GpuColumnVector] = null
    var inputTable : Table = null
    var aggResultTable : Table = null

    try {
      // Project required column batches.
      groupingColsCB    = GpuProjectExec.project(cb, boundWindowPartKeys(i))
      assert(boundWindowSortKeys(i).size == 1, "Expected a single sort column.")
      assert(windowSortOrderIsAscending(i).size == 1, "Expected a single sort column.")
      sortColsCB        = GpuProjectExec.project(cb, boundWindowSortKeys(i))
      aggregationColsCB = GpuProjectExec.project(cb, Seq(boundWindowAggregations(i)))

      // Extract required columns columns.
      groupingCols = GpuColumnVector.extractColumns(groupingColsCB)
      sortCols        = GpuColumnVector.extractColumns(sortColsCB)
      aggregationCols = GpuColumnVector.extractColumns(aggregationColsCB)

      inputTable = new Table( ( groupingCols ++ sortCols ++ aggregationCols ).map(_.getBase) : _* )

      aggResultTable = inputTable.groupBy(0 until groupingColsCB.numCols(): _*)
        .aggregateWindowsOverTimeRanges(
          GpuWindowExec.getRangeBasedWindowFrame(
            groupingColsCB.numCols() + sortColsCB.numCols(),
            groupingColsCB.numCols(),
            windowFunctions(i),
            windowExpressions(i).windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame],
            windowSortOrderIsAscending(i).head
          )
        )

      val aggColumn = windowFunctions(i) match {

        // Special-case handling for COUNT(1)/COUNT(*):
        // GpuCount aggregation expects to return LongType (INT64), but CUDF returns IntType (INT32) for COUNT() window
        // function. Must cast back up to INT64.
        case GpuAggregateExpression(GpuCount(_), _, _, _) =>
          aggResultTable.getColumn(0).castTo(DType.INT64) // Aggregation column is at index `0`

        case _ =>
          val origAggColumn = aggResultTable.getColumn(0) // Aggregation column is at index `0`
          origAggColumn.incRefCount()
          origAggColumn
      }

      GpuColumnVector.from(aggColumn)
    } finally {
      if (groupingColsCB != null) groupingColsCB.close()
      if (sortColsCB != null) sortColsCB.close()
      if (aggregationColsCB != null) aggregationColsCB.close()
      if (inputTable != null) inputTable.close()
      if (aggResultTable != null) aggResultTable.close()
    }
  }

}

object GpuWindowExec {

  def getRowBasedWindowFrame(columnIndex : Int,
                             aggExpression : GpuExpression,
                             windowSpec : GpuSpecifiedWindowFrame)
  : WindowAggregate = {

    // FIXME: Currently, only negative or 0 values are supported.
    var lower = getBoundaryValue(windowSpec.lower)
    if(lower > 0) {
      throw new IllegalStateException(s"Lower-bounds ahead of current row is not supported. Found $lower")
    }

    // Now, translate the lower bound value to CUDF semantics:
    //  1. CUDF requires lower bound value to include the current row.
    //       i.e. If Spark's lower bound == 3, CUDF's lower bound == 2.
    //  2. Spark's lower_bound (preceding CURRENT ROW) as a negative offset. CUDF requires a positive number
    //       Note: UNBOUNDED PRECEDING implies lower == Int.MinValue, which needs special handling for negation.
    // The following covers both requirements:
    lower = Math.abs(lower-1)

    val upper = getBoundaryValue(windowSpec.upper)
    if (upper < 0) {
      throw new IllegalStateException(s"Upper-bounds behind of current row is not supported. Found $upper")
    }

    val windowOption = WindowOptions.builder().minPeriods(1)
      .window(lower, upper).build()

    aggExpression match {
      case gpuAggregateExpression : GpuAggregateExpression => gpuAggregateExpression.aggregateFunction match {
        case GpuCount(_) => WindowAggregate.count(columnIndex, windowOption)
        case GpuSum(_) => WindowAggregate.sum(columnIndex, windowOption)
        case GpuMin(_) => WindowAggregate.min(columnIndex, windowOption)
        case GpuMax(_) => WindowAggregate.max(columnIndex, windowOption)
        case anythingElse => throw new UnsupportedOperationException(s"Unsupported aggregation: ${anythingElse.prettyName}")
      }
      case _: GpuRowNumber => WindowAggregate.row_number(0, windowOption) // ROW_NUMBER does not depend on input column values.
      case anythingElse => throw new UnsupportedOperationException(s"Unsupported window aggregation: ${anythingElse.prettyName}")
    }
  }

  def getRangeBasedWindowFrame(aggColumnIndex : Int,
                               timeColumnIndex : Int,
                               aggExpression : GpuExpression,
                               windowSpec : GpuSpecifiedWindowFrame,
                               timestampIsAscending : Boolean)
  : WindowAggregate = {

    // FIXME: Currently, only negative or 0 values are supported.
    var lower = getBoundaryValue(windowSpec.lower)
    if (lower > 0) {
      throw new IllegalStateException(s"Lower-bounds ahead of current row is not supported. Found: $lower")
    }

    // Now, translate the lower bound value to CUDF semantics:
    // Spark's lower_bound (preceding CURRENT ROW) as a negative offset. CUDF requires a positive offset.
    // Note: UNBOUNDED PRECEDING implies lower == Int.MinValue, which needs special handling for negation.
    lower = if (lower == Int.MinValue) Int.MaxValue else Math.abs(lower)

    val upper = getBoundaryValue(windowSpec.upper)
    if(upper < 0) {
      throw new IllegalStateException(s"Upper-bounds behind current row is not supported. Found: $upper")
    }

    val windowOptionBuilder = WindowOptions.builder()
                                           .minPeriods(1)
                                           .window(lower,upper)
                                           .timestampColumnIndex(timeColumnIndex)
    if (timestampIsAscending) {
      windowOptionBuilder.timestampAscending()
    }
    else {
      windowOptionBuilder.timestampDescending()
    }

    val windowOption = windowOptionBuilder.build()

    aggExpression match {
      case gpuAggExpression : GpuAggregateExpression => gpuAggExpression.aggregateFunction match {
        case GpuCount(_) => WindowAggregate.count(aggColumnIndex, windowOption)
        case GpuSum(_) => WindowAggregate.sum(aggColumnIndex, windowOption)
        case GpuMin(_) => WindowAggregate.min(aggColumnIndex, windowOption)
        case GpuMax(_) => WindowAggregate.max(aggColumnIndex, windowOption)
        case anythingElse => throw new UnsupportedOperationException(s"Unsupported aggregation: ${anythingElse.prettyName}")
      }
      case anythingElse => throw new UnsupportedOperationException(s"Unsupported window aggregation: ${anythingElse.prettyName}")
    }
  }

  def getBoundaryValue(boundary : GpuExpression) : Int = boundary match {
    case literal: GpuLiteral if literal.dataType.equals(IntegerType) => literal.value.asInstanceOf[Int]
    case literal: GpuLiteral if literal.dataType.equals(CalendarIntervalType) => literal.value.asInstanceOf[CalendarInterval].days
    case special: GpuSpecialFrameBoundary => special.value
    case anythingElse => throw new UnsupportedOperationException(s"Unsupported window frame expression $anythingElse")
  }
}
