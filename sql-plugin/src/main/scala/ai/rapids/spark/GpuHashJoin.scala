package ai.rapids.spark

import ai.rapids.cudf.{NvtxColor, Table}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, HashJoin}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuHashJoin {
  def isJoinTypeAllowed(joinType: JoinType): Boolean = joinType match {
    case LeftOuter => true
    case Inner => true
    case _ => false
  }
}

trait GpuHashJoin extends GpuExec with HashJoin {

  protected lazy val (gpuBuildKeys, gpuStreamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = GpuBindReferences.bindReferences(leftKeys.asInstanceOf[Seq[GpuExpression]], left.output)
    val rkeys = GpuBindReferences.bindReferences(rightKeys.asInstanceOf[Seq[GpuExpression]], right.output)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  /**
   * Place the columns in left and the columns in right into a single ColumnarBatch
   */
  def combine(left: ColumnarBatch, right: ColumnarBatch): ColumnarBatch = {
    val l = GpuColumnVector.extractColumns(left)
    val r = GpuColumnVector.extractColumns(right)
    val c = l ++ r
    new ColumnarBatch(c.asInstanceOf[Array[ColumnVector]], left.numRows())
  }

  // TODO eventually dedupe the keys
  lazy val joinKeyIndices: Range = gpuBuildKeys.indices

  val localBuildOutput: Seq[Attribute] = buildPlan.output
  // The first columns are the ones we joined on and need to remove
  lazy val joinIndices: Seq[Int] = output.indices.map(v => v + joinKeyIndices.length)

  def doJoin(builtTable: Table,
      streamedBatch: ColumnarBatch,
      condition: Option[GpuExpression],
      numOutputRows: SQLMetric,
      numJoinOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      joinTime: SQLMetric,
      filterTime: SQLMetric): ColumnarBatch = {

    val streamedTable = try {
      val streamedKeysBatch = GpuProjectExec.project(streamedBatch, gpuStreamedKeys)
      try {
        val combined = combine(streamedKeysBatch, streamedBatch)
        GpuColumnVector.from(combined)
      } finally {
        streamedKeysBatch.close()
      }
    } finally {
      streamedBatch.close()
    }

    val nvtxRange = new NvtxWithMetrics("hash join", NvtxColor.ORANGE, joinTime)
    val joined = try {
      buildSide match {
        case BuildLeft => doJoinLeftRight(builtTable, streamedTable)
        case BuildRight => doJoinLeftRight(streamedTable, builtTable)
      }
    } finally {
      streamedTable.close()
      nvtxRange.close()
    }

    numJoinOutputRows += joined.numRows()

    if (condition.isDefined) {
      GpuFilter(joined, condition.get, numOutputRows, numOutputBatches, filterTime)
    } else {
      numOutputRows += joined.numRows()
      numOutputBatches += 1
      joined
    }
  }

  def doJoinLeftRight(leftTable: Table, rightTable: Table): ColumnarBatch = {
    val joinedTable = joinType match {
      case LeftOuter => leftTable.onColumns(joinKeyIndices: _*)
        .leftJoin(rightTable.onColumns(joinKeyIndices: _*))
      case Inner =>
        leftTable.onColumns(joinKeyIndices: _*).innerJoin(rightTable.onColumns(joinKeyIndices: _*))
      case _ => throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
        s" supported")
    }
    try {
      val result = joinIndices.map(joinIndex =>
        GpuColumnVector.from(joinedTable.getColumn(joinIndex).incRefCount()))
        .toArray[ColumnVector]

      new ColumnarBatch(result, joinedTable.getRowCount.toInt)
    } finally {
      joinedTable.close()
    }
  }
}
