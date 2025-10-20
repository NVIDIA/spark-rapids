/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
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

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, GatherMap, NvtxColor, NvtxRange, OrderByArg, OutOfBoundsPolicy, Scalar, Table}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Generic trait for all join gather instances.  A JoinGatherer takes the gather maps that are the
 * result of a cudf join call along with the data batches that need to be gathered and allow
 * someone to materialize the join in batches.  It also provides APIs to help decide on how
 * many rows to gather.
 */
trait JoinGatherer extends AutoCloseable with Retryable {
  /**
   * Gather the next n rows from the join gather maps.
   *
   * @param n how many rows to gather
   * @return the gathered data as a ColumnarBatch
   */
  def gatherNext(n: Int): ColumnarBatch

  /**
   * Is all of the data gathered so far.
   */
  def isDone: Boolean

  /**
   * Number of rows left to gather
   */
  def numRowsLeft: Long

  /**
   * A really fast and dirty way to estimate the size of each row in the join output measured as in
   * bytes.
   */
  def realCheapPerRowSizeEstimate: Double

  /**
   * Get the bit count size map for the next n rows to be gathered. It returns a column of
   * INT64 values. One for each of the next n rows requested. This is a bit count to deal with
   * validity bits, etc. This is an INT64 to allow a prefix sum (running total) to be done on
   * it without overflowing so we can compute an accurate cuttoff point for a batch size limit.
   */
  def getBitSizeMap(n: Int): ColumnView

  /**
   * If the data is all fixed width return the size of each row, otherwise return None.
   */
  def getFixedWidthBitSize: Option[Int]

  def checkpoint(): Unit

  def restore(): Unit

  /**
   * Do a complete/expensive job to get the number of rows that can be gathered to get close
   * to the targetSize for the final output.
   *
   * @param targetSize The target size in bytes for the final output batch.
   */
  def gatherRowEstimate(targetSize: Long): Int = {
    val bitSizePerRow = getFixedWidthBitSize
    if (bitSizePerRow.isDefined) {
      Math.min(Math.min((targetSize / bitSizePerRow.get) * 8, numRowsLeft), Integer.MAX_VALUE).toInt
    } else {
      // WARNING magic number below. The rowEstimateMultiplier is arbitrary, we want to get
      // enough rows that we include that we go over the target size, but not too much so we
      // waste memory. It could probably be tuned better.
      val rowEstimateMultiplier = 1.1
      val estimatedRows = Math.min(
        ((targetSize / realCheapPerRowSizeEstimate) * rowEstimateMultiplier).toLong,
        numRowsLeft)
      val numRowsToProbe = Math.min(estimatedRows, Integer.MAX_VALUE).toInt
      if (numRowsToProbe <= 0) {
        1
      } else {
        val sum = withResource(getBitSizeMap(numRowsToProbe)) { bitSizes =>
          bitSizes.prefixSum()
        }
        val cutoff = withResource(sum) { sum =>
          // Lower bound needs tables, so we have to wrap everything in tables...
          withResource(new Table(sum)) { sumTable =>
            withResource(ai.rapids.cudf.ColumnVector.fromLongs(targetSize * 8)) { bound =>
              withResource(new Table(bound)) { boundTab =>
                sumTable.lowerBound(boundTab, OrderByArg.asc(0))
              }
            }
          }
        }
        withResource(cutoff) { cutoff =>
          withResource(cutoff.copyToHost()) { hostCutoff =>
            Math.max(1, hostCutoff.getInt(0))
          }
        }
      }
    }
  }
}

object JoinGatherer {
  def apply(gatherMap: SpillableGatherMap,
      inputData: SpillableColumnarBatch,
      outOfBoundsPolicy: OutOfBoundsPolicy): JoinGatherer =
    new JoinGathererImpl(gatherMap, inputData, outOfBoundsPolicy)

  def apply(leftMap: SpillableGatherMap,
      leftData: SpillableColumnarBatch,
      rightMap: SpillableGatherMap,
      rightData: SpillableColumnarBatch,
      outOfBoundsPolicyLeft: OutOfBoundsPolicy,
      outOfBoundsPolicyRight: OutOfBoundsPolicy): JoinGatherer = {
    val left = JoinGatherer(leftMap, leftData, outOfBoundsPolicyLeft)
    val right = JoinGatherer(rightMap, rightData, outOfBoundsPolicyRight)
    MultiJoinGather(left, right)
  }

  def getRowsInNextBatch(gatherer: JoinGatherer, targetSize: Long): Int = {
    withResource(new NvtxRange("calc gather size", NvtxColor.YELLOW)) { _ =>
      val rowsLeft = gatherer.numRowsLeft
      val rowEstimate: Long = gatherer.getFixedWidthBitSize match {
        case Some(fixedBitSize) =>
          // Odd corner cases for tests, make sure we do at least one row
          Math.max(1, (targetSize / fixedBitSize) * 8)
        case None =>
          // Heuristic to see if we need to do the expensive calculation
          if (rowsLeft * gatherer.realCheapPerRowSizeEstimate <= targetSize * 0.75) {
            rowsLeft
          } else {
            gatherer.gatherRowEstimate(targetSize)
          }
      }
      Math.min(Math.min(rowEstimate, rowsLeft), Integer.MAX_VALUE).toInt
    }
  }
}

trait SpillableGatherMap extends AutoCloseable {
  /**
   * How many rows total are in this gather map
   */
  val getRowCount: Long

  /**
   * Get a column view that can be used to gather.
   * @param startRow the row to start at.
   * @param numRows the number of rows in the map.
   */
  def toColumnView(startRow: Long, numRows: Int): ColumnView
}

object SpillableGatherMap {
  def apply(map: GatherMap, name: String): SpillableGatherMap =
    new SpillableGatherMapImpl(
      SpillableBuffer(map.releaseBuffer(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
      map.getRowCount(),
      name)

  def leftCross(leftCount: Int, rightCount: Int): SpillableGatherMap =
    new LeftCrossGatherMap(leftCount, rightCount)

  def rightCross(leftCount: Int, rightCount: Int): SpillableGatherMap =
    new RightCrossGatherMap(leftCount, rightCount)
}

/**
 * Holds a gather map that is also lazy spillable.
 */
class SpillableGatherMapImpl(
    mapBuffer: SpillableBuffer,
    rowCount: Long,
    name: String) extends SpillableGatherMap {

  override val getRowCount: Long = rowCount

  override def toColumnView(startRow: Long, numRows: Int): ColumnView = {
    withResource(getBuffer) { buff =>
      ColumnView.fromDeviceBuffer(buff, startRow * 4L, DType.INT32, numRows)
    }
  }

  private def getBuffer = mapBuffer.getDeviceBuffer()

  override def close(): Unit = {
    mapBuffer.close()
  }

  override def toString: String = s"SpillableGatherMap $name rowCount: $rowCount"
}

abstract class BaseCrossJoinGatherMap(leftCount: Int, rightCount: Int)
    extends SpillableGatherMap {
  override val getRowCount: Long = leftCount.toLong * rightCount.toLong

  override def toColumnView(startRow: Long, numRows: Int): ColumnView = withRetryNoSplit {
    withResource(GpuScalar.from(startRow, LongType)) { startScalar =>
      withResource(ai.rapids.cudf.ColumnVector.sequence(startScalar, numRows)) { rowNum =>
        compute(rowNum)
      }
    }
  }

  /**
   * Given a vector of INT64 row numbers compute the corresponding gather map (result should be
   * INT32)
   */
  def compute(rowNum: ai.rapids.cudf.ColumnVector): ai.rapids.cudf.ColumnVector

  override def close(): Unit = {
    // NOOP, we don't cache anything on the GPU
  }
}

class LeftCrossGatherMap(leftCount: Int, rightCount: Int) extends
    BaseCrossJoinGatherMap(leftCount, rightCount) {

  override def compute(rowNum: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(rightCount, IntegerType)) { rightCountScalar =>
      rowNum.div(rightCountScalar, DType.INT32)
    }
  }

  override def toString: String =
    s"LEFT CROSS MAP $leftCount by $rightCount"
}

class RightCrossGatherMap(leftCount: Int, rightCount: Int) extends
    BaseCrossJoinGatherMap(leftCount, rightCount) {

  override def compute(rowNum: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(rightCount, IntegerType)) { rightCountScalar =>
      rowNum.mod(rightCountScalar, DType.INT32)
    }
  }

  override def toString: String =
    s"RIGHT CROSS MAP $leftCount by $rightCount"
}

object JoinGathererImpl {

  /**
   * Calculate the row size in bits for a fixed width schema. If a type is encountered that is
   * not fixed width, or is not known a None is returned.
   */
  def fixedWidthRowSizeBits(dts: Seq[DataType]): Option[Int] =
    sumRowSizesBits(dts, nullValueCalc = false)

  /**
   * Calculate the null row size for a given schema in bits. If an unexpected type is encountered
   * an exception is thrown
   */
  def nullRowSizeBits(dts: Seq[DataType]): Int =
    sumRowSizesBits(dts, nullValueCalc = true).get


  /**
   * Sum the row sizes for each data type passed in. If any one of the sizes is not available
   * the entire result is considered to not be available. If nullValueCalc is true a result is
   * guaranteed to be returned or an exception thrown.
   */
  private def sumRowSizesBits(dts: Seq[DataType], nullValueCalc: Boolean): Option[Int] = {
    val allOptions = dts.map(calcRowSizeBits(_, nullValueCalc))
    if (allOptions.exists(_.isEmpty)) {
      None
    } else {
      Some(allOptions.map(_.get).sum)
    }
  }

  /**
   * Calculate the row bit size for the given data type. If nullValueCalc is false
   * then variable width types and unexpected types will result in a None being returned.
   * If it is true variable width types will have a value returned that corresponds to a
   * null, and unknown types will throw an exception.
   */
  private def calcRowSizeBits(dt: DataType, nullValueCalc: Boolean): Option[Int] = dt match {
    case StructType(fields) =>
      sumRowSizesBits(fields.map(_.dataType), nullValueCalc).map(_ + 1)
    case _: NumericType | DateType | TimestampType | BooleanType | NullType =>
      Some(GpuColumnVector.getNonNestedRapidsType(dt).getSizeInBytes * 8 + 1)
    case StringType | BinaryType | ArrayType(_, _) | MapType(_, _, _) if nullValueCalc =>
      // Single offset value and a validity value
      Some((DType.INT32.getSizeInBytes * 8) + 1)
    case x if nullValueCalc =>
      throw new IllegalArgumentException(s"Found an unsupported type $x")
    case _ => None
  }
}

/**
 * JoinGatherer for a single map/table
 */
class JoinGathererImpl(
    private val gatherMap: SpillableGatherMap,
    private val data: SpillableColumnarBatch,
    boundsCheckPolicy: OutOfBoundsPolicy) extends JoinGatherer {

  assert(data.numCols() > 0, "data with no columns should have been filtered out already")

  // How much of the gather map we have output so far
  private var gatheredUpTo: Long = 0
  private var gatheredUpToCheckpoint: Long = 0
  private val totalRows: Long = gatherMap.getRowCount
  private val (fixedWidthRowSizeBits, nullRowSizeBits) = {
    val dts = data.dataTypes
    val fw = JoinGathererImpl.fixedWidthRowSizeBits(dts)
    val nullVal = JoinGathererImpl.nullRowSizeBits(dts)
    (fw, nullVal)
  }

  override def checkpoint: Unit = {
    gatheredUpToCheckpoint = gatheredUpTo
  }

  override def restore: Unit = {
    gatheredUpTo = gatheredUpToCheckpoint
  }

  override def toString: String = {
    s"GATHERER $gatheredUpTo/$totalRows $gatherMap $data"
  }

  override def realCheapPerRowSizeEstimate: Double = {
    val totalInputRows: Int = data.numRows
    val totalInputSize: Long = data.sizeInBytes
    // Avoid divide by 0 here and later on
    if (totalInputRows > 0 && totalInputSize > 0) {
      totalInputSize.toDouble / totalInputRows
    } else {
      1.0
    }
  }

  override def getFixedWidthBitSize: Option[Int] = fixedWidthRowSizeBits

  override def gatherNext(n: Int): ColumnarBatch = {
    val start = gatheredUpTo
    assert((start + n) <= totalRows)
    val ret = withResource(gatherMap.toColumnView(start, n)) { gatherView =>
      withResource(data.getColumnarBatch()) { batch =>
        val gatheredTable = withResource(GpuColumnVector.from(batch)) { table =>
          table.gather(gatherView, boundsCheckPolicy)
        }
        withResource(gatheredTable) { gt =>
          GpuColumnVector.from(gt, GpuColumnVector.extractTypes(batch))
        }
      }
    }
    gatheredUpTo += n
    ret
  }

  override def isDone: Boolean =
    gatheredUpTo >= totalRows

  override def numRowsLeft: Long = totalRows - gatheredUpTo

  override def getBitSizeMap(n: Int): ColumnView = {
    val inputBitCounts = withResource(data.getColumnarBatch()) { batch =>
      withResource(GpuColumnVector.from(batch)) { table =>
        withResource(table.rowBitCount()) { bits =>
          bits.castTo(DType.INT64)
        }
      }
    }
    // Gather the bit counts so we know what the output table will look like
    val gatheredBitCount = withResource(inputBitCounts) { inputBitCounts =>
      withResource(gatherMap.toColumnView(gatheredUpTo, n)) { gatherView =>
        // Gather only works on a table so wrap the single column
        val gatheredTab = withResource(new Table(inputBitCounts)) { table =>
          table.gather(gatherView)
        }
        withResource(gatheredTab) { gatheredTab =>
          gatheredTab.getColumn(0).incRefCount()
        }
      }
    }
    // The gather could have introduced nulls in the case of outer joins. Because of that
    // we need to replace them with an appropriate size
    if (gatheredBitCount.hasNulls) {
      withResource(gatheredBitCount) { gatheredBitCount =>
        withResource(Scalar.fromLong(nullRowSizeBits.toLong)) { nullSize =>
          withResource(gatheredBitCount.isNull) { nullMask =>
            nullMask.ifElse(nullSize, gatheredBitCount)
          }
        }
      }
    } else {
      gatheredBitCount
    }
  }

  override def close(): Unit = {
    gatherMap.close()
    data.close()
  }
}

/**
 * JoinGatherer for the case where the gather produces the same table as the input table.
 */
class JoinGathererSameTable(
    private val data: SpillableColumnarBatch) extends JoinGatherer {

  assert(data.numCols() > 0, "data with no columns should have been filtered out already")

  // How much of the gather map we have output so far
  private var gatheredUpTo: Long = 0
  private var gatheredUpToCheckpoint: Long = 0
  private val totalRows: Long = data.numRows
  private val fixedWidthRowSizeBits = {
    val dts = data.dataTypes
    JoinGathererImpl.fixedWidthRowSizeBits(dts)
  }

  override def checkpoint: Unit = {
    gatheredUpToCheckpoint = gatheredUpTo
  }

  override def restore: Unit = {
    gatheredUpTo = gatheredUpToCheckpoint
  }

  override def toString: String = {
    s"SAMEGATHER $gatheredUpTo/$totalRows $data"
  }

  override def realCheapPerRowSizeEstimate: Double = {
    val totalInputRows: Int = data.numRows
    val totalInputSize: Long = data.sizeInBytes
    // Avoid divide by 0 here and later on
    if (totalInputRows > 0 && totalInputSize > 0) {
      totalInputSize.toDouble / totalInputRows
    } else {
      1.0
    }
  }

  override def getFixedWidthBitSize: Option[Int] = fixedWidthRowSizeBits

  override def gatherNext(n: Int): ColumnarBatch = {
    assert(gatheredUpTo + n <= totalRows)
    val ret = sliceForGather(n)
    gatheredUpTo += n
    ret
  }

  override def isDone: Boolean =
    gatheredUpTo >= totalRows

  override def numRowsLeft: Long = totalRows - gatheredUpTo

  override def getBitSizeMap(n: Int): ColumnView = {
    withResource(sliceForGather(n)) { cb =>
      withResource(GpuColumnVector.from(cb)) { table =>
        withResource(table.rowBitCount()) { bits =>
          bits.castTo(DType.INT64)
        }
      }
    }
  }

  override def close(): Unit = {
    data.close()
  }

  private def isFullBatchGather(n: Int): Boolean = gatheredUpTo == 0 && n == totalRows

  private def sliceForGather(n: Int): ColumnarBatch = {
    withResource(data.getColumnarBatch()) { cb =>
      if (isFullBatchGather(n)) {
        GpuColumnVector.incRefCounts(cb)
      } else {
        val splitStart = gatheredUpTo.toInt
        val splitEnd = splitStart + n
        val inputColumns = GpuColumnVector.extractColumns(cb)
        val outputColumns: Array[vectorized.ColumnVector] = inputColumns.safeMap { c =>
          val views = c.getBase.splitAsViews(splitStart, splitEnd)
          assert(views.length == 3, s"Unexpected number of views: ${views.length}")
          views(0).safeClose()
          views(2).safeClose()
          withResource(views(1)) { v =>
            GpuColumnVector.from(v.copyToColumnVector(), c.dataType())
          }
        }
        new ColumnarBatch(outputColumns, splitEnd - splitStart)
      }
    }
  }
}

/**
 * Join Gatherer for a left table and a right table
 */
case class MultiJoinGather(left: JoinGatherer, right: JoinGatherer) extends JoinGatherer {
  assert(left.numRowsLeft == right.numRowsLeft,
    "all gatherers much have the same number of rows to gather")

  override def gatherNext(n: Int): ColumnarBatch = {
    withResource(left.gatherNext(n)) { leftGathered =>
      withResource(right.gatherNext(n)) { rightGathered =>
        val vectors = Seq(leftGathered, rightGathered).flatMap { batch =>
          (0 until batch.numCols()).map { i =>
            val col = batch.column(i)
            col.asInstanceOf[GpuColumnVector].incRefCount()
            col
          }
        }.toArray
        new ColumnarBatch(vectors, n)
      }
    }
  }

  override def isDone: Boolean = left.isDone

  override def numRowsLeft: Long = left.numRowsLeft

  override def checkpoint: Unit = {
    left.checkpoint
    right.checkpoint
  }
  override def restore: Unit = {
    left.restore
    right.restore
  }

  override def realCheapPerRowSizeEstimate: Double =
    left.realCheapPerRowSizeEstimate + right.realCheapPerRowSizeEstimate

  override def getBitSizeMap(n: Int): ColumnView = {
    (left.getFixedWidthBitSize, right.getFixedWidthBitSize) match {
      case (Some(l), Some(r)) =>
        // This should never happen because all fixed width should be covered by
        // a faster code path. But just in case we provide it anyways.
        withResource(GpuScalar.from(l.toLong + r.toLong, LongType)) { s =>
          ai.rapids.cudf.ColumnVector.fromScalar(s, n)
        }
      case (Some(l), None) =>
        withResource(GpuScalar.from(l.toLong, LongType)) { ls =>
          withResource(right.getBitSizeMap(n)) { rightBits =>
            ls.add(rightBits, DType.INT64)
          }
        }
      case (None, Some(r)) =>
        withResource(GpuScalar.from(r.toLong, LongType)) { rs =>
          withResource(left.getBitSizeMap(n)) { leftBits =>
            rs.add(leftBits, DType.INT64)
          }
        }
      case _ =>
        withResource(left.getBitSizeMap(n)) { leftBits =>
          withResource(right.getBitSizeMap(n)) { rightBits =>
            leftBits.add(rightBits, DType.INT64)
          }
        }
    }
  }

  override def getFixedWidthBitSize: Option[Int] = {
    (left.getFixedWidthBitSize, right.getFixedWidthBitSize) match {
      case (Some(l), Some(r)) => Some(l + r)
      case _ => None
    }
  }

  override def close(): Unit = {
    left.close()
    right.close()
  }

  override def toString: String = s"MULTI-GATHER $left and $right"
}
