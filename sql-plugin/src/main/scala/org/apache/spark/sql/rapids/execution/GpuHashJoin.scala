/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
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

import ai.rapids.cudf.{ColumnView, DType, GatherMap, NvtxColor, NvtxRange, OrderByArg, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object JoinTypeChecks {
  def tagForGpu(joinType: JoinType, meta: RapidsMeta[_, _, _]): Unit = {
    val conf = meta.conf
    joinType match {
      case Inner if !conf.areInnerJoinsEnabled =>
        meta.willNotWorkOnGpu("inner joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_INNER_JOIN.key} to true")
      case Cross if !conf.areCrossJoinsEnabled =>
        meta.willNotWorkOnGpu("cross joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_CROSS_JOIN.key} to true")
      case LeftOuter if !conf.areLeftOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("left outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_OUTER_JOIN.key} to true")
      case RightOuter if !conf.areRightOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("right outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_RIGHT_OUTER_JOIN.key} to true")
      case FullOuter if !conf.areFullOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("full outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_FULL_OUTER_JOIN.key} to true")
      case LeftSemi if !conf.areLeftSemiJoinsEnabled =>
        meta.willNotWorkOnGpu("left semi joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_SEMI_JOIN.key} to true")
      case LeftAnti if !conf.areLeftAntiJoinsEnabled =>
        meta.willNotWorkOnGpu("left anti joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_ANTI_JOIN.key} to true")
      case _ => // not disabled
    }
  }
}

object GpuHashJoin extends Arm {

  def tagJoin(
      meta: RapidsMeta[_, _, _],
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)
    if (keyDataTypes.exists(dtype =>
      dtype.isInstanceOf[ArrayType] || dtype.isInstanceOf[StructType]
        || dtype.isInstanceOf[MapType])) {
      meta.willNotWorkOnGpu("Nested types in join keys are not supported")
    }
    JoinTypeChecks.tagForGpu(joinType, meta)
    joinType match {
      case _: InnerLike =>
      case FullOuter | RightOuter | LeftOuter | LeftSemi | LeftAnti =>
        if (condition.isDefined) {
          meta.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
        }
      case _ => meta.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
  }

  def extractTopLevelAttributes(
      exprs: Seq[Expression],
      includeAlias: Boolean): Seq[Option[Attribute]] =
    exprs.map {
      case a: AttributeReference => Some(a.toAttribute)
      case GpuAlias(a: AttributeReference, _) if includeAlias => Some(a.toAttribute)
      case _ => None
    }

  /**
   * Filter rows from the batch where all of the keys are null.
   */
  def filterNulls(cb: ColumnarBatch, boundKeys: Seq[Expression]): ColumnarBatch = {
    var mask: ai.rapids.cudf.ColumnVector = null
    try {
      withResource(GpuProjectExec.project(cb, boundKeys)) { keys =>
        val keyColumns = GpuColumnVector.extractBases(keys)
        // to remove a row all of the key columns must be null for that row
        // If there is even one key column with no nulls in it, don't filter anything
        // we do this by leaving mask as null
        if (keyColumns.forall(_.hasNulls)) {
          keyColumns.foreach { column =>
            withResource(column.isNull) { nn =>
              if (mask == null) {
                mask = nn.incRefCount()
              } else {
                mask = withResource(mask) { _ =>
                  mask.and(nn)
                }
              }
            }
          }
        }
      }

      if (mask == null) {
        // There was nothing to filter.
        GpuColumnVector.incRefCounts(cb)
      } else {
        val colTypes = GpuColumnVector.extractTypes(cb)
        withResource(GpuColumnVector.from(cb)) { tbl =>
          withResource(tbl.filter(mask)) { filtered =>
            GpuColumnVector.from(filtered, colTypes)
          }
        }
      }
    } finally {
      if (mask != null) {
        mask.close()
        mask = null
      }
    }
  }
}

/**
 * Generic trait for all join gather instances.
 * All instances should be spillable.
 * The life cycle of this assumes that when it is created that the data and
 * gather maps will be used shortly.
 * If you are not going to use these for a while, like when returning from an iterator,
 * then allowSpilling should be called so that the cached data is released and spilling
 * can be allowed.  If you need/want to use the data again, just start using it, and it
 * will be cached yet again until allowSpilling is called.
 * When you are completely done with this object call close on it.
 */
trait JoinGatherer extends AutoCloseable with Arm {
  /**
   * Gather the next n rows from the join gather maps.
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
   * Indicate that we are done messing with the data for now and it can be spilled.
   */
  def allowSpilling(): Unit

  /**
   * A really fast and dirty way to estimate the size of each row in the join output
   */
  def realCheapPerRowSizeEstimate: Double

  /**
   * Get the bit count size map for the next n rows to be gathered.
   */
  def getBitSizeMap(n: Int): ColumnView

  /**
   * If the data is all fixed width return the size of each row, otherwise return null.
   */
  def getFixedWidthBitSize: Option[Int]

  /**
   * Do a complete/expensive job to get the number of rows that can be gathered to get close
   * to the targetSize for the final output.
   */
  def gatherRowEstimate(targetSize: Long): Int = {
    val bitSizePerRow = getFixedWidthBitSize
    if (bitSizePerRow.isDefined) {
      Math.min(Math.min((targetSize/bitSizePerRow.get) / 8, numRowsLeft), Integer.MAX_VALUE).toInt
    } else {
      val estimatedRows = Math.min(
        ((targetSize / realCheapPerRowSizeEstimate) * 1.1).toLong,
        numRowsLeft)
      val numRowsToProbe = Math.min(estimatedRows, Integer.MAX_VALUE).toInt
      if (numRowsToProbe <= 0) {
        1
      } else {
        val sum = withResource(getBitSizeMap(numRowsToProbe)) { bitSizes =>
          bitSizes.prefixSum()
        }
        val cutoff = withResource(sum) { sum =>
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
            hostCutoff.getInt(0)
            hostCutoff.getInt(0)
          }
        }
      }
    }
  }
}

/**
 * Holds a columnar batch that is cached until it is marked that it can be spilled.
 */
class LazySpillableColumnarBatch(
    cb: ColumnarBatch,
    spillCallback: SpillCallback) extends AutoCloseable with Arm {

  private var cached: Option[ColumnarBatch] = Some(GpuColumnVector.incRefCounts(cb))
  private var spill: Option[SpillableColumnarBatch] = None
  val numRows: Int = cb.numRows()
  val deviceMemorySize: Long = GpuColumnVector.getTotalDeviceMemoryUsed(cb)
  val dataTypes: Array[DataType] = GpuColumnVector.extractTypes(cb)
  val numCols: Int = dataTypes.length

  def getBatch: ColumnarBatch = synchronized {
    if (cached.isEmpty) {
      cached = Some(spill.get.getColumnarBatch())
    }
    cached.get
  }

  def allowSpilling(): Unit = synchronized {
    if (spill.isEmpty && cached.isDefined) {
      // First time we need to allow for spilling
      spill = Some(SpillableColumnarBatch(cached.get,
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
        spillCallback))
      // Putting data in a SpillableColumnarBatch takes ownership of it.
      cached = None
    }
    cached.foreach(_.close())
    cached = None
  }

  override def close(): Unit = synchronized {
    cached.foreach(_.close())
    cached = None
    spill.foreach(_.close())
    spill = None
  }
}

object JoinGathererImpl {

  /**
   * Calculate the row size in bits for a fixed width schema. If a type is encountered that is
   * not fixed width, or is not known a None is returned.
   */
  def fixedWidthRowSizeBits(dts: Seq[DataType]): Option[Int] =
    sumRowSizesBits(dts, nullValueCalc = false)

  /**
   * Calculate the null row size for a given schema in bits. If an unexpected type is enountered
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
      Some(allOptions.map(_.get).sum + 1)
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
      sumRowSizesBits(fields.map(_.dataType), nullValueCalc)
    case dt: DecimalType if dt.precision > DType.DECIMAL64_MAX_PRECISION =>
      if (nullValueCalc) {
        throw new IllegalArgumentException(s"Found an unsupported type $dt")
      } else {
        None
      }
    case _: NumericType | DateType | TimestampType | BooleanType | NullType =>
      Some(GpuColumnVector.getNonNestedRapidsType(dt).getSizeInBytes * 8 + 1)
    case StringType | BinaryType | ArrayType(_, _) if nullValueCalc =>
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
    // TODO need a way to spill/cache the GatherMap
    private val gatherMap: GatherMap,
    private val data: LazySpillableColumnarBatch,
    private val closeData: Boolean) extends JoinGatherer {

  // How much of the gather map we have output so far
  private var gatheredUpTo: Long = 0
  private val totalRows: Long = gatherMap.getRowCount
  private val totalInputRows: Int = data.numRows
  private val totalInputSize: Long = data.deviceMemorySize
  private val (fixedWidthRowSizeBits, nullRowSizeBits) = {
    val dts = data.dataTypes
    val fw = JoinGathererImpl.fixedWidthRowSizeBits(dts)
    val nullVal = JoinGathererImpl.nullRowSizeBits(dts)
    (fw, nullVal)
  }

  override def realCheapPerRowSizeEstimate: Double = {
    // Avoid divide by 0 here and later on
    if (totalInputRows > 0 && totalInputSize > 0) {
      totalInputSize.toDouble / totalInputRows
    } else {
      1.0
    }
  }

  override def getFixedWidthBitSize: Option[Int] = fixedWidthRowSizeBits

  override def gatherNext(n: Int): ColumnarBatch = synchronized {
    val start = gatheredUpTo
    assert((start + n) <= totalRows)
    val ret = withResource(gatherMap.toColumnView(start, n)) { gatherView =>
      val batch = data.getBatch
      val gatheredTab = withResource(GpuColumnVector.from(batch)) { table =>
        table.gather(gatherView)
      }
      withResource(gatheredTab) { gt =>
        GpuColumnVector.from(gt, GpuColumnVector.extractTypes(batch))
      }
    }
    gatheredUpTo += n
    ret
  }

  override def isDone: Boolean = synchronized {
    gatheredUpTo >= totalRows
  }

  override def numRowsLeft: Long = totalRows - gatheredUpTo

  override def allowSpilling(): Unit = {
    data.allowSpilling()
  }

  override def getBitSizeMap(n: Int): ColumnView = synchronized {
    val cb = data.getBatch
    val inputBitCounts = withResource(GpuColumnVector.from(cb)) { table =>
      withResource(table.rowBitCount()) { bits =>
        bits.castTo(DType.INT64)
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

  override def close(): Unit = synchronized {
    gatherMap.close()
    if (closeData) {
      data.close()
    } else {
      data.allowSpilling()
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

  override def allowSpilling(): Unit = {
    left.allowSpilling()
    right.allowSpilling()
  }

  override def realCheapPerRowSizeEstimate: Double =
    left.realCheapPerRowSizeEstimate + right.realCheapPerRowSizeEstimate

  override def getBitSizeMap(n: Int): ColumnView = {
    (left.getFixedWidthBitSize, right.getFixedWidthBitSize) match {
      case (Some(l), Some(r)) => // This should never happen, but just in case
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
}

object JoinGatherer extends Arm {
  def apply(gatherMap: GatherMap,
      inputData: LazySpillableColumnarBatch,
      closeData: Boolean): JoinGatherer =
    new JoinGathererImpl(gatherMap, inputData, closeData)

  def apply(leftMap: GatherMap,
      leftData: LazySpillableColumnarBatch,
      closeLeftData: Boolean,
      rightMap: GatherMap,
      rightData: LazySpillableColumnarBatch,
      closeRightData: Boolean): JoinGatherer = {
    val left = JoinGatherer(leftMap, leftData, closeLeftData)
    val right = JoinGatherer(rightMap, rightData, closeRightData)
    MultiJoinGather(left, right)
  }

  def getRowsInNextBatch(gatherer: JoinGatherer, targetSize: Long): Int = {
    withResource(new NvtxRange("calc gather size", NvtxColor.YELLOW)) { _ =>
      val rowsLeft = gatherer.numRowsLeft
      val rowEstimate: Long = gatherer.getFixedWidthBitSize match {
        case Some(fixedSize) =>
          // Odd corner cases for tests, make sure we do at least one row
          Math.max(1, (targetSize / fixedSize) / 8)
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

class HashJoinIterator(
    inputBuiltKeys: ColumnarBatch,
    inputBuiltData: ColumnarBatch,
    val stream: Iterator[ColumnarBatch],
    val boundStreamKeys: Seq[Expression],
    val boundStreamData: Seq[Expression],
    val streamAttributes: Seq[Attribute],
    val targetSize: Long,
    val joinType: JoinType,
    val buildSide: GpuBuildSide,
    val spillCallback: SpillCallback,
    streamTime: GpuMetric,
    joinTime: GpuMetric,
    totalTime: GpuMetric) extends Iterator[ColumnarBatch] with Arm {
  import scala.collection.JavaConverters._

  // For some join types even if there is no stream data we might output something
  private var initialJoin = true
  private var nextCb: Option[ColumnarBatch] = None
  private var gathererStore: Option[JoinGatherer] = None
  private val builtKeys = {
    val tmp = new LazySpillableColumnarBatch(inputBuiltKeys, spillCallback)
    // Close the input keys, the lazy spillable batch now owns it.
    inputBuiltKeys.close()
    tmp
  }
  private val builtData = {
    val tmp = new LazySpillableColumnarBatch(inputBuiltData, spillCallback)
    // Close the input data, the lazy spillable batch now owns it.
    inputBuiltData.close()
    tmp
  }

  def close(): Unit = {
    builtKeys.close()
    builtData.close()
    nextCb.foreach(_.close())
    nextCb = None
    gathererStore.foreach(_.close())
    gathererStore = None
  }

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  private def nextCbFromGatherer(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("hash join gather", NvtxColor.DARK_GREEN, joinTime)) { _ =>
      val ret = gathererStore.map { gather =>
        val nextRows = JoinGatherer.getRowsInNextBatch(gather, targetSize)
        gather.gatherNext(nextRows)
      }
      if (gathererStore.exists(_.isDone)) {
        gathererStore.foreach(_.close())
        gathererStore = None
      }

      if (ret.isDefined && gathererStore.isDefined) {
        // We are about to return something. We got everything we need from it so now let it spill
        // if there is more to be gathered later on.
        gathererStore.foreach(_.allowSpilling())
      }
      ret
    }
  }

  private def makeGatherer(
      maps: Array[GatherMap],
      leftData: LazySpillableColumnarBatch,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    // The joiner should own/close the data that is on the stream side
    // the build side is owned by the iterator.
    val (joinerOwnsLeftData, joinerOwnsRightData) = buildSide match {
      case GpuBuildRight => (true, false)
      case GpuBuildLeft => (false, true)
    }
    val gatherer = maps.length match {
      case 1 =>
        if (joinerOwnsRightData) {
          rightData.close()
        }
        JoinGatherer(maps(0), leftData, joinerOwnsLeftData)
      case 2 => if (rightData.numCols == 0) {
        maps(1).close()
        if (joinerOwnsRightData) {
          rightData.close()
        }
        JoinGatherer(maps(0), leftData, joinerOwnsLeftData)
      } else {
        JoinGatherer(maps(0), leftData, joinerOwnsLeftData,
          maps(1), rightData, joinerOwnsRightData)
      }
      case other =>
        throw new IllegalArgumentException(s"Got back unexpected number of gather maps $other")
    }
    if (gatherer.isDone) {
      gatherer.close()
      None
    } else {
      Some(gatherer)
    }
  }

  private def joinGatherMapLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("hash join gather map", NvtxColor.ORANGE, joinTime)) { _ =>
      val maps = joinType match {
        case LeftOuter => leftKeys.leftJoinGatherMaps(rightKeys, false)
        case RightOuter =>
          // Reverse the output of the join, because we expect the right gather map to
          // always be on the right
          rightKeys.leftJoinGatherMaps(leftKeys, false).reverse
        case _: InnerLike => leftKeys.innerJoinGatherMaps(rightKeys, false)
        case LeftSemi => Array(leftKeys.leftSemiJoinGatherMap(rightKeys, false))
        case LeftAnti => Array(leftKeys.leftAntiJoinGatherMap(rightKeys, false))
        case FullOuter => leftKeys.fullJoinGatherMaps(rightKeys, false)
        case _ =>
          throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
              s" supported")
      }
      makeGatherer(maps, leftData, rightData)
    }
  }

  private def joinGatherMapLeftRight(
      leftKeys: ColumnarBatch,
      leftData: LazySpillableColumnarBatch,
      rightKeys: ColumnarBatch,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuColumnVector.from(leftKeys)) { leftKeysTab =>
      withResource(GpuColumnVector.from(rightKeys)) { rightKeysTab =>
        joinGatherMapLeftRight(leftKeysTab, leftData, rightKeysTab, rightData)
      }
    }
  }

  private def joinGatherMap(
      buildKeys: ColumnarBatch,
      buildData: LazySpillableColumnarBatch,
      streamKeys: ColumnarBatch,
      streamData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    buildSide match {
      case GpuBuildLeft =>
        joinGatherMapLeftRight(buildKeys, buildData, streamKeys, streamData)
      case GpuBuildRight =>
        joinGatherMapLeftRight(streamKeys, streamData, buildKeys, buildData)
    }
  }

  private def joinGatherMap(
      buildKeys: ColumnarBatch,
      buildData: LazySpillableColumnarBatch,
      streamCb: ColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuProjectExec.project(streamCb, boundStreamKeys)) { streamKeys =>
      withResource(GpuProjectExec.project(streamCb, boundStreamData)) { streamData =>
        joinGatherMap(buildKeys, buildData,
          streamKeys, new LazySpillableColumnarBatch(streamData, spillCallback))
      }
    }
  }

  override def hasNext: Boolean = {
    var mayContinue = true
    while (nextCb.isEmpty && mayContinue) {
      val startTime = System.nanoTime()
      if (gathererStore.exists(!_.isDone)) {
        nextCb = nextCbFromGatherer()
      } else if (stream.hasNext) {
        // Need to refill the gatherer
        gathererStore.foreach(_.close())
        gathererStore = None
        withResource(stream.next()) { cb =>
          streamTime += (System.nanoTime() - startTime)
          gathererStore = joinGatherMap(builtKeys.getBatch, builtData, cb)
        }
        nextCb = nextCbFromGatherer()
      } else if (initialJoin) {
        withResource(GpuColumnVector.emptyBatch(streamAttributes.asJava)) { cb =>
          gathererStore = joinGatherMap(builtKeys.getBatch, builtData, cb)
        }
        nextCb = nextCbFromGatherer()
      } else {
        mayContinue = false
      }
      totalTime += (System.nanoTime() - startTime)
      initialJoin = false
    }
    if (nextCb.isEmpty) {
      // Nothing is left to return so close ASAP.
      close()
    } else {
      builtKeys.allowSpilling()
    }
    nextCb.isDefined
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val ret = nextCb.get
    nextCb = None
    ret
  }
}


trait GpuHashJoin extends GpuExec {
  def left: SparkPlan
  def right: SparkPlan
  def joinType: JoinType
  def condition: Option[Expression]
  def leftKeys: Seq[Expression]
  def rightKeys: Seq[Expression]
  def buildSide: GpuBuildSide

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case GpuBuildLeft => (left, right)
    case GpuBuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
        leftKeys.map(_.dataType)
            .zip(rightKeys.map(_.dataType))
            .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case GpuBuildLeft => (leftKeys, rightKeys)
      case GpuBuildRight => (rightKeys, leftKeys)
    }
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }
  }

  // If we have a single batch streamed in then we will produce a single batch of output
  // otherwise it can get smaller or bigger, we just don't know.  When we support out of
  // core joins this will change
  override def outputBatching: CoalesceGoal = {
    val batching = buildSide match {
      case GpuBuildLeft => GpuExec.outputBatching(right)
      case GpuBuildRight => GpuExec.outputBatching(left)
    }
    if (batching == RequireSingleBatch) {
      RequireSingleBatch
    } else {
      null
    }
  }

  def dedupDataFromKeys(
      rightOutput: Seq[Attribute],
      rightKeys: Seq[Expression],
      leftKeys: Seq[Expression]): (Seq[Attribute], Seq[NamedExpression]) = {
    // This means that we need a mapping from what we remove on the right to what in leftData can
    // provide it. These are still symbolic references, so we are going to convert everything into
    // attributes, and use it to make out mapping.
    val leftKeyAttributes = GpuHashJoin.extractTopLevelAttributes(leftKeys, includeAlias = true)
    val rightKeyAttributes = GpuHashJoin.extractTopLevelAttributes(rightKeys, includeAlias = false)
    val zippedKeysMapping = rightKeyAttributes.zip(leftKeyAttributes)
    val rightToLeftKeyMap = zippedKeysMapping.filter {
      case (Some(_), Some(_: AttributeReference)) => true
      case _ => false
    }.map {
      case (Some(right), Some(left)) => (right.exprId, left)
      case _ => throw new IllegalStateException("INTERNAL ERROR THIS SHOULD NOT BE REACHABLE")
    }.toMap

    val rightData = rightOutput.filterNot(att => rightToLeftKeyMap.contains(att.exprId))
    val remappedRightOutput = rightOutput.map { att =>
      rightToLeftKeyMap.get(att.exprId)
          .map(leftAtt => GpuAlias(leftAtt, att.name)(att.exprId))
          .getOrElse(att)
    }
    (rightData, remappedRightOutput)
  }

  /**
   * Spark does joins rather simply. They do it row by row, and as such don't really worry
   * about how much space is being taken up. We are doing this in batches, and have the option to
   * deduplicate columns that we know are the same to save even more memory.
   *
   * As such we do the join in a few different stages.
   *
   * 1. We separate out the join keys from the data that will be gathered. The join keys are used
   * to produce a gather map, and then can be released. The data needs to stay until it has been
   * gathered. Depending on the type of join and what is being done the join output is likely to
   * contain the join keys twice. We don't want to do this because it takes up too much memory
   * so we remove the keys from the data for one side of the join.
   *
   * 2. After this we will do the join. We can produce multiple batches from a single
   * pair of input batches. The output of this stage is called the intermediate output and is the
   * data columns each side of the join smashed together.
   *
   * 3. In some cases there is a condition that filters out data from the join that should not be
   * included. In the CPU code the condition will operate on the intermediate output. In some cases
   * the condition may need to be rewritten to point to the deduplicated key column.
   *
   * 4. Finally we need to fix up the data to produce the correct output. This should be a simple
   * projection that puts the deduplicated keys back to where they need to be.
   */
  protected lazy val (leftData, rightData, intermediateOutput, finalProject) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val (leftData, remappedLeftOutput, rightData, remappedRightOutput) = joinType match {
      case FullOuter | RightOuter | LeftOuter =>
        // We cannot dedupe anything here because the we can get nulls in the key columns
        // at least one side
        (left.output, left.output, right.output, right.output)
      case _: InnerLike | LeftSemi | LeftAnti =>
        val (rightData, remappedRightData) = dedupDataFromKeys(right.output, rightKeys, leftKeys)
        (left.output, left.output, rightData, remappedRightData)
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }

    val intermediateOutput = leftData ++ rightData

    val finalProject: Seq[Expression] = joinType match {
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        remappedLeftOutput ++ remappedRightOutput
//      case j: ExistenceJoin =>
//        remappedLeftOutput :+ j.exists
      case LeftExistence(_) =>
        remappedLeftOutput
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }
    (leftData, rightData, intermediateOutput, finalProject)
  }

  protected lazy val (boundBuildKeys, boundBuildData,
      boundStreamKeys, boundStreamData,
      boundCondition, boundFinalProject) = {
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val ldata = GpuBindReferences.bindGpuReferences(leftData, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)
    val rdata = GpuBindReferences.bindGpuReferences(rightData, right.output)
    val boundCondition =
      condition.map(c => GpuBindReferences.bindGpuReference(c, intermediateOutput))
    val boundFinalProject = GpuBindReferences.bindGpuReferences(finalProject, intermediateOutput)

    buildSide match {
      case GpuBuildLeft => (lkeys, ldata, rkeys, rdata, boundCondition, boundFinalProject)
      case GpuBuildRight => (rkeys, rdata, lkeys, ldata, boundCondition, boundFinalProject)
    }
  }

  val localBuildOutput: Seq[Attribute] = buildPlan.output

  def doJoin(
      builtBatch: ColumnarBatch,
      stream: Iterator[ColumnarBatch],
      targetSize: Long,
      spillCallback: SpillCallback,
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      streamTime: GpuMetric,
      joinTime: GpuMetric,
      filterTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    val realTarget = Math.max(targetSize, 10 * 1024)

    val (builtKeys, builtData) = {
      val builtAnyNullable =
        (joinType == LeftSemi || joinType == LeftAnti) && boundBuildKeys.forall(_.nullable)

      val cb = if (builtAnyNullable) {
        GpuHashJoin.filterNulls(builtBatch, boundBuildKeys)
      } else {
        GpuColumnVector.incRefCounts(builtBatch)
      }

      withResource(cb) { cb =>
        closeOnExcept(GpuProjectExec.project(cb, boundBuildKeys)) { builtKeys =>
          (builtKeys, GpuProjectExec.project(cb, boundBuildData))
        }
      }
    }

    // The HashJoinIterator takes ownership of the built keys and built data. It will close
    // them when it is done
    val joinIterator =
      new HashJoinIterator(builtKeys, builtData, stream, boundStreamKeys, boundStreamData,
        streamedPlan.output, realTarget, joinType, buildSide, spillCallback,
        streamTime, joinTime, totalTime)
    val boundFinal = boundFinalProject
    if (boundCondition.isDefined) {
      val condition = boundCondition.get
      joinIterator.flatMap { cb =>
        joinOutputRows += cb.numRows()
        val tmp = GpuFilter(cb, condition, numOutputRows, numOutputBatches, filterTime)
        if (tmp.numRows == 0) {
          // Not sure if there is a better way to work around this
          numOutputBatches.set(numOutputBatches.value - 1)
          tmp.close()
          None
        } else {
          Some(GpuProjectExec.projectAndClose(tmp, boundFinal, NoopMetric))
        }
      }
    } else {
      joinIterator.map { cb =>
        joinOutputRows += cb.numRows()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        GpuProjectExec.projectAndClose(cb, boundFinal, NoopMetric)
      }
    }
  }
}
