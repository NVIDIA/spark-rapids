/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import java.{util => ju}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.arrow.memory.ReferenceManager
import org.apache.arrow.vector.ValueVector

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.sql.vectorized.rapids.AccessibleArrowColumnVector

object HostColumnarToGpu extends Logging {

  // use reflection to get access to a private field in a class
  private def getClassFieldAccessible(className: String, fieldName: String) = {
    val classObj = Class.forName(className)
    val fields = classObj.getDeclaredFields.toList
    val field = fields.filter( x => {
      x.getName.contains(fieldName)
    }).head
    field.setAccessible(true)
    field
  }

  private lazy val accessorField = {
    getClassFieldAccessible("org.apache.spark.sql.vectorized.ArrowColumnVector", "accessor")
  }

  private lazy val vecField = {
    getClassFieldAccessible("org.apache.spark.sql.vectorized.ArrowColumnVector$ArrowVectorAccessor",
      "vector")
  }

  // use reflection to get value vector from ArrowColumnVector
  private def getArrowValueVector(cv: ColumnVector): ValueVector = {
    val arrowCV = cv.asInstanceOf[ArrowColumnVector]
    val accessor = accessorField.get(arrowCV)
    vecField.get(accessor).asInstanceOf[ValueVector]
  }

  def arrowColumnarCopy(
      cv: ColumnVector,
      ab: ai.rapids.cudf.ArrowColumnBuilder,
      rows: Int): ju.List[ReferenceManager] = {
    val valVector = cv match {
      case v: ArrowColumnVector =>
        try {
          getArrowValueVector(v)
        } catch {
          case e: Exception =>
            throw new IllegalStateException("Trying to read from a ArrowColumnVector but can't " +
              "access its Arrow ValueVector", e)
        }
      case av: AccessibleArrowColumnVector =>
        av.getArrowValueVector()
      case _ =>
        throw new IllegalStateException(s"Illegal column vector type: ${cv.getClass}")
    }

    val referenceManagers = new mutable.ListBuffer[ReferenceManager]

    def getBufferAndAddReference(getter: => (ByteBuffer, ReferenceManager)): ByteBuffer = {
      val (buf, ref) = getter
      referenceManagers += ref
      buf
    }

    val nullCount = valVector.getNullCount()
    val dataBuf = getBufferAndAddReference(ShimLoader.getSparkShims.getArrowDataBuf(valVector))
    val validity = getBufferAndAddReference(ShimLoader.getSparkShims.getArrowValidityBuf(valVector))
    // this is a bit ugly, not all Arrow types have the offsets buffer
    var offsets: ByteBuffer = null
    try {
      offsets = getBufferAndAddReference(ShimLoader.getSparkShims.getArrowOffsetsBuf(valVector))
    } catch {
      case _: UnsupportedOperationException =>
        // swallow the exception and assume no offsets buffer
    }
    ab.addBatch(rows, nullCount, dataBuf, validity, offsets)
    referenceManagers.result().asJava
  }

  def columnarCopy(cv: ColumnVector, b: ai.rapids.cudf.HostColumnVector.ColumnBuilder,
      nullable: Boolean, rows: Int): Unit = {
    (cv.dataType(), nullable) match {
      case (BooleanType, true) if cv.isInstanceOf[ArrowColumnVector] =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getBoolean(i))
          }
        }
      case (BooleanType, false) if cv.isInstanceOf[ArrowColumnVector] =>
        for (i <- 0 until rows) {
          b.append(cv.getBoolean(i))
        }
      case (ByteType | BooleanType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getByte(i))
          }
        }
      case (ByteType | BooleanType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getByte(i))
        }
      case (ShortType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getShort(i))
          }
        }
      case (ShortType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getShort(i))
        }
      case (IntegerType | DateType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getInt(i))
          }
        }
      case (IntegerType | DateType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getInt(i))
        }
      case (LongType | TimestampType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getLong(i))
          }
        }
      case (LongType | TimestampType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getLong(i))
        }
      case (FloatType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getFloat(i))
          }
        }
      case (FloatType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getFloat(i))
        }
      case (DoubleType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getDouble(i))
          }
        }
      case (DoubleType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getDouble(i))
        }
      case (StringType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.appendUTF8String(cv.getUTF8String(i).getBytes)
          }
        }
      case (StringType, false) =>
        for (i <- 0 until rows) {
          b.appendUTF8String(cv.getUTF8String(i).getBytes)
        }
      case (NullType, true) =>
        for (_ <- 0 until rows) {
          b.appendNull()
        }
      case (dt: DecimalType, nullable) =>
        if (nullable) {
          for (i <- 0 until rows) {
            if (cv.isNullAt(i)) {
              b.appendNull()
            } else {
              // The precision here matters for cpu column vectors (such as OnHeapColumnVector).
              if (DecimalType.is32BitDecimalType(dt)) {
                b.append(cv.getDecimal(i, dt.precision, dt.scale).toUnscaledLong.toInt)
              } else {
                b.append(cv.getDecimal(i, dt.precision, dt.scale).toUnscaledLong)
              }
            }
          }
        } else {
          if (DecimalType.is32BitDecimalType(dt)) {
            for (i <- 0 until rows) {
              b.append(cv.getDecimal(i, dt.precision, dt.scale).toUnscaledLong.toInt)
            }
          } else {
            for (i <- 0 until rows) {
              b.append(cv.getDecimal(i, dt.precision, dt.scale).toUnscaledLong)
            }
          }
        }
      case (t, _) =>
        throw new UnsupportedOperationException(s"Converting to GPU for $t is not currently " +
          s"supported")
    }
  }
}

/**
 * This iterator builds GPU batches from host batches. The host batches potentially use Spark's
 * UnsafeRow so it is not safe to cache these batches. Rows must be read and immediately written
 * to CuDF builders.
 */
class HostToGpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
    schema: StructType,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    collectTime: GpuMetric,
    concatTime: GpuMetric,
    totalTime: GpuMetric,
    peakDevMemory: GpuMetric,
    opName: String,
    useArrowCopyOpt: Boolean)
  extends AbstractGpuCoalesceIterator(iter,
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    collectTime,
    concatTime,
    totalTime,
    opName) {

  // RequireSingleBatch goal is intentionally not supported in this iterator
  assert(goal != RequireSingleBatch)

  var batchBuilder: GpuColumnVector.GpuColumnarBatchBuilderBase = _
  var totalRows = 0
  var maxDeviceMemory: Long = 0

  // the arrow cudf converter only supports primitive types and strings
  // decimals and nested types aren't supported yet
  private def arrowTypesSupported(schema: StructType): Boolean = {
    val dataTypes = schema.fields.map(_.dataType)
    dataTypes.forall(GpuOverrides.isSupportedType(_))
  }

  /**
   * Initialize the builders using an estimated row count based on the schema and the desired
   * batch size defined by [[RapidsConf.GPU_BATCH_SIZE_BYTES]].
   */
  override def initNewBatch(batch: ColumnarBatch): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }

    // when reading host batches it is essential to read the data immediately and pass to a
    // builder and we need to determine how many rows to allocate in the builder based on the
    // schema and desired batch size
    batchRowLimit = if (batch.numCols() > 0) {
       GpuBatchUtils.estimateRowCount(goal.targetSizeBytes,
         GpuBatchUtils.estimateGpuMemory(schema, 512), 512)
    } else {
      // when there aren't any columns, it generally means user is doing a count() and we don't
      // need to limit batch size because there isn't any actual data
      Integer.MAX_VALUE
    }

    // if no columns then probably a count operation so doesn't matter which builder we use
    // as we won't actually copy any data and we can't tell what type of data it is without
    // having a column
    if (useArrowCopyOpt && batch.numCols() > 0 &&
      arrowTypesSupported(schema) &&
      (batch.column(0).isInstanceOf[ArrowColumnVector] ||
        batch.column(0).isInstanceOf[AccessibleArrowColumnVector])) {
      logDebug("Using GpuArrowColumnarBatchBuilder")
      batchBuilder = new GpuColumnVector.GpuArrowColumnarBatchBuilder(schema)
    } else {
      logDebug("Using GpuColumnarBatchBuilder")
      batchBuilder = new GpuColumnVector.GpuColumnarBatchBuilder(schema, batchRowLimit)
    }
    totalRows = 0
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit = {
    val rows = batch.numRows()
    for (i <- 0 until batch.numCols()) {
      batchBuilder.copyColumnar(batch.column(i), i, schema.fields(i).nullable, rows)
    }
    totalRows += rows
  }

  override def getBatchDataSize(batch: ColumnarBatch): Long = {
    schema.fields.indices.map(GpuBatchUtils.estimateGpuMemory(schema, _, batch.numRows())).sum
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    // About to place data back on the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val ret = batchBuilder.build(totalRows)
    maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(ret)

    // refine the estimate for number of rows based on this batch
    batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes, maxDeviceMemory,
      ret.numRows())

    ret
  }

  override def cleanupConcatIsDone(): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }
    totalRows = 0
    peakDevMemory.set(maxDeviceMemory)
  }

  private var onDeck: Option[ColumnarBatch] = None

  override protected def hasOnDeck: Boolean = onDeck.isDefined
  override protected def saveOnDeck(batch: ColumnarBatch): Unit = onDeck = Some(batch)
  override protected def clearOnDeck(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }
  override protected def popOnDeck(): ColumnarBatch = {
    val ret = onDeck.get
    onDeck = None
    ret
  }
}

/**
 * Put columnar formatted data on the GPU.
 */
case class HostColumnarToGpu(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode
  with GpuExec {
  import GpuMetric._
  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    COLLECT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_COLLECT_TIME),
    CONCAT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_CONCAT_TIME),
    PEAK_DEVICE_MEMORY -> createMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY)
  )

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def outputBatching: CoalesceGoal = goal

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  /**
   * Returns an RDD[ColumnarBatch] that when mapped over will produce GPU-side column vectors
   * that are expected to be closed by its caller, not [[HostColumnarToGpu]].
   *
   * The expectation is that the only valid instantiation of this node is
   * as a child of a GPU exec node.
   *
   * @return an RDD of `ColumnarBatch`
   */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val collectTime = gpuLongMetric(COLLECT_TIME)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val peakDevMemory = gpuLongMetric(PEAK_DEVICE_MEMORY)

    // cache in a local to avoid serializing the plan
    val outputSchema = schema

    val batches = child.executeColumnar()

    val confUseArrow = new RapidsConf(child.conf).useArrowCopyOptimization
    batches.mapPartitions { iter =>
      new HostToGpuCoalesceIterator(iter, goal, outputSchema,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime, concatTime,
        totalTime, peakDevMemory, "HostColumnarToGpu", confUseArrow)
    }
  }
}
