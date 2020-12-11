/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.execution.python

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import ai.rapids.cudf._
import com.nvidia.spark.rapids._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonEvalType,
  PythonFunction, PythonRDD, SpecialLengths}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * A simple queue that holds the pending batches that need to line up with
 * and combined with batches coming back from python
 */
class BatchQueue extends AutoCloseable with Arm {

  private var isSet = false
  private val queue: mutable.Queue[SpillableColumnarBatch] =
    mutable.Queue[SpillableColumnarBatch]()

  def add(batch: ColumnarBatch, spillCallback: RapidsBuffer.SpillCallback): Unit = synchronized {
    queue.enqueue(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
      spillCallback))
    if (!isSet) {
      // Wake up anyone waiting for the first batch.
      isSet = true
      notifyAll()
    }
  }

  def finish(): Unit = synchronized {
    if (!isSet) {
      // Wake up anyone waiting for the first batch.
      isSet = true
      notifyAll()
    }
  }

  def remove(): ColumnarBatch = synchronized {
    if (queue.isEmpty) {
      null
    } else {
      withResource(queue.dequeue()) { scp =>
        scp.getColumnarBatch()
      }
    }
  }

  def hasNext: Boolean = synchronized {
    if (!isSet) {
      wait()
    }
    queue.nonEmpty
  }

  /**
   * Get the number of rows in the next batch, without actually getting the batch.
   */
  def peekBatchSize: Int = synchronized {
    queue.head.numRows()
  }

  override def close(): Unit = synchronized {
    if (!isSet) {
      isSet = true
      notifyAll()
    }
    while(queue.nonEmpty) {
      queue.dequeue().close()
    }
  }
}

/**
 * Helper functions for [[GpuPythonUDF]]
 */
object GpuPythonUDF {
  private[this] val SCALAR_TYPES = Set(
    PythonEvalType.SQL_BATCHED_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
  )

  def isScalarPythonUDF(e: Expression): Boolean = {
    e.isInstanceOf[GpuPythonUDF] && SCALAR_TYPES.contains(e.asInstanceOf[GpuPythonUDF].evalType)
  }

  def isGroupedAggPandasUDF(e: Expression): Boolean = {
    e.isInstanceOf[GpuPythonUDF] &&
        e.asInstanceOf[GpuPythonUDF].evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF
  }

  // This is currently same as GroupedAggPandasUDF, but we might support new types in the future,
  // e.g, N -> N transform.
  def isWindowPandasUDF(e: Expression): Boolean = isGroupedAggPandasUDF(e)
}

/**
 * A serialized version of a Python lambda function. This is a special expression, which needs a
 * dedicated physical operator to execute it, and thus can't be pushed down to data sources.
 */
case class GpuPythonUDF(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId)
    extends Expression with GpuUnevaluable with NonSQLExpression with UserDefinedExpression
    with GpuAggregateWindowFunction {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def toString: String = s"$name(${children.mkString(", ")})"

  lazy val resultAttribute: Attribute = AttributeReference(toPrettySQL(this), dataType, nullable)(
    exprId = resultId)

  override def nullable: Boolean = true

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  // Support window things
  override val windowInputProjection: Seq[Expression] = Seq.empty
  override def windowAggregation(inputs: Seq[(ColumnVector, Int)]): AggregationOnColumn = {
    throw new UnsupportedOperationException(s"GpuPythonUDF should run in a Python process.")
  }
}

/**
 * A trait that can be mixed-in with `GpuArrowPythonRunner`. It implements the logic from
 * Python (Arrow) to GPU/JVM (ColumnarBatch).
 */
trait GpuPythonArrowOutput extends Arm { self: GpuArrowPythonRunner =>

  private var minReadTargetBatchSize: Int = 1

  /**
   * Update the expected batch size for next reading.
   */
  private[python] def updateMinReadTargetBatchSize(size: Int) = {
    minReadTargetBatchSize = size
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ReaderIterator(stream, writerThread, startTime, env, worker, releasedOrClosed, context) {

      private[this] var arrowReader: StreamedTableReader = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (arrowReader != null) {
          arrowReader.close()
          arrowReader = null
        }
      }

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          // Because of batching and other things we have to be sure that we release the semaphore
          // before any operation that could block. This is because we are using multiple threads
          // for a single task and the GpuSemaphore might not wake up both threads associated with
          // the task, so a reader can be blocked waiting for data, while a writer is waiting on
          // the semaphore
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          if (arrowReader != null && batchLoaded) {
            // The GpuSemaphore is acquired in a callback
            val table =
              withResource(new NvtxRange("read python batch", NvtxColor.DARK_GREEN)) { _ =>
                arrowReader.getNextIfAvailable(minReadTargetBatchSize)
              }
            if (table == null) {
              batchLoaded = false
              arrowReader.close()
              arrowReader = null
              read()
            } else {
              withResource(table) { table =>
                batchLoaded = true
                GpuColumnVector.from(table, GpuColumnVector.extractTypes(pythonOutSchema))
              }
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                val builder = ArrowIPCOptions.builder()
                builder.withCallback(() => GpuSemaphore.acquireIfNecessary(TaskContext.get()))
                arrowReader = Table.readArrowIPCChunked(builder.build(),
                  new StreamToBufferProvider(stream))
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }

      override protected def handleTimingData(): Unit = {
        // Timing data from worker
        val bootTime = stream.readLong()
        val initTime = stream.readLong()
        val finishTime = stream.readLong()
        val boot = bootTime - startTime
        val init = initTime - bootTime
        val finish = finishTime - initTime
        val total = finishTime - startTime
        logInfo("Times for task[%s]: total = %s, boot = %s, init = %s, finish = %s".format(
          context.taskAttemptId(), total, boot, init, finish))
        val memoryBytesSpilled = stream.readLong()
        val diskBytesSpilled = stream.readLong()
        context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
        context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
        listener.foreach(_.onReceivedTimingDataFromPython(finish))
      }
    }
  }
}


/**
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
class GpuArrowPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    val pythonOutSchema: StructType,
    val listener: Option[GpuPythonRunnerListener] = None)
      extends BasePythonRunner[ColumnarBatch, ColumnarBatch](funcs, evalType, argOffsets)
        with GpuPythonArrowOutput {

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4, "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val writer = {
          val builder = ArrowIPCWriterOptions.builder()
          builder.withMaxChunkSize(batchSize)
          builder.withCallback((table: Table) => {
            table.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
          })
          pythonInSchema.foreach { field =>
            if (field.nullable) {
              builder.withColumnNames(field.name)
            } else {
              builder.withNotNullableColumnNames(field.name)
            }
          }
          Table.writeArrowIPCChunked(builder.build(), new BufferToStreamWriter(dataOut))
        }
        Utils.tryWithSafeFinally {
          listener.foreach(_.onWriteIteratorToPythonStart())
          while(inputIterator.hasNext) {
            val table = withResource(inputIterator.next()) { nextBatch =>
              GpuColumnVector.from(nextBatch)
            }
            withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
              val rowCount = table.getRowCount
              // The callback will handle closing table and releasing the semaphore
              writer.write(table)
              listener.foreach(_.onWriteBatchToPythonFinished(rowCount))
            }
          }
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        } {
          writer.close()
          dataOut.flush()
          listener.foreach(_.onWriteIteratorToPythonFinished())
        }
      }
    }
  }
}

class BufferToStreamWriter(outputStream: DataOutputStream) extends HostBufferConsumer with Arm {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  override def handleBuffer(hostBuffer: HostMemoryBuffer, length: Long): Unit = {
    withResource(hostBuffer) { buffer =>
      var len = length
      var offset: Long = 0
      while(len > 0) {
        val toCopy = math.min(tempBuffer.length, len).toInt
        buffer.getBytes(tempBuffer, 0, offset, toCopy)
        outputStream.write(tempBuffer, 0, toCopy)
        len = len - toCopy
        offset = offset + toCopy
      }
    }
  }
}

class StreamToBufferProvider(inputStream: DataInputStream) extends HostBufferProvider {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  override def readInto(hostBuffer: HostMemoryBuffer, length: Long): Long = {
    var amountLeft = length
    var totalRead : Long = 0
    while (amountLeft > 0) {
      val amountToRead = Math.min(tempBuffer.length, amountLeft).toInt
      val amountRead = inputStream.read(tempBuffer, 0, amountToRead)
      if (amountRead <= 0) {
        // Reached EOF
        amountLeft = 0
      } else {
        amountLeft -= amountRead
        hostBuffer.setBytes(totalRead, tempBuffer, 0, amountRead)
        totalRead += amountRead
      }
    }
    totalRead
  }
}

/**
 * A trait to define some events to notify the callers of GPU python runner.
 * It is designed to calculate some metrics. Do not do any heavy things in the callbacks.
 */
trait GpuPythonRunnerListener {

  /**
   * Called just before starting to write the data iterator to the Python side.
   */
  def onWriteIteratorToPythonStart(): Unit = {}
  /**
   * Called after finishing writing all the data to the Python side.
   */
  def onWriteIteratorToPythonFinished(): Unit = {}

  /**
   * Called after finishing writing a batch to the Python side.
   * It map called multiple times if there are multiple batches.
   * @param batchRows the row number of the batch just written to python.
   */
  def onWriteBatchToPythonFinished(batchRows: Long): Unit = {}

  /**
   * Called when received the timing data from the Python side.
   *
   * @param pythonExecTime the time taken by the Python processing
   */
  def onReceivedTimingDataFromPython(pythonExecTime: Long): Unit = {}
}

/**
 * Class used to combine the original input batch and the result batch from the python side.
 *
 * @param inputBatchQueue the queue caching the original input batches.
 * @param pythonOutputIter the iterator of the result batch from the python side.
 * @param pythonArrowReader the gpu arrow reader to read batch from the python side.
 * @param numOutputRows metric for output rows.
 * @param numOutputBatches metric for output batches
 */
class CombiningIterator(
    inputBatchQueue: BatchQueue,
    pythonOutputIter: Iterator[ColumnarBatch],
    pythonArrowReader: GpuPythonArrowOutput,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric) extends Iterator[ColumnarBatch] with Arm {
  // for hasNext we are waiting on the queue to have something inserted into it
  // instead of waiting for a result to be ready from python. The reason for this
  // is to let us know the target number of rows in the batch that we want when reading.
  // It is a bit hacked up but it works. In the future when we support spilling we should
  // store the number of rows separate from the batch. That way we can get the target batch
  // size out without needing to grab the GpuSemaphore which we cannot do if we might block
  // on a read operation.
  // Besides, when the queue is empty, need to call the `hasNext` of the out iterator to
  // trigger reading and handling the control data followed with the stream data.
  override def hasNext: Boolean = inputBatchQueue.hasNext || pythonOutputIter.hasNext

  override def next(): ColumnarBatch = {
    val numRows = inputBatchQueue.peekBatchSize
    // Update the expected batch size for next read
    pythonArrowReader.updateMinReadTargetBatchSize(numRows)
    // Read next batch from python and combine it with the input batch by the left side.
    withResource(pythonOutputIter.next()) { cbFromPython =>
      assert(cbFromPython.numRows() == numRows)
      withResource(inputBatchQueue.remove()) { origBatch =>
        numOutputBatches += 1
        numOutputRows += numRows
        combine(origBatch, cbFromPython)
      }
    }
  }

  private[this] def combine(lBatch: ColumnarBatch, rBatch: ColumnarBatch): ColumnarBatch = {
    val lColumns = GpuColumnVector.extractColumns(lBatch).map(_.incRefCount())
    val rColumns = GpuColumnVector.extractColumns(rBatch).map(_.incRefCount())
    new ColumnarBatch(lColumns ++ rColumns, lBatch.numRows())
  }

}

object GpuPythonMetrics {
  // Metric names.
  // Common
  val NUM_TO_PYTHON_ROWS = "numToPythonRows"
  val NUM_TO_PYTHON_BATCHES = "numToPythonBatches"
  val WRITE_BATCHES_TIME = "writeBatchesTime"
  val PYTHON_EXECUTION_TIME ="pythonExecutionTime"
  // For windowing
  val COUNT_GROUP_TIME = "countGroupTime"
  val BUILD_BOUNDS_TIME = "buildBoundsTime"

  // Metric Descriptions.
  val DESCRIPTION_NUM_TO_PYTHON_ROWS = "number of rows sent to python"
  val DESCRIPTION_NUM_TO_PYTHON_BATCHES = "number of batches sent to python"
  val DESCRIPTION_WRITE_BATCHES_TIME = "write batches to python time"
  val DESCRIPTION_PYTHON_EXECUTION_TIME ="python execution time"
  // For windowing
  val DESCRIPTION_COUNT_GROUP_TIME = "count group sizes time"
  val DESCRIPTION_BUILD_BOUNDS_TIME = "build window bounds time"

  def createMetricsAndWakeUpListener(
      queue: BatchQueue,
      numToPythonRows: GpuMetric,
      numToPythonBatches: GpuMetric,
      writeBatchesTime: GpuMetric,
      pythonExecutionTime: GpuMetric): GpuPythonRunnerListener = new GpuPythonRunnerListener {
    private var writeBatchesStartTime: Long = System.currentTimeMillis()

    final override def onWriteIteratorToPythonFinished(): Unit = {
      // Notify the writing is done. This is for empty partition case, since there is no batch in
      // the queue, otherwise the CombiningIterator will wait on the queue forever.
      queue.finish()
      writeBatchesTime += (System.currentTimeMillis() - writeBatchesStartTime)
    }

    final override def onWriteIteratorToPythonStart(): Unit = {
      writeBatchesStartTime = System.currentTimeMillis()
    }

    final override def onWriteBatchToPythonFinished(batchRows: Long): Unit = {
      numToPythonRows += batchRows
      numToPythonBatches += 1
    }

    final override def onReceivedTimingDataFromPython(pythonExecTime: Long): Unit = {
      pythonExecutionTime += pythonExecTime
    }
  }
}
