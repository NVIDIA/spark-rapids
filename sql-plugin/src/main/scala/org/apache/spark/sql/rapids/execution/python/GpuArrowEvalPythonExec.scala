/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.cudf.Aggregation.SumAggregation
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * This iterator will round incoming batches to multiples of targetRoundoff rows, if possible.
 * The last batch might not be a multiple of it.
 * @param wrapped the incoming ColumnarBatch iterator.
 * @param targetRoundoff the target multiple number of rows
 * @param inputRows metric for rows read.
 * @param inputBatches metric for batches read
 */
class RebatchingRoundoffIterator(
    wrapped: Iterator[ColumnarBatch],
    schema: StructType,
    targetRoundoff: Int,
    inputRows: GpuMetric,
    inputBatches: GpuMetric,
    spillCallback: RapidsBuffer.SpillCallback)
    extends Iterator[ColumnarBatch] with Arm {
  var pending: Option[SpillableColumnarBatch] = None

  TaskContext.get().addTaskCompletionListener[Unit]{ _ =>
    pending.foreach(_.close())
    pending = None
  }

  override def hasNext: Boolean = pending.isDefined || wrapped.hasNext

  private[this] def popPending(): ColumnarBatch = {
    withResource(pending.get) { scb =>
      pending = None
      scb.getColumnarBatch()
    }
  }

  private[this] def concat(l: ColumnarBatch, r: ColumnarBatch): ColumnarBatch = {
    withResource(GpuColumnVector.from(l)) { lTable =>
      withResource(GpuColumnVector.from(r)) { rTable =>
        withResource(Table.concatenate(lTable, rTable)) { concatTable =>
          GpuColumnVector.from(concatTable, GpuColumnVector.extractTypes(l))
        }
      }
    }
  }

  private[this] def fillAndConcat(batches: ArrayBuffer[SpillableColumnarBatch]): ColumnarBatch = {
    var rowsSoFar = batches.map(_.numRows()).sum
    while (wrapped.hasNext && rowsSoFar < targetRoundoff) {
      val got = wrapped.next()
      inputBatches += 1
      inputRows += got.numRows()
      rowsSoFar += got.numRows()
      batches.append(SpillableColumnarBatch(got, SpillPriorities.ACTIVE_BATCHING_PRIORITY,
        spillCallback))
    }
    val toConcat = batches.safeMap(_.getColumnarBatch()).toArray
    ConcatAndConsumeAll.buildNonEmptyBatch(toConcat, schema)
  }

  override def next(): ColumnarBatch = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val combined : ColumnarBatch = if (pending.isDefined) {
      if (!wrapped.hasNext) {
        // No more data return what is in pending
        popPending()
      } else {
        // Don't read pending yet, because we are going to call next to get enough data.
        // The target number of rows is typically small enough that we will be able to do this
        // in a single call.
        val rowsNeeded = targetRoundoff - pending.get.numRows()
        val cb = wrapped.next()
        inputBatches += 1
        inputRows += cb.numRows()
        if (cb.numRows() >= rowsNeeded) {
          withResource(cb) { cb =>
            withResource(popPending()) { fromPending =>
              concat(fromPending, cb)
            }
          }
        } else {
          // If that does not work then we will need to fall back to slower special case code
          val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty
          try {
            val localPending = pending.get
            localPending.setSpillPriority(SpillPriorities.ACTIVE_BATCHING_PRIORITY)
            batches.append(localPending)
            pending = None
            batches.append(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY,
              spillCallback))
            fillAndConcat(batches)
          } finally {
            batches.safeClose()
          }
        }
      }
    } else {
      val cb = wrapped.next()
      inputBatches += 1
      inputRows += cb.numRows()
      if (cb.numRows() >= targetRoundoff) {
        cb
      } else {
        val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty
        try {
          batches.append(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY,
            spillCallback))
          fillAndConcat(batches)
        } finally {
          batches.safeClose()
        }
      }
    }

    val rc: Long = combined.numRows()

    if (rc % targetRoundoff == 0 || rc < targetRoundoff) {
      return combined
    }

    val splitIndex = (targetRoundoff * (rc/targetRoundoff)).toInt
    val split = withResource(combined) { combinedCb =>
      withResource(GpuColumnVector.from(combinedCb)) { combinedTable =>
        combinedTable.contiguousSplit(splitIndex)
      }
    }
    withResource(split) { split =>
      assert(pending.isEmpty)
      pending =
          Some(SpillableColumnarBatch(GpuColumnVectorFromBuffer.from(split.last,
            GpuColumnVector.extractTypes(schema)),
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
            spillCallback))
      GpuColumnVectorFromBuffer.from(split.head, GpuColumnVector.extractTypes(schema))
    }
  }
}

/**
 * A simple queue that holds the pending batches that need to line up with
 * and combined with batches coming back from python
 */
class BatchQueue extends AutoCloseable with Arm {
  private val queue: mutable.Queue[SpillableColumnarBatch] =
    mutable.Queue[SpillableColumnarBatch]()
  private var isSet = false

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
    // The generic parameter is here to enforce at compile time that we are doing something
    // that is allowed, but this is a special case and we might want to rething the type
    // hierarchy here a bit.
    with GpuAggregateWindowFunction[SumAggregation] {

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
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): AggregationOnColumn[SumAggregation] = {
    throw new UnsupportedOperationException(s"GpuPythonUDF should run in a Python process.")
  }
}

/**
 * A trait that can be mixed-in with `GpuArrowPythonRunner`. It implements the logic from
 * Python (Arrow) to GPU/JVM (ColumnarBatch).
 */
trait GpuPythonArrowOutput extends Arm { self: GpuArrowPythonRunner =>

  /**
   * Update the expected batch size for next reading.
   */
  private[python] final def updateMinReadTargetBatchSize(size: Int) = {
    self.minReadTargetBatchSize = size
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
                arrowReader.getNextIfAvailable(self.minReadTargetBatchSize)
              }
            if (table == null) {
              batchLoaded = false
              arrowReader.close()
              arrowReader = null
              read()
            } else {
              withResource(table) { table =>
                batchLoaded = true
                GpuColumnVector.from(table, GpuColumnVector.extractTypes(self.pythonOutSchema))
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
    onDataWriteFinished: () => Unit,
    val pythonOutSchema: StructType,
    var minReadTargetBatchSize: Int = 1)
    extends BasePythonRunner[ColumnarBatch, ColumnarBatch](funcs, evalType, argOffsets)
        with GpuPythonArrowOutput {

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
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
          // Flatten the names of nested struct columns, required by cudf arrow IPC writer.
          flattenNames(pythonInSchema).foreach { case (name, nullable) =>
              if (nullable) {
                builder.withColumnNames(name)
              } else {
                builder.withNotNullableColumnNames(name)
              }
          }
          Table.writeArrowIPCChunked(builder.build(), new BufferToStreamWriter(dataOut))
        }
        Utils.tryWithSafeFinally {
          while(inputIterator.hasNext) {
            val table = withResource(inputIterator.next()) { nextBatch =>
              GpuColumnVector.from(nextBatch)
            }
            withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
              // The callback will handle closing table and releasing the semaphore
              writer.write(table)
            }
          }
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        } {
          writer.close()
          dataOut.flush()
          if (onDataWriteFinished != null) onDataWriteFinished()
        }
      }

      private def flattenNames(d: DataType, nullable: Boolean=true): Seq[(String, Boolean)] =
        d match {
          case s: StructType =>
            s.flatMap(sf => Seq((sf.name, sf.nullable)) ++ flattenNames(sf.dataType, sf.nullable))
          case m: MapType =>
            flattenNames(m.keyType, nullable) ++ flattenNames(m.valueType, nullable)
          case a: ArrayType => flattenNames(a.elementType, nullable)
          case _ => Nil
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
 * A physical plan that evaluates a [[GpuPythonUDF]]. The transformation of the data to arrow
 * happens on the GPU (practically a noop), But execution of the UDFs are on the CPU.
 */
case class GpuArrowEvalPythonExec(
    udfs: Seq[GpuPythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int) extends UnaryExecNode with GpuExec {

  // We split the input batch up into small pieces when sending to python for compatibility reasons
  override def coalesceAfter: Boolean = true

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

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

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)


  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES)
  ) ++ spillMetrics

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)

    // cache in a local to avoid serializing the plan
    val inputSchema = child.output.toStructType
    // Build the Python output schema from UDF expressions instead of the 'resultAttrs', because
    // the 'resultAttrs' is NOT always equal to the Python output schema. For example,
    // On Databricks when projecting only one column from a Python UDF output where containing
    // multiple result columns, there will be only one attribute in the 'resultAttrs' for the
    // projecting output, but the output schema for this Python UDF contains multiple columns.
    val pythonOutputSchema = StructType.fromAttributes(udfs.map(_.resultAttribute))

    val childOutput = child.output
    val targetBatchSize = batchSize
    val runnerConf = pythonRunnerConf
    val timeZone = sessionLocalTimeZone

    val inputRDD = child.executeColumnar()
    inputRDD.mapPartitions { iter =>
      val queue: BatchQueue = new BatchQueue()
      val context = TaskContext.get()
      context.addTaskCompletionListener[Unit](_ => queue.close())

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // Not sure why we are doing this in every task.  It is not going to change, but it might
      // just be less that we have to ship.

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      // TODO eventually we should just do type checking on these, but that can get a little complex
      // with how things are setup for replacement...
      // perhaps it needs to be with the special, it is an gpu compatible expression, but not a
      // gpu expression...
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }.toArray

      val pythonInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      })

      val boundReferences = GpuBindReferences.bindReferences(allInputs, childOutput)
      val batchedIterator = new RebatchingRoundoffIterator(iter, inputSchema, targetBatchSize,
        numInputRows, numInputBatches, spillCallback)
      val pyInputIterator = batchedIterator.map { batch =>
        // We have to do the project before we add the batch because the batch might be closed
        // when it is added
        val ret = GpuProjectExec.project(batch, boundReferences)
        queue.add(batch, spillCallback)
        ret
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      if (pyInputIterator.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          pyFuncs,
          evalType,
          argOffsets,
          pythonInputSchema,
          timeZone,
          runnerConf,
          targetBatchSize,
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
                combine(origBatch, cbFromPython)
              }
            }
          }
        }
      } else {
        // Empty partition, return it directly
        iter
      }

    } // End of mapPartitions
  }
}
