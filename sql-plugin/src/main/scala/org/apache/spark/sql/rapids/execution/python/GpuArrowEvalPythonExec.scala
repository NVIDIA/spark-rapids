/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetricNames._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils


class RebatchingIterator(
    wrapped: Iterator[ColumnarBatch],
    targetRowSize: Int,
    inputRows: SQLMetric,
    inputBatches: SQLMetric)
    extends Iterator[ColumnarBatch] with Arm {
  var pending: Table = _

  override def hasNext: Boolean = pending != null || wrapped.hasNext

  private[this] def updatePending(): Unit = {
    if (pending == null) {
      withResource(wrapped.next()) { cb =>
        inputBatches += 1
        inputRows += cb.numRows()
        pending = GpuColumnVector.from(cb)
      }
    }
  }

  override def next(): ColumnarBatch = {
    updatePending()

    while (pending.getRowCount < targetRowSize) {
      if (wrapped.hasNext) {
        val combined = withResource(wrapped.next()) { cb =>
          inputBatches += 1
          inputRows += cb.numRows()
          withResource(GpuColumnVector.from(cb)) { nextTable =>
            Table.concatenate(pending, nextTable)
          }
        }
        pending.close()
        pending = combined
      } else {
        // No more to data so return what is left
        val ret = withResource(pending) { p =>
          GpuColumnVector.from(p)
        }
        pending = null
        return ret
      }
    }

    // We got lucky
    if (pending.getRowCount == targetRowSize) {
      val ret = withResource(pending) { p =>
        GpuColumnVector.from(p)
      }
      pending = null
      return ret
    }

    val split = pending.contiguousSplit(targetRowSize)
    split.foreach(_.getBuffer.close())
    pending.close()
    pending = split(1).getTable
    withResource(split.head.getTable) { ret =>
      GpuColumnVector.from(ret)
    }
  }
}

// TODO extend this with spilling and other wonderful things
class BatchQueue extends AutoCloseable {
  // TODO for now we will use an built in queue
  private val queue: mutable.Queue[ColumnarBatch] = mutable.Queue[ColumnarBatch]()

  def add(batch: ColumnarBatch): Unit = synchronized {
    // If you cannot add something blow up
    queue.enqueue(batch)
  }

  def remove(): ColumnarBatch = synchronized {
    if (queue.isEmpty) {
      null
    } else {
      queue.dequeue()
    }
  }

  override def close(): Unit = synchronized {
    while(queue.nonEmpty) {
      queue.dequeue().close()
    }
  }
}

/*
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

/*
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
    extends Expression with GpuUnevaluable with NonSQLExpression with UserDefinedExpression {

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
}

/*
 * A trait that can be mixed-in with `BasePythonRunner`. It implements the logic from
 * Python (Arrow) to GPU/JVM (ColumnarBatch).
 */
trait GpuPythonArrowOutput extends Arm { self: BasePythonRunner[_, ColumnarBatch] =>

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
          if (arrowReader != null && batchLoaded) {
            val table =
              withResource(new NvtxRange("read python batch", NvtxColor.DARK_GREEN)) { _ =>
                arrowReader.getNextIfAvailable
              }
            if (table == null) {
              batchLoaded = false
              arrowReader.close()
              arrowReader = null
              read()
            } else {
              withResource(table) { table =>
                batchLoaded = true
                GpuColumnVector.from(table)
              }
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                arrowReader = Table.readArrowIPCChunked(new StreamToBufferProvider(stream))
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


/*
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
class GpuArrowPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    schema: StructType,
    timeZoneId: String,
    conf: Map[String, String])
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
          schema.foreach { field =>
            if (field.nullable) {
              builder.withColumnNames(field.name)
            } else {
              builder.withNotNullableColumnNames(field.name)
            }
          }
          Table.writeArrowIPCChunked(builder.build(), new BufferToStreamWriter(dataOut))
        }
        Utils.tryWithSafeFinally {
          while(inputIterator.hasNext) {
            withResource(inputIterator.next()) { nextBatch =>
              withResource(GpuColumnVector.from(nextBatch)) { table =>
                withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
                  writer.write(table)
                }
              }
            }
          }
        } {
          writer.close()
          dataOut.flush()
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

/*
 * A physical plan that evaluates a [[GpuPythonUDF]]. The transformation of the data to arrow
 * happens on the GPU (practically a noop), But execution of the UDFs are on the CPU or GPU.
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

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_BATCHES),
    NUM_INPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_BATCHES)
  )

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val numInputRows = longMetric(NUM_INPUT_ROWS)
    val numInputBatches = longMetric(NUM_INPUT_BATCHES)

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
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

      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      })

      val boundReferences = GpuBindReferences.bindReferences(allInputs, child.output)
      val batchedIterator = new RebatchingIterator(iter, batchSize, numInputRows, numInputBatches)
      val projectedIterator = batchedIterator.map { batch =>
        queue.add(batch)
        GpuProjectExec.project(batch, boundReferences)
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      val outputBatchIterator = new GpuArrowPythonRunner(
        pyFuncs,
        evalType,
        argOffsets,
        schema,
        sessionLocalTimeZone,
        pythonRunnerConf).compute(projectedIterator,
        context.partitionId(),
        context)

      outputBatchIterator.map { outputBatch =>
        withResource(outputBatch) { outBatch =>
          withResource(queue.remove()) { origBatch =>
            val rows = origBatch.numRows()
            assert(outBatch.numRows() == rows)
            val lColumns = GpuColumnVector.extractColumns(origBatch)
            val rColumns = GpuColumnVector.extractColumns(outBatch)
            numOutputBatches += 1
            numOutputRows += rows
            new ColumnarBatch(lColumns.map(_.incRefCount()) ++ rColumns.map(_.incRefCount()),
              rows)
          }
        }
      }
    }
  }
}
