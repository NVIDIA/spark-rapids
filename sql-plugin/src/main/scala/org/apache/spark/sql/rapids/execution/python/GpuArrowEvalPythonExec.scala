/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.shims.GpuArrowPythonRunner
import org.apache.spark.sql.rapids.shims.{ArrowUtilsShim, DataTypeUtilsShim}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

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
    inputBatches: GpuMetric)
    extends Iterator[ColumnarBatch] {
  var pending: Option[SpillableColumnarBatch] = None

  onTaskCompletion {
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
      batches.append(SpillableColumnarBatch(got, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
    }
    val toConcat = batches.toArray.safeMap(_.getColumnarBatch())
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
            batches.append(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
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
          batches.append(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
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
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      GpuColumnVectorFromBuffer.from(split.head, GpuColumnVector.extractTypes(schema))
    }
  }
}

/** A queue that consumes an Iterator and provides peek and remove operations */
trait IteratorBatchQueue {
  def peekBatchNumRows(): Int

  def remove(): ColumnarBatch
}

/**
 * An iterator that will cache the batches in a queue after pulling in to support
 * the IteratorBatchQueue.
 * The cached batches will be used for later combination each time getting
 * a result batch from the Python side.
 * It will transform the input batch by "cacheConverter" and cache the result.
 * It also supports to do a transforming for the output of "next" by "outputConverter".
 */
class BatchProducer(
    input: Iterator[ColumnarBatch],
    cacheConverter: ColumnarBatch => ColumnarBatch = GpuColumnVector.incRefCounts,
    outputConverter: ColumnarBatch => ColumnarBatch = GpuColumnVector.incRefCounts
) extends Iterator[ColumnarBatch] with AutoCloseable with IteratorBatchQueue {

  Option(TaskContext.get()).foreach(onTaskCompletion(_)(close()))

  // Cache for later combinations
  private val queue: mutable.Queue[SpillableColumnarBatch] =
    mutable.Queue[SpillableColumnarBatch]()

  // Cache for batches pulled in by calling to "peekBatchRowsNum". In fact,
  // there is usually only one batch. But using a queue here is because in theory
  // "peekBatchRowsNum" can be called multiple times, then more than one batch can
  // be pulled in.
  private val pending: mutable.Queue[SpillableColumnarBatch] =
    mutable.Queue[SpillableColumnarBatch]()

  private def convertAndAdd(
      cb: ColumnarBatch,
      cache: mutable.Queue[SpillableColumnarBatch],
      convert: ColumnarBatch => ColumnarBatch): Unit = {
    cache.enqueue(SpillableColumnarBatch(convert(cb), SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
  }

  /** Return and remove the first batch in the cache. */
  override def remove(): ColumnarBatch = synchronized {
    if (queue.isEmpty) {
      null
    } else {
      withResource(queue.dequeue()) { scb =>
        scb.getColumnarBatch()
      }
    }
  }

  /**
   * Get the number of rows in the next batch, without actually getting the batch.
   */
  override def peekBatchNumRows(): Int = synchronized {
    if (queue.isEmpty && input.hasNext) {
      // Pre-fetch one batch for peeking the rows number.
      withResource(input.next()) { cb =>
        convertAndAdd(cb, queue, cacheConverter)
        // This is not consumed by "next" so need to put it into pending.
        convertAndAdd(cb, pending, outputConverter)
      }
    }

    if (queue.nonEmpty) {
      queue.head.numRows()
    } else {
      0 // Should not go here but just in case.
    }
  }

  override def hasNext: Boolean = synchronized {
    pending.nonEmpty || input.hasNext
  }

  override def next(): ColumnarBatch = synchronized {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    if (pending.nonEmpty) {
      withResource(pending.dequeue()) { scb =>
        scb.getColumnarBatch()
      }
    } else {
      withResource(input.next()) { cb =>
        // cache it in queue
        convertAndAdd(cb, queue, cacheConverter)
        outputConverter(cb)
      }
    }
  }

  override def close(): Unit = synchronized {
    while(queue.nonEmpty) {
      queue.dequeue().close()
    }
    while(pending.nonEmpty) {
      pending.dequeue().close()
    }
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
    evalType: Int) extends ShimUnaryExecNode with GpuPythonExecBase {

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
  private val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (numInputRows, numInputBatches, numOutputRows, numOutputBatches) = commonGpuMetrics()

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)

    // cache in a local to avoid serializing the plan
    val inputSchema = child.output.toStructType
    // Build the Python output schema from UDF expressions instead of the 'resultAttrs', because
    // the 'resultAttrs' is NOT always equal to the Python output schema. For example,
    // On Databricks when projecting only one column from a Python UDF output where containing
    // multiple result columns, there will be only one attribute in the 'resultAttrs' for the
    // projecting output, but the output schema for this Python UDF contains multiple columns.
    val pythonOutputSchema = DataTypeUtilsShim.fromAttributes(udfs.map(_.resultAttribute))

    val childOutput = child.output
    val targetBatchSize = batchSize
    val runnerConf = pythonRunnerConf
    val timeZone = sessionLocalTimeZone

    val inputRDD = child.executeColumnar()
    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
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
      }.toArray)

      val boundReferences = GpuBindReferences.bindReferences(allInputs.toSeq, childOutput)
      val pyInputProducer = new BatchProducer(
        new RebatchingRoundoffIterator(iter, inputSchema, targetBatchSize, numInputRows,
          numInputBatches),
        outputConverter = GpuProjectExec.project(_, boundReferences))

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      if (pyInputProducer.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          pyFuncs,
          evalType,
          argOffsets,
          pythonInputSchema,
          timeZone,
          runnerConf,
          targetBatchSize,
          pythonOutputSchema)

        val outputIterator = pyRunner.compute(pyInputProducer, context.partitionId(), context)
        new CombiningIterator(pyInputProducer, outputIterator, pyRunner, numOutputRows,
          numOutputBatches)
      } else {
        // Empty partition, return it directly
        iter
      }

    } // End of mapPartitions
  }
}
