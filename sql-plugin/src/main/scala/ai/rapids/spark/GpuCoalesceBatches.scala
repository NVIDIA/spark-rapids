/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Table

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Consumes an Iterator of ColumnarBatches and concatenates them into a single ColumnarBatch.
 * The batches will be closed with this operation is done.
 */
object ConcatAndConsumeAll {
  private def conversionForConcat(cb: ColumnarBatch): Table = try {
    val asStringCat = GpuColumnVector.convertToStringCategoriesIfNeeded(cb)
    try {
      GpuColumnVector.from(asStringCat)
    } finally {
      asStringCat.close()
    }
  } finally {
    cb.close()
  }

  def buildBatchNoEmpty(arrayOfBatches: Array[ColumnarBatch]): ColumnarBatch = {
    if (arrayOfBatches.length == 1) {
      // Need to convert the strings to string categories to be consistent.
      val table = conversionForConcat(arrayOfBatches(0))
      try {
        GpuColumnVector.from(table)
      } finally {
        table.close()
      }
    } else {
      // TODO if this fails in the middle we may leak column vectors.
      val tables = arrayOfBatches.map(conversionForConcat)
      try {
        val combined = Table.concatenate(tables: _*)
        try {
          GpuColumnVector.from(combined)
        } finally {
          combined.close()
        }
      } finally {
        tables.foreach(_.close())
      }
    }
  }

  def buildBatch(arrayOfBatches: Array[ColumnarBatch], format: Seq[Attribute]): ColumnarBatch = {
    import collection.JavaConverters._
    if (arrayOfBatches.length <= 0) {
      GpuColumnVector.emptyBatch(format.asJava)
    } else {
      buildBatchNoEmpty(arrayOfBatches)
    }
  }

  def verifyGotSingleBatch(batches: Iterator[ColumnarBatch], format: Seq[Attribute]): ColumnarBatch = {
    import collection.JavaConverters._
    if (!batches.hasNext) {
      GpuColumnVector.emptyBatch(format.asJava)
    } else {
      val batch = batches.next()
      if (batches.hasNext) {
        batch.close()
        throw new IllegalStateException("Expected to only receive a single batch")
      }
      batch
    }
  }

  def apply(batches: Iterator[ColumnarBatch], format: Seq[Attribute]): ColumnarBatch = {
    buildBatch(batches.toArray, format)
  }
}

object CoalesceGoal {
  def max(a: CoalesceGoal, b: CoalesceGoal): CoalesceGoal = (a, b) match {
    case (RequireSingleBatch, _) => a
    case (_, RequireSingleBatch) => b
    case (PreferSingleBatch, _) => a
    case (_, PreferSingleBatch) => b
    case (TargetSize(aSize), TargetSize(bSize)) if aSize > bSize => a
    case _ => b
  }
}

sealed abstract class CoalesceGoal extends Serializable {
  val maxSize: Long
  val targetSize: Long

  def whenMaxExceeded(actualSize: Long): Unit = {}
}

object RequireSingleBatch extends CoalesceGoal {
  override val maxSize: Long = Integer.MAX_VALUE
  override val targetSize: Long = Integer.MAX_VALUE

  override def whenMaxExceeded(actualSize: Long): Unit = {
    throw new IllegalStateException("A single batch is required for this operation." +
      " Please try increasing your partition count.")
  }
}

object PreferSingleBatch extends CoalesceGoal {
  override val maxSize: Long = Integer.MAX_VALUE
  override val targetSize: Long = Integer.MAX_VALUE
}

case class TargetSize(override val targetSize: Long) extends CoalesceGoal {
  assert(targetSize <= Integer.MAX_VALUE)

  override val maxSize: Long = {
    val tmp = targetSize * 1.1
    if (tmp > Integer.MAX_VALUE) {
      Integer.MAX_VALUE
    } else {
      tmp.asInstanceOf[Long]
    }
  }
}

case class CoalesceIterator(iter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
    numInputRows: SQLMetric,
    numInputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    collectTime: SQLMetric,
    concatTime: SQLMetric) extends Iterator[ColumnarBatch] {
  private var onDeck: Option[ColumnarBatch] = None
  private var collectStart: Long = -1

  TaskContext.get().addTaskCompletionListener[Unit](_ => onDeck.foreach(_.close()))

  override def hasNext: Boolean = {
    while (onDeck.isEmpty && iter.hasNext) {
      if (collectStart < 0) {
        collectStart = System.nanoTime()
      }
      val cb = iter.next()
      if (numInputBatches != null) {
        numInputBatches += 1
      }
      val rows = cb.numRows()
      if (rows > 0) {
        onDeck = Some(cb)
        if (numInputRows != null) {
          numInputRows += rows
        }
      } else {
        cb.close()
      }
    }
    onDeck.isDefined
  }

  override def next(): ColumnarBatch = {
    val buffer = ArrayBuffer[ColumnarBatch]()
    var numRows: Long = 0 // to avoid overflows
    if (onDeck.isDefined) {
      val cb = onDeck.get
      val rows = cb.numRows()
      if (rows > goal.maxSize) {
        goal.whenMaxExceeded(rows)
      }
      buffer += cb
      onDeck = None
      numRows += rows
    }

    while (numRows < goal.targetSize && onDeck.isEmpty && iter.hasNext) {
      if (collectStart < 0) {
        collectStart = System.nanoTime()
      }
      val cb = iter.next()
      val nextRows = cb.numRows()
      if (numInputBatches != null) {
        numInputBatches += 1
        numInputRows += nextRows
      }
      val wouldBeRows = nextRows + numRows
      if (wouldBeRows > goal.maxSize) {
        goal.whenMaxExceeded(wouldBeRows)
        onDeck = Some(cb)
      } else {
        buffer += cb
        numRows = wouldBeRows
      }
    }
    if (numOutputRows != null) {
      numOutputRows += numRows
      numOutputBatches += 1
    }
    val collectEnd = System.nanoTime()
    val arr = buffer.toArray
    val ret = ConcatAndConsumeAll.buildBatchNoEmpty(arr)
    val end = System.nanoTime()
    if (collectTime != null) {
      collectTime += TimeUnit.NANOSECONDS.toMillis(collectEnd - collectStart)
      concatTime += TimeUnit.NANOSECONDS.toMillis(end - collectEnd)
    }
    collectStart = -1
    ret
  }
}

case class GpuCoalesceBatches(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode with GpuExec {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of Input batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "how long taken to collect batches"),
    "concatTime" -> SQLMetrics.createTimingMetric(sparkContext, "how long taken to concat batches")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def output: Seq[Attribute] = child.output

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")

    val batches = child.executeColumnar()
    batches.mapPartitions(iter =>
      CoalesceIterator(iter, goal,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime, concatTime))
  }
}
