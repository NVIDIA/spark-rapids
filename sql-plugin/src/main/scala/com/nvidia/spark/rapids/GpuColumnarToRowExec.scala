/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.GpuMetricNames._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class ColumnarToRowIterator(batches: Iterator[ColumnarBatch], numInputBatches: SQLMetric = null,
   numOutputRows: SQLMetric = null, totalTime: SQLMetric = null) extends Iterator[InternalRow] {
  // GPU batches read in must be closed by the receiver (us)
  @transient var cb: ColumnarBatch = null
  var it: java.util.Iterator[InternalRow] = null

  TaskContext.get().addTaskCompletionListener[Unit](_ => closeCurrentBatch())

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }

  def loadNextBatch(): Unit = {
    closeCurrentBatch()
    if (it != null) {
      it = null
    }
    if (batches.hasNext) {
      val devCb = batches.next()
      val nvtxRange = if (totalTime != null) {
        new NvtxWithMetrics("ColumnarToRow: batch", NvtxColor.RED, totalTime)
      } else {
        new NvtxRange("ColumnarToRow: batch", NvtxColor.RED)
      }

      try {
        cb = new ColumnarBatch(GpuColumnVector.extractColumns(devCb).map(_.copyToHost()),
          devCb.numRows())
        it = cb.rowIterator()
        if (numInputBatches != null) {
          numInputBatches += 1
        }
        // In order to match the numOutputRows metric in the generated code we update
        // numOutputRows for each batch. This is less accurate than doing it at output
        // because it will over count the number of rows output in the case of a limit,
        // but it is more efficient.
        if (numOutputRows != null) {
          numOutputRows += cb.numRows()
        }
      } finally {
        devCb.close()
        // Leaving the GPU for a while
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        nvtxRange.close()
      }
    }
  }

  override def hasNext: Boolean = {
    val itHasNext = it != null && it.hasNext
    if (!itHasNext) {
      loadNextBatch()
      it != null && it.hasNext
    } else {
      itHasNext
    }
  }

  override def next(): InternalRow = {
    if (it == null || !it.hasNext) {
      loadNextBatch()
    }
    if (it == null) {
      throw new NoSuchElementException()
    }
    it.next()
  }

  // This is to convert the InternalRow to an UnsafeRow. Even though the type is
  // InternalRow some operations downstream operations like collect require it to
  // be UnsafeRow
}

abstract class GpuColumnarToRowExecParent(child: SparkPlan, val exportColumnarRdd: Boolean)
    extends UnaryExecNode with CodegenSupport with GpuExec {
  // We need to do this so the assertions don't fail
  override def supportsColumnar = false

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // `GpuColumnarToRowExec` processes the input RDD directly, which is kind of a leaf node in the
  // codegen stage and needs to do the limit check.
  protected override def canCheckLimitNotReached: Boolean = true

  override def supportCodegen: Boolean = !exportColumnarRdd

  // Override the original metrics to remove NUM_OUTPUT_BATCHES, which makes no sense.
  override lazy val metrics: Map[String, SQLMetric] = Map(
    NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_ROWS),
    TOTAL_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext, DESCRIPTION_TOTAL_TIME),
    NUM_INPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_BATCHES))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(child.executeColumnar().asInstanceOf[RDD[InternalRow]]) // Hack because of type erasure
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numInputBatches = longMetric(NUM_INPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)

    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output

    val f = (batches: Iterator[ColumnarBatch]) => {
      // UnsafeProjection is not serializable so do it on the executor side
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      new ColumnarToRowIterator(batches, numInputBatches, numOutputRows, totalTime).map(toUnsafe)
    }

    val cdata = child.executeColumnar()
    if (exportColumnarRdd) {
      // If we are exporting columnar rdd we need an easy way for the code that walks the
      // RDDs to know where the columnar to row transition is happening.
      GpuColumnToRowMapPartitionsRDD.mapPartitions(cdata, f)
    } else {
      cdata.mapPartitions(f)
    }
  }

  /**
   * Generate [[ColumnVector]] expressions for our parent to consume as rows.
   * This is called once per [[ColumnVector]] in the batch.
   */
  private def genCodeColumnVector(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val value = CodeGenerator.getValueFromVector(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) {
      JavaCode.isNullVariable(ctx.freshName("isNull"))
    } else {
      FalseLiteral
    }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = code"${ctx.registerComment(str)}" + (if (nullable) {
      code"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${CodeGenerator.defaultValue(dataType)} : ($value);
      """
    } else {
      code"$javaType $valueVar = $value;"
    })
    ExprCode(code, isNullVar, JavaCode.variable(valueVar, dataType))
  }

  /**
   * Produce code to process the input iterator as `ColumnarBatch`es.
   * This produces an `org.apache.spark.sql.catalyst.expressions.UnsafeRow` for each row in
   * each batch..
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    // PhysicalRDD always just has one input
    val input = ctx.addMutableState("scala.collection.Iterator", "input",
      v => s"$v = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, NUM_OUTPUT_ROWS)
    val totalTime = metricTerm(ctx, TOTAL_TIME)
    val numInputBatches = metricTerm(ctx, NUM_INPUT_BATCHES)

    val columnarBatchClz = classOf[ColumnarBatch].getName
    val initTCListener = ctx.freshName("initTCListener")
    val devBatch = ctx.freshName("devBatch")
    val hostBatch = ctx.addMutableState(columnarBatchClz, "hostBatch",
    v => {
      val initTCListenerFuncName = ctx.addNewFunction(initTCListener,
        s"""
           | private void $initTCListener() {
           |   org.apache.spark.TaskContext.get().addTaskCompletionListener(
           |      new org.apache.spark.util.TaskCompletionListener() {
           |        @Override
           |        public void onTaskCompletion(TaskContext context) {
           |          if ($v != null) {
           |            $v.close(); }
           |        }
           |    });
           | }
           """.stripMargin.trim)
      s"$initTCListenerFuncName();" }, forceInline = true)

    val idx = ctx.addMutableState(CodeGenerator.JAVA_INT, "batchIdx") // init as batchIdx = 0
    val columnVectorClzs = child.vectorTypes.getOrElse(
      Seq.fill(output.indices.size)(classOf[GpuColumnVector].getName))
    val hostClz = classOf[RapidsHostColumnVector].getName
    val (colVars, columnAssigns) = columnVectorClzs.zipWithIndex.map {
      case (columnVectorClz, i) =>
        val hostName = ctx.addMutableState(hostClz, s"hostColInstance$i")
        (hostName, s"$hostName = (($columnVectorClz) $devBatch.column($i)).copyToHost();")
    }.unzip

    val batchArray = ctx.freshName("batchArray")
    val convertStart = ctx.freshName("convertStart")
    val convertRange = ctx.freshName("convertRange")
    val nextBatch = ctx.freshName("nextBatch")
    // scalastyle:off line.size.limit
    val nextBatchFuncName = ctx.addNewFunction(nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  if ($input.hasNext()) {
         |    $columnarBatchClz $devBatch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($devBatch.numRows());
         |    $numInputBatches.add(1);
         |    $idx = 0;
         |    long $convertStart = System.nanoTime();
         |    ai.rapids.cudf.NvtxRange $convertRange = new ai.rapids.cudf.NvtxRange("ColumnarToRow: convert", ai.rapids.cudf.NvtxColor.CYAN);
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |    $convertRange.close();
         |    $totalTime.add(System.nanoTime() - $convertStart);
         |    org.apache.spark.sql.vectorized.ColumnVector[] $batchArray = new org.apache.spark.sql.vectorized.ColumnVector[] {${colVars.mkString("", ", ", "")}};
         |    $hostBatch = new org.apache.spark.sql.vectorized.ColumnarBatch($batchArray, $devBatch.numRows());
         |    $devBatch.close();
         |    com.nvidia.spark.rapids.GpuSemaphore$$.MODULE$$.releaseIfNecessary(org.apache.spark.TaskContext.get());
         |  }
         |}""".stripMargin)
    // scalastyle:on line.size.limit

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
      genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val shouldStop = if (parent.needStopCheck) {
      s"if (shouldStop()) { $idx = $rowidx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }
    s"""
       |if ($hostBatch == null) {
       |  $nextBatchFuncName();
       |}
       |while ($limitNotReachedCond $hostBatch != null) {
       |  int $numRows = $hostBatch.numRows();
       |  int $localEnd = $numRows - $idx;
       |  for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
       |    int $rowidx = $idx + $localIdx;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    $shouldStop
       |  }
       |  $idx = $numRows;
       |  $hostBatch.close();
       |  $hostBatch = null;
       |  $nextBatchFuncName();
       |}
     """.stripMargin
  }
}

object GpuColumnarToRowExecParent {
  def unapply(arg: GpuColumnarToRowExecParent): Option[(SparkPlan, Boolean)] = {
    Option(Tuple2(arg.child, arg.exportColumnarRdd))
  }
}

case class GpuColumnarToRowExec(child: SparkPlan, override val exportColumnarRdd: Boolean = false)
   extends GpuColumnarToRowExecParent(child, exportColumnarRdd)
