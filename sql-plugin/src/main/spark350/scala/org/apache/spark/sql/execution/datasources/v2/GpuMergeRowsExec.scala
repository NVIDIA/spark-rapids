/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer
import ai.rapids.cudf.{ColumnView, NvtxColor}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * GPU version of MergeRowsExec.
 * 
 * This operator processes the output of a join between source and target tables,
 * applying merge logic to determine which rows to insert, update, or delete.
 * 
 *
 * @param isSourceRowPresent GPU expression indicating if source row is present in join
 * @param isTargetRowPresent GPU expression indicating if target row is present in join
 * @param matchedInstructions Sequence of instructions for MATCHED clauses
 * @param notMatchedInstructions Sequence of instructions for NOT MATCHED clauses
 * @param notMatchedBySourceInstructions Sequence of instructions for NOT MATCHED BY SOURCE clauses
 * @param checkCardinality Whether to check for cardinality violations (multiple matches)
 * @param output Output attributes
 * @param child Child plan providing joined data
 */
case class GpuMergeRowsExec(
    isSourceRowPresent: GpuExpression,
    isTargetRowPresent: GpuExpression,
    matchedInstructions: Seq[GpuInstructionExec],
    notMatchedInstructions: Seq[GpuInstructionExec],
    notMatchedBySourceInstructions: Seq[GpuInstructionExec],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES)
  )

  override def supportsColumnar: Boolean = true

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)

    val dataTypes = GpuColumnVector.extractTypes(child.schema)

    child.executeColumnar().mapPartitions { iter =>
      new GpuMergeBatchIterator(
        dataTypes,
        iter,
        isSourceRowPresent,
        isTargetRowPresent,
        matchedInstructions,
        notMatchedInstructions,
        notMatchedBySourceInstructions,
        output,
        numOutputRows,
        numOutputBatches,
        opTime)
    }
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
}

/**
 * GPU version of InstructionExec. Evaluates merge instruction conditions and applies outputs.
 * 
 * Similar to Spark's InstructionExec which is a private class in MergeRowsExec.
 *
 * @param condition GPU expression for the instruction condition
 * @param outputs GPU expressions for the instruction outputs
 * @param childOutput Output schema from child plan for binding expressions
 */
class GpuInstructionExec(
    condition: GpuExpression,
    outputs: Seq[Seq[GpuExpression]],
    childOutput: Seq[Attribute]) {
  
  /**
   * Evaluate the condition on the input batch.
   * @param batch Input columnar batch
   * @return Column vector containing boolean mask where condition is true
   */
  def evaluateCondition(batch: ColumnarBatch): GpuColumnVector = {
    condition.columnarEval(batch)
  }

  /**
   * Apply the instruction's outputs to the input batch.
   * @param batch Input columnar batch
   * @return Array of column vectors representing the projected outputs
   */
  def applyOutputs(batch: ColumnarBatch): Seq[SpillableColumnarBatch] = {
    outputs.safeMap(output => SpillableColumnarBatch.apply(GpuProjectExec.project(batch, output),
      ACTIVE_ON_DECK_PRIORITY))
  }
}

/**
 * GPU version of MergeRowIterator. Processes columnar batches for MERGE operations.
 * Similar to Spark's MergeRowIterator but operates on batches instead of rows.
 *
 * @param inputIter Iterator of input columnar batches
 * @param isSourceRowPresent Bound GPU expression to check if source row is present
 * @param isTargetRowPresent Bound GPU expression to check if target row is present
 * @param matchedInstructionExecs Executors for MATCHED instructions
 * @param notMatchedInstructionExecs Executors for NOT MATCHED instructions
 * @param notMatchedBySourceInstructionExecs Executors for NOT MATCHED BY SOURCE instructions
 * @param outputAttrs Output schema
 * @param numOutputRows Metric for output rows
 * @param numOutputBatches Metric for output batches
 * @param opTime Metric for operation time
 */
class GpuMergeBatchIterator(
    inputDataTypes: Array[DataType],
    inputIter: Iterator[ColumnarBatch],
    isSourceRowPresent: GpuExpression,
    isTargetRowPresent: GpuExpression,
    matchedInstructionExecs: Seq[GpuInstructionExec],
    notMatchedInstructionExecs: Seq[GpuInstructionExec],
    notMatchedBySourceInstructionExecs: Seq[GpuInstructionExec],
    outputAttrs: Seq[Attribute],
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {

  override def hasNext: Boolean = inputIter.hasNext

  override def next(): ColumnarBatch = {
      withResource(inputIter.next()) { batch =>
        withResource(new NvtxWithMetrics("GpuMergeBatchIterator", NvtxColor.CYAN, opTime)) { _ =>
        val result = processBatch(batch)
        numOutputBatches += 1
        numOutputRows += result.numRows()
        result
      }
    }
  }

  /**
   * Process a single columnar batch, applying merge logic.
   */
  private def processBatch(batch: ColumnarBatch): ColumnarBatch = {

    val smallBatches = withRetryNoSplit(batch) { _ =>
      // Evaluate presence flags
      val sourcePresentCol = isSourceRowPresent.columnarEval(batch)
      val targetPresentCol = isTargetRowPresent.columnarEval(batch)

      val outputs = new ArrayBuffer[SpillableColumnarBatch](
        matchedInstructionExecs.size + notMatchedInstructionExecs.size
          + notMatchedBySourceInstructionExecs.size)

      closeOnExcept(outputs) { _ =>
        withResource(sourcePresentCol) { sourcePresent =>
          withResource(targetPresentCol) { targetPresent =>

            withResource(sourcePresent.getBase.and(targetPresent.getBase)) { mask =>
              processInstructionSet(inputDataTypes, outputs, batch, mask, matchedInstructionExecs)
            }

            processInstructionSet(inputDataTypes, outputs, batch, sourcePresent.getBase,
              notMatchedInstructionExecs)

            processInstructionSet(inputDataTypes, outputs, batch, targetPresent.getBase,
              notMatchedBySourceInstructionExecs)
          }
        }
      }

      outputs
    }

    withRetryNoSplit(smallBatches) { outputs =>

      // TODO: There is a small chance that the output batch is much larger input batch,
      //  e.g. all cases need to update split, which leads to a result batch whose row
      //  count is twice of input batch. We need to handle this case by splitting the outputs
      withResource(GpuBatchUtils.concatSpillBatchesAndClose(outputs)) { spillable =>
        spillable.get.getColumnarBatch()
      }
    }
  }

  /**
   * Process a set of instructions for rows matching the given mask.
   */
  private def processInstructionSet(
     dataTypes: Array[DataType],
     outputs: ArrayBuffer[SpillableColumnarBatch],
     batch: ColumnarBatch,
     mask: ColumnView,
     instructionExecs: Seq[GpuInstructionExec]): Unit = {

    if (instructionExecs.isEmpty) return

    withResource(GpuColumnVector.filter(batch, dataTypes, mask)) { filteredBatch =>
      // For each instruction, check if any rows match and apply outputs
      for (instructionExec <- instructionExecs) {
        withResource(instructionExec.evaluateCondition(batch)) { cond =>
          val thisFilteredBatch = GpuColumnVector.filter(filteredBatch, dataTypes, cond.getBase)
          withResource(thisFilteredBatch) { _ =>
            outputs ++= instructionExec.applyOutputs(thisFilteredBatch)
          }
        }
      }
    }
  }
}

