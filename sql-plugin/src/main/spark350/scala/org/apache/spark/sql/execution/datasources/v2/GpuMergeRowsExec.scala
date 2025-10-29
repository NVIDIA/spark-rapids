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
import com.nvidia.spark.rapids.shims.{ShimExpression, ShimUnaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Instruction, ROW_ID}
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
    isTargetRowPresent: Expression,
    isSourceRowPresent: Expression,
    matchedInstructions: Seq[Instruction],
    notMatchedInstructions: Seq[Instruction],
    notMatchedBySourceInstructions: Seq[Instruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    val usedExprs = if (checkCardinality) {
      val rowIdAttr = child.output.find(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdAttr.isDefined, "Cannot find row ID attr")
      rowIdAttr.get +: expressions
    } else {
      expressions
    }
    AttributeSet.fromAttributeSets(usedExprs.map(_.references)) -- producedAttributes
  }

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

    val boundTargetRowPresent = GpuBindReferences.bindGpuReference(isTargetRowPresent, child.output)
    val boundSourceRowPresent = GpuBindReferences.bindGpuReference(isSourceRowPresent, child.output)
    val boundMatchedInsts = matchedInstructions.map(GpuInstructionExec.bind(_, child.output))
    val boundNotMatchedInsts = notMatchedInstructions.map(GpuInstructionExec.bind(_, child.output))
    val boundMatchedBySourceInsts = notMatchedBySourceInstructions
      .map(GpuInstructionExec.bind(_, child.output))


    child.executeColumnar().mapPartitions { iter =>
      new GpuMergeBatchIterator(
        dataTypes,
        iter,
        boundTargetRowPresent,
        boundSourceRowPresent,
        boundMatchedInsts,
        boundNotMatchedInsts,
        boundMatchedBySourceInsts,
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
 */
case class GpuInstructionExec(
    cpu: Instruction,
    condition: GpuExpression,
    outputs: Seq[Seq[GpuExpression]]) extends GpuUnevaluable with ShimExpression {
  
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

  override def nullable: Boolean = cpu.nullable

  override def dataType: DataType = cpu.dataType

  override def children: Seq[Expression] = Seq(condition) ++ outputs.flatten
}

object GpuInstructionExec {
  def bind(instruction: Instruction, inputs: Seq[Attribute]): GpuInstructionExec = {
    val gpuCond = GpuBindReferences.bindGpuReference(instruction.condition, inputs)
    val gpuOutputs = instruction.outputs
      .map(output => output.map(GpuBindReferences.bindGpuReference(_, inputs)))

    new GpuInstructionExec(instruction, gpuCond, gpuOutputs)
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
 * @param numOutputRows Metric for output rows
 * @param numOutputBatches Metric for output batches
 * @param opTime Metric for operation time
 */
class GpuMergeBatchIterator(
    inputDataTypes: Array[DataType],
    inputIter: Iterator[ColumnarBatch],
    isTargetRowPresent: GpuExpression,
    isSourceRowPresent: GpuExpression,
    matchedInstructionExecs: Seq[GpuInstructionExec],
    notMatchedInstructionExecs: Seq[GpuInstructionExec],
    notMatchedBySourceInstructionExecs: Seq[GpuInstructionExec],
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {

  override def hasNext: Boolean = inputIter.hasNext

  override def next(): ColumnarBatch = {
    val batch = inputIter.next()
    withResource(new NvtxWithMetrics("GpuMergeBatchIterator", NvtxColor.CYAN, opTime)) { _ =>
      val result = processBatch(batch)
      numOutputBatches += 1
      numOutputRows += result.numRows()
      result
    }
  }

  /**
   * Process a single columnar batch, applying merge logic.
   */
  private def processBatch(batch: ColumnarBatch): ColumnarBatch = {

    withRetryNoSplit(batch) { _ =>
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

            val sourceNotMatchedMask = withResource(targetPresent.getBase.not()) { noTargetMask =>
              sourcePresent.getBase.and(noTargetMask)
            }
            withResource(sourceNotMatchedMask) { mask =>
              processInstructionSet(inputDataTypes, outputs, batch, mask,
                notMatchedInstructionExecs)
            }

            val targetNotMatchedMask = withResource(sourcePresent.getBase.not()) { noSourceMask =>
              targetPresent.getBase.and(noSourceMask)
            }
            withResource(targetNotMatchedMask) { mask =>
              processInstructionSet(inputDataTypes, outputs, batch, mask,
                notMatchedBySourceInstructionExecs)
            }
          }
        }
      }
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

    // For each instruction, check if any rows match and apply outputs
    for (instructionExec <- instructionExecs) {
      val condMask = withResource(instructionExec.evaluateCondition(batch)) { cond =>
        cond.getBase.and(mask)
      }
      withResource(condMask) { _ =>
        val thisFilteredBatch = GpuColumnVector.filter(batch, dataTypes, condMask)
        withResource(thisFilteredBatch) { _ =>
          outputs ++= instructionExec.applyOutputs(thisFilteredBatch)
        }
      }
    }
  }
}

