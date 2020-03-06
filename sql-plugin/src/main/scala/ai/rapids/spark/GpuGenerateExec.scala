/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, NvtxColor, Table}
import ai.rapids.spark.GpuMetricNames.{NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, TOTAL_TIME}
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, CreateArray, Expression, PosExplode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{GenerateExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuGenerateExecSparkPlanMeta(
    gen: GenerateExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: ConfKeysAndIncompat) extends SparkPlanMeta[GenerateExec](gen, conf, p, r) {

  override val childExprs: Seq[ExprMeta[_]] = gen.generator match {
    case PosExplode(CreateArray(exprs, _)) =>
      // This bypasses the check to see if we can support an array or not.
      // and the posexplode which is going to be built into this one...
      exprs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
    case g => Seq(GpuOverrides.wrapExpr[Expression](g, conf, Some(this)))
  }

  override def tagPlanForGpu(): Unit = {
    // We can only run on the GPU if we are doing a posexplode of an array we are generating
    // right now
    gen.generator match {
      case PosExplode(CreateArray(_, _)) => //Nothing
      case _ => willNotWorkOnGpu("Only posexplode of a created array is currently supported")
    }

    if (gen.outer) {
      willNotWorkOnGpu("outer is not currently supported")
    }

    if (!gen.requiredChildOutput.isEmpty) {
      willNotWorkOnGpu("Cannot support other output besides an exploded array")
    }
  }

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExec =
    GpuGenerateExec(childExprs.map(_.convertToGpu()),
      gen.generatorOutput,
      childPlans(0).convertIfNeeded())
}

object GpuGenerateExec {
  def createPositionsColumn(numColumns: Integer, numRows: Integer): ColumnVector = {
    val pos = new Array[ColumnVector](numColumns)
    try {
      (0 until numColumns).foreach(i => {
        val scalar = GpuScalar.from(i, IntegerType)
        pos(i) = try {
          ColumnVector.fromScalar(scalar, numRows)
        } finally {
          scalar.close()
        }
      })
      ColumnVector.concatenate(pos: _*)
    } finally {
      pos.safeClose()
    }
  }

  def concatColumnsAndClose(numColumns: Integer, table: Table): ColumnVector = {
    val col = new Array[ColumnVector](numColumns)
    try {
      try {
        (0 until numColumns).foreach(i => col(i) = table.getColumn(i).incRefCount())
      } finally {
        table.close()
      }
      ColumnVector.concatenate(col: _*)
    } finally {
      col.safeClose()
    }
  }
}

/**
 * Takes the place of GenerateExec(PosExplode(CreateArray(_))).  It would be great to do it in a
 * more general case but because we don't support arrays/maps, we have to hard code the cases
 * where we don't actually need to put the data into an array first.
 */
case class GpuGenerateExec(
    arrayProject: Seq[GpuExpression],
    generatorOutput: Seq[Attribute],
    child: SparkPlan
) extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "buildArrayTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "build array time"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val projectTime = longMetric("buildArrayTime")
    val boundProjectList = GpuBindReferences.bindReferences(arrayProject, child.output)
    val numColumns = boundProjectList.length
    child.executeColumnar().map { cb =>
      val nvtxRange = new NvtxWithMetrics("GpuGenerateExec", NvtxColor.PURPLE, totalTime)
      try {
        // First we do a project to get the values the array would have had in it.
        val projOut = GpuProjectExec.projectAndClose(cb, boundProjectList, projectTime)
        val table = try {
          GpuColumnVector.from(projOut)
        } finally {
          projOut.close()
        }

        val numRows = table.getRowCount.toInt

        var c: ColumnVector = null
        var p: ColumnVector = null
        try {
          c = GpuGenerateExec.concatColumnsAndClose(numColumns, table)
          p = GpuGenerateExec.createPositionsColumn(numColumns, numRows)

          val result = new Table(p, c)
          try {
            numOutputBatches += 1
            numOutputRows += result.getRowCount
            GpuColumnVector.from(result)
          } finally {
            result.close()
          }
        } finally {
          if (c != null) {
            c.close()
          }
          if (p != null) {
            p.close()
          }
        }
      } finally {
        nvtxRange.close()
      }
    }
  }
}
