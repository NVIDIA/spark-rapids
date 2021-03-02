/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.NvtxColor

import com.nvidia.spark.rapids.GpuGenerateExecSparkPlanMeta.isOuterSupported
import com.nvidia.spark.rapids.GpuMetric.{ESSENTIAL_LEVEL, MODERATE_LEVEL, NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, TOTAL_TIME}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Generator}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{GenerateExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuGenerateExecSparkPlanMeta(
    gen: GenerateExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[GenerateExec](gen, conf, p, r) {

  override val childExprs: Seq[BaseExprMeta[_]] = {
    (Seq(gen.generator) ++ gen.requiredChildOutput).map(
      GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override def tagPlanForGpu(): Unit = {
    if (gen.outer && !isOuterSupported(gen.generator)) {
      willNotWorkOnGpu(s"outer is not currently supported with ${gen.generator.nodeName}")
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuGenerateExec(
      childExprs.head.convertToGpu().asInstanceOf[GpuGenerator],
      gen.requiredChildOutput,
      gen.outer,
      gen.generatorOutput,
      childPlans.head.convertIfNeeded())
  }
}

object GpuGenerateExecSparkPlanMeta {
  private val OUTER_SUPPORTED_GENERATORS: Set[String] = Set()

  def isOuterSupported(gen: Generator): Boolean = {
    OUTER_SUPPORTED_GENERATORS.contains(gen.nodeName)
  }
}

/**
 * GPU overrides of [[org.apache.spark.sql.catalyst.expressions.Generator]], corporate with
 * `GpuGenerateExec`.
 */
trait GpuGenerator extends GpuUnevaluable {

  override def dataType: DataType = ArrayType(elementSchema)

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  /**
   * The output element schema.
   */
  def elementSchema: StructType

  /**
   * Apply generator to produce result ColumnarBatch from input batch.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec who owns
   * the generator. The reason of creating a new method rather than implementing columnarEval is
   * that generator is an integrated Table transformer instead of column transformer in terms of
   * cuDF.
   *
   * @param inputBatch projected input data, which ensures appending columns are ahead of
   *                   generators' inputs. So, generators can distinguish them with an offset.
   * @param generatorOffset column offset of generator's input columns in `inputBatch`
   * @param outer when true, each input row will be output at least once, even if the output of the
   *              given `generator` is empty.
   *
   * @return result ColumnarBatch
   */
  def generate(inputBatch: ColumnarBatch,
    generatorOffset: Int,
    outer: Boolean): ColumnarBatch
}

trait GpuCollectionGenerator extends GpuGenerator {
  /** The position of an element within the collection should also be returned. */
  def position: Boolean

  /** Rows will be inlined during generation. */
  def inline: Boolean

  /** The type of the returned collection object. */
  def collectionType: DataType = dataType
}

abstract class GpuExplodeBase extends GpuUnevaluableUnaryExpression with GpuCollectionGenerator {
  override val inline: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case _: ArrayType | _: MapType =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        "input to function explode should be array or map type, " +
          s"not ${child.dataType.catalogString}")
  }

  // hive-compatible default alias for explode function ("col" for array, "key", "value" for map)
  override def elementSchema: StructType = child.dataType match {
    case ArrayType(et, containsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType, nullable = false)
          .add("col", et, containsNull)
      } else {
        new StructType()
          .add("col", et, containsNull)
      }
    case MapType(kt, vt, valueContainsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType, nullable = false)
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
      } else {
        new StructType()
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
      }
  }

  override def collectionType: DataType = child.dataType
}

case class GpuExplode(child: Expression) extends GpuExplodeBase {
  override val position: Boolean = false

  override def generate(inputBatch: ColumnarBatch,
    generatorOffset: Int,
    outer: Boolean): ColumnarBatch = {

    require(inputBatch.numCols() - 1 == generatorOffset,
      "GpuExplode should has and only has one input attribute")
    val schema = GpuColumnVector.extractTypes(inputBatch).zipWithIndex.map {
      // extract output type of explode from input ArrayData
      case (dataType, index) if index == generatorOffset =>
        require(dataType.isInstanceOf[ArrayType], "GpuExplode only supports ArrayData now")
        dataType.asInstanceOf[ArrayType].elementType
      // map types of other required columns
      case (dataType, _) =>
        dataType
    }
    withResource(GpuColumnVector.from(inputBatch)) { table =>
      withResource(table.explode(generatorOffset)) { exploded =>
        GpuColumnVector.from(exploded, schema)
      }
    }
  }
}

case class GpuGenerateExec(
    generator: GpuGenerator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override protected val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val generatorProjectList: Seq[GpuExpression] =
      GpuBindReferences.bindGpuReferences(generator.children, child.output)
    val othersProjectList: Seq[GpuExpression] =
      GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output)

    child.executeColumnar().map { inputFromChild =>
      withResource(inputFromChild) { input =>
        withResource(new NvtxWithMetrics("GpuGenerateExec", NvtxColor.PURPLE, totalTime)) { _ =>
          // Project input columns, setting other columns ahead of generator's input columns.
          // With the projected batches and an offset, generators can extract input columns or
          // other required columns separately.
          val projectedInput = GpuProjectExec.project(
            input, othersProjectList ++ generatorProjectList)
          val result = withResource(projectedInput) { generatorInput =>
            generator.generate(generatorInput, othersProjectList.length, outer)
          }
          numOutputBatches += 1
          numOutputRows += result.numRows()
          result
        }
      }
    }
  }
}
