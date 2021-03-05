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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, HostColumnVector, NvtxColor}
import com.nvidia.spark.rapids.GpuGenerateExecSparkPlanMeta.isOuterSupported
import com.nvidia.spark.rapids.GpuMetric.{ESSENTIAL_LEVEL, MODERATE_LEVEL, NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, TOTAL_TIME}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Generator}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{GenerateExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.rapids.GpuCreateArray
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
 * GPU overrides of `Generator`, corporate with `GpuGenerateExec`.
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

  /**
   * Determine split number for generator's input batches.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec to split up
   * input batches to reduce total memory cost during generating. It is necessary because most of
   * generators may produce multiple records for each input record, which make output batch size
   * much larger than input size.
   *
   * @param generatorInput input batch, only containing columns for generation
   * @param outer when true, each input row will be output at least once, even if the output of the
   *              given `generator` is empty.
   *
   * @return recommended number of splits for given input batch
   */
  def inputSplitNum(generatorInput: ColumnarBatch, outer: Boolean): Int = 1
}

abstract class GpuExplodeBase extends GpuUnevaluableUnaryExpression with GpuGenerator {

  /** The position of an element within the collection should also be returned. */
  def position: Boolean

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

  // In terms of GpuExplodeBase, split number should be array size of each row. It is because
  // we would like to keep size of output batches close to input ones in case of OOM caused by
  // expanded output.
  override def inputSplitNum(genIn: ColumnarBatch, outer: Boolean): Int = {
    // only split up input batch if it is large enough
    if (genIn.numRows() < SPLIT_BATCH_THRESHOLD) return 1

    child match {
      // array size of CreateArray is fixed
      case createArray: GpuCreateArray =>
        createArray.children.length
      // estimate array size via calculating average size of sample data
      case _ =>
        val getListSize = if (!outer) {
          (cv: HostColumnVector, i: Int) => if (cv.isNull(i)) 0 else cv.getList(i).size()
        } else {
          (cv: HostColumnVector, i: Int) => if (cv.isNull(i)) 1 else cv.getList(i).size() min 1
        }
        val sampleSize = ESTIMATE_SAMPLE_SIZE
        val colToExplode = genIn.column(0).asInstanceOf[GpuColumnVector]
        val sum = withResource(colToExplode.getBase.subVector(0, sampleSize)) { sampleVec =>
          withResource(sampleVec.copyToHost()) { hostSampleVec =>
            (0 until sampleSize).fold(0) { case (sumBuf, i) =>
              sumBuf + getListSize(hostSampleVec, i)
            }
          }
        }
        (sum.toDouble / sampleSize).round.toInt
    }
  }

  // Infer result schema of GenerateExec from input schema
  protected def resultSchema(inputSchema: Array[DataType],
    genOffset: Int,
    includePos: Boolean = false): Array[DataType] = {
    val outputSchema = ArrayBuffer[DataType]()
    inputSchema.zipWithIndex.foreach {
      // extract output type of explode from input ArrayData
      case (dataType, index) if index == genOffset =>
        require(dataType.isInstanceOf[ArrayType], "GpuExplode only supports ArrayData now")
        if (includePos) {
          outputSchema += IntegerType
        }
        outputSchema += dataType.asInstanceOf[ArrayType].elementType
      // map types of other required columns
      case (dataType, _) =>
        outputSchema += dataType
    }
    outputSchema.toArray
  }

  val SPLIT_BATCH_THRESHOLD = 1024
  val ESTIMATE_SAMPLE_SIZE = 100
}

case class GpuExplode(child: Expression) extends GpuExplodeBase {

  override def generate(inputBatch: ColumnarBatch,
    generatorOffset: Int,
    outer: Boolean): ColumnarBatch = {

    require(inputBatch.numCols() - 1 == generatorOffset,
      "Internal Error GpuExplode supports one and only one input attribute.")
    val schema = resultSchema(GpuColumnVector.extractTypes(inputBatch), generatorOffset)
    withResource(GpuColumnVector.from(inputBatch)) { table =>
      withResource(table.explode(generatorOffset)) { exploded =>
        GpuColumnVector.from(exploded, schema)
      }
    }
  }

  override val position: Boolean = false
}

case class GpuPosExplode(child: Expression) extends GpuExplodeBase {

  override def generate(inputBatch: ColumnarBatch,
    generatorOffset: Int,
    outer: Boolean): ColumnarBatch = {

    require(inputBatch.numCols() - 1 == generatorOffset,
      "Internal Error GpuPosExplode supports one and only one input attribute.")
    val schema = resultSchema(
      GpuColumnVector.extractTypes(inputBatch), generatorOffset, includePos = true)
    withResource(GpuColumnVector.from(inputBatch)) { table =>
      withResource(table.explodePosition(generatorOffset)) { exploded =>
        GpuColumnVector.from(exploded, schema)
      }
    }
  }

  override def position: Boolean = true
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
    val genProjectList: Seq[GpuExpression] =
      GpuBindReferences.bindGpuReferences(generator.children, child.output)
    val othersProjectList: Seq[GpuExpression] =
      GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output)

    child.executeColumnar().flatMap { inputFromChild =>
      withResource(inputFromChild) { input =>
        withResource(new NvtxWithMetrics("GpuGenerateExec", NvtxColor.PURPLE, totalTime)) { _ =>
          // At first, determine number of input splits through `generator.inputSplitNum`
          val splitNum = withResource(GpuProjectExec.project(input, genProjectList)) { genIn =>
            generator.inputSplitNum(genIn, outer)
          }

          // Project input columns, setting other columns ahead of generator's input columns.
          // With the projected batches and an offset, generators can extract input columns or
          // other required columns separately.
          val projectedInput = GpuProjectExec.project(input, othersProjectList ++ genProjectList)
          withResource(projectedInput) { projIn =>
            makeSplitIterator(projIn, splitNum).map { splitIn =>
              withResource(splitIn) { splitIn =>
                val ret = generator.generate(splitIn, othersProjectList.length, outer)
                numOutputBatches += 1
                numOutputRows += ret.numRows()
                ret
              }
            }
          }
        }
      }
    }
  }

  private def makeSplitIterator(input: ColumnarBatch, splitNum: Int): Iterator[ColumnarBatch] = {
    val schema = GpuColumnVector.extractTypes(input)

    if (splitNum == 1) {
      withResource(GpuColumnVector.from(input)) { table =>
        Array(GpuColumnVector.from(table, schema)).iterator
      }
    } else {
      val splitInput = withResource(GpuColumnVector.from(input)) { table =>
        val increment = math.round(table.getRowCount.toDouble / splitNum).toInt
        val splitIndices = (1 until splitNum).foldLeft(ArrayBuffer[Int]()) { case (buf, i) =>
          buf += i * increment - 1
        }.toArray
        table.contiguousSplit(splitIndices: _*)
      }

      splitInput.zipWithIndex.iterator.map { case (ct, i) =>
        closeOnExcept(splitInput.slice(i + 1, splitInput.length)) { _ =>
          withResource(ct) { ct: ContiguousTable =>
            GpuColumnVector.from(ct.getTable, schema)
          }
        }
      }
    }
  }
}
