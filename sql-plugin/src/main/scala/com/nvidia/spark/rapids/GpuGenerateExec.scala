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

import ai.rapids.cudf.{ColumnVector, ContiguousTable, NvtxColor, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, CreateArray, Expression, Generator}
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
    if (gen.outer &&
      !childExprs.head.asInstanceOf[GeneratorExprMeta[Generator]].supportOuter) {
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

abstract class GeneratorExprMeta[INPUT <: Generator](
    gen: INPUT,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[INPUT](gen, conf, p, r) {
  /* whether supporting outer generate or not */
  val supportOuter: Boolean = false
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
   * @param inputBatch      projected input data, which ensures appending columns are ahead of
   *                        generators' inputs. So, generators can distinguish them with an offset.
   * @param generatorOffset column offset of generator's input columns in `inputBatch`
   * @param outer           when true, each input row will be output at least once, even if the
   *                        output of the given `generator` is empty.
   * @return result ColumnarBatch
   */
  def generate(inputBatch: ColumnarBatch,
    generatorOffset: Int,
    outer: Boolean): ColumnarBatch

  /**
   * Compute split indices for generator's input batches.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec to split up
   * input batches to reduce total memory cost during generating. It is necessary because most of
   * generators may produce multiple records for each input record, which make output batch size
   * much larger than input size.
   *
   * @param inputBatch      projected input data, which ensures appending columns are ahead of
   *                        generators' inputs. So, generators can distinguish them with an offset.
   * @param generatorOffset column offset of generator's input columns in `inputBatch`
   * @param outer           when true, each input row will be output at least once, even if the
   *                        output of the given `generator` is empty.
   * @param targetSizeBytes the target number of bytes for a GPU batch, one of `RapidsConf`
   * @return split indices of input batch
   */
  def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean,
      targetSizeBytes: Long): Array[Int]

  /**
   * Extract lazy expressions from generator if exists.
   *
   * This is a specialized method for GPU runtime, which is called by GpuGenerateExec to determine
   * whether current generation plan can be executed with optimized lazy array generation or not.
   *
   * @return fixed length lazy expressions for generation. Nil value means no lazy expressions to
   *         extract, which indicates fixed length lazy array generation is unavailable.
   */
  def fixedLenLazyExpressions: Seq[Expression] = Nil

  /**
   * Optimized lazy generation interface which is specialized for fixed length array input.
   *
   * For some generators (like explode), it is possible to improve performance through lazy
   * evaluation when input schema is fixed length array.
   *
   * @param inputIterator          input iterator from child plan
   * @param boundLazyProjectList   lazy expressions bounded with child outputs
   * @param boundOthersProjectList other required expressions bounded with child outputs
   * @param outputSchema           result schema of GpuGenerateExec
   * @param outer                  when true, each input row will be output at least once, even if
   *                               the output of the given `generator` is empty.
   * @param numOutputRows          Gpu spark metric of output rows
   * @param numOutputBatches       Gpu spark metric of output batches
   * @param totalTime              Gpu spark metric of total time costed by GpuGenerateExec
   * @return result iterator
   */
  def fixedLenLazyArrayGenerate(inputIterator: Iterator[ColumnarBatch],
      boundLazyProjectList: Seq[Expression],
      boundOthersProjectList: Seq[Expression],
      outputSchema: Array[DataType],
      outer: Boolean,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    throw new NotImplementedError("The method should be implemented by specific generators.")
  }
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

  override def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean,
      targetSizeBytes: Long): Array[Int] = {

    val vectors = GpuColumnVector.extractBases(inputBatch)
    val inputRows = inputBatch.numRows()
    if (inputRows == 0) return Array()

    // Get the output size in bytes of the column that we are going to explode
    // along with an estimate of how many output rows produced by the explode
    val (explodeColOutputSize, estimatedOutputRows) = withResource(
      vectors(generatorOffset).getChildColumnView(0)) { listValues =>
      val totalSize = listValues.getDeviceMemorySize
      val totalCount = listValues.getRowCount
      (totalSize.toDouble, totalCount.toDouble)
    }
    // input size of columns to be repeated during exploding
    val repeatColsInputSize = vectors.slice(0, generatorOffset).map(_.getDeviceMemorySize).sum
    // estimated output size of repeated columns
    val repeatColsOutputSize = repeatColsInputSize * estimatedOutputRows / inputRows
    // estimated total output size
    val estimatedOutputSizeBytes = explodeColOutputSize + repeatColsOutputSize
    // how may splits will we need to keep the output size under the target size
    val numSplitsForTargetSize = math.ceil(estimatedOutputSizeBytes / targetSizeBytes).toInt
    // how may splits will we need to keep the output rows under max value
    val numSplitsForTargetRow = math.ceil(estimatedOutputRows / Int.MaxValue).toInt
    // how may splits will we need to keep exploding working safely
    val numSplits = numSplitsForTargetSize max numSplitsForTargetRow

    if (numSplits == 0) Array()
    else GpuBatchUtils.generateSplitIndices(inputRows, numSplits)
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

  override def fixedLenLazyExpressions: Seq[Expression] = child match {
    // GpuLiteral of ArrayData will be converted to GpuCreateArray with GpuLiterals
    case GpuCreateArray(expressions, _) => expressions
    case GpuAlias(GpuCreateArray(expressions, _), _) => expressions
    case _ => Nil
  }

  override def fixedLenLazyArrayGenerate(inputIterator: Iterator[ColumnarBatch],
      boundLazyProjectList: Seq[Expression],
      boundOthersProjectList: Seq[Expression],
      outputSchema: Array[DataType],
      outer: Boolean,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {

    val numArrayColumns = boundLazyProjectList.length
    val numOtherColumns = boundOthersProjectList.length
    val numExplodeColumns = if (position) 2 else 1

    new Iterator[ColumnarBatch] {
      // TODO: Perhaps we can make currentBatch spillable in the future
      // https://github.com/NVIDIA/spark-rapids/issues/1940
      var currentBatch: ColumnarBatch = _
      var indexIntoData = 0

      private def closeCurrent(): Unit = if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }

      TaskContext.get().addTaskCompletionListener[Unit](_ => closeCurrent())

      def fetchNextBatch(): Unit = {
        indexIntoData = 0
        closeCurrent()
        if (inputIterator.hasNext) {
          currentBatch = inputIterator.next()
        }
      }

      override def hasNext: Boolean = {
        if (currentBatch == null || indexIntoData >= numArrayColumns) {
          fetchNextBatch()
        }
        currentBatch != null
      }

      override def next(): ColumnarBatch = {
        if (currentBatch == null || indexIntoData >= numArrayColumns) {
          fetchNextBatch()
        }

        withResource(new NvtxWithMetrics("GpuGenerateExec", NvtxColor.PURPLE, totalTime)) { _ =>
          withResource(new Array[ColumnVector](numExplodeColumns + numOtherColumns)) { result =>

            withResource(GpuProjectExec.project(currentBatch, boundOthersProjectList)) { cb =>
              (0 until cb.numCols()).foreach { i =>
                result(i) = cb.column(i).asInstanceOf[GpuColumnVector].getBase.incRefCount()
              }
            }
            if (position) {
              result(numOtherColumns) = withResource(GpuScalar.from(indexIntoData, IntegerType)) {
                scalar => ColumnVector.fromScalar(scalar, currentBatch.numRows())
              }
            }
            result(numOtherColumns + numExplodeColumns - 1) =
                withResource(GpuProjectExec.project(currentBatch,
                  Seq(boundLazyProjectList(indexIntoData)))) { cb =>
                  cb.column(0).asInstanceOf[GpuColumnVector].getBase.incRefCount()
                }

            withResource(new Table(result: _*)) { table =>
              indexIntoData += 1
              numOutputBatches += 1
              numOutputRows += table.getRowCount
              GpuColumnVector.from(table, outputSchema)
            }
          }
        }
      }
    }
  }
}

case class GpuExplode(child: Expression) extends GpuExplodeBase {

  override def generate(inputBatch: ColumnarBatch,
    generatorOffset: Int,
    outer: Boolean): ColumnarBatch = {

    require(inputBatch.numCols() - 1 == generatorOffset,
      "Internal Error GpuExplode supports one and only one input attribute.")
    val schema = resultSchema(GpuColumnVector.extractTypes(inputBatch), generatorOffset)
    val explodeFun = (t: Table) =>
      if (outer) t.explodeOuter(generatorOffset) else t.explode(generatorOffset)
    withResource(GpuColumnVector.from(inputBatch)) { table =>
      withResource(explodeFun(table)) { exploded =>
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
    val explodePosFun = (t: Table) =>
      if (outer) t.explodeOuterPosition(generatorOffset) else t.explodePosition(generatorOffset)
    withResource(GpuColumnVector.from(inputBatch)) { table =>
      withResource(explodePosFun(table)) { exploded =>
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

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME)
  )

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

    generator.fixedLenLazyExpressions match {
      // If lazy expressions can be extracted from generator,
      // perform optimized lazy generation via `generator.fixedLenLazyArrayGenerate`
      case expressions if expressions.nonEmpty =>
        val boundLazyProjectList =
          GpuBindReferences.bindGpuReferences(expressions, child.output).toArray
        val boundOthersProjectList =
          GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output).toArray
        val outputSchema = output.map(_.dataType).toArray

        child.executeColumnar().mapPartitions { iter =>
          generator.fixedLenLazyArrayGenerate(iter,
            boundLazyProjectList,
            boundOthersProjectList,
            outputSchema,
            outer,
            numOutputRows,
            numOutputBatches,
            totalTime)
        }

      // Otherwise, perform common generation via `generator.generate`
      case _ =>
        val genProjectList: Seq[GpuExpression] =
          GpuBindReferences.bindGpuReferences(generator.children, child.output)
        val othersProjectList: Seq[GpuExpression] =
          GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output)

        child.executeColumnar().flatMap { inputFromChild =>
          withResource(inputFromChild) { input =>
            doGenerate(input, genProjectList, othersProjectList,
              numOutputRows, numOutputBatches, totalTime)
          }
        }
    }
  }

  private def doGenerate(input: ColumnarBatch,
      genProjectList: Seq[GpuExpression],
      othersProjectList: Seq[GpuExpression],
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("GpuGenerateExec", NvtxColor.PURPLE, totalTime)) { _ =>
      // Project input columns, setting other columns ahead of generator's input columns.
      // With the projected batches and an offset, generators can extract input columns or
      // other required columns separately.
      val projectedInput = GpuProjectExec.project(input, othersProjectList ++ genProjectList)
      withResource(projectedInput) { projIn =>
        // 1. compute split indices of input batch
        val splitIndices = generator.inputSplitIndices(
          projIn,
          othersProjectList.length,
          outer,
          new RapidsConf(conf).gpuTargetBatchSizeBytes)
        // 2. split up input batch with indices
        makeSplitIterator(projIn, splitIndices).map { splitIn =>
          withResource(splitIn) { splitIn =>
            // 3. apply generation on each (sub)batch
            val ret = generator.generate(splitIn, othersProjectList.length, outer)
            numOutputBatches += 1
            numOutputRows += ret.numRows()
            ret
          }
        }
      }
    }
  }

  private def makeSplitIterator(input: ColumnarBatch,
      splitIndices: Array[Int]): Iterator[ColumnarBatch] = {
    val schema = GpuColumnVector.extractTypes(input)

    if (splitIndices.isEmpty) {
      withResource(GpuColumnVector.from(input)) { table =>
        Array(GpuColumnVector.from(table, schema)).iterator
      }
    } else {
      val splitInput = withResource(GpuColumnVector.from(input)) { table =>
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
