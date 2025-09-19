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

package org.apache.spark.sql.rapids.aggregate

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, GroupByAggregationOnColumn, Scalar}
import com.nvidia.spark.{RapidsAdvancedGroupByAggregation, RapidsSimpleGroupByAggregation, RapidsUDAF, RapidsUDAFGroupByAggregation}
import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, GpuUnsignedIntegerType, GpuUnsignedLongType, GpuUserDefinedFunction, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, UserDefinedExpression}
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.rapids.GpuScalaUDF
import org.apache.spark.sql.types._

/**
 * Co-work with a GpuAdvancedAggregateFunction to customize the aggregate computation.
 */
trait AdvancedCudfAggregate extends Serializable {
  /**
   * Do some optional pre-process before executing the "reduce" or "aggregateXXX".
   * The output will be fed to "reduce" or "aggregateXXX".
   *
   * Now this is only called for the "merge" stage of an aggregate, it plays the
   * role similar as "preMerge" in a GpuAggregateFunction.
   */
  def preStepAndClose(numRows: Int, args: Array[GpuColumnVector]): Array[GpuColumnVector] = args
  // Similar as "reductionAggregate" in the CudfAggregate
  def reduce(numRows: Int, preStepData: Array[GpuColumnVector]): Array[GpuScalar]
  // Similar as "groupByAggregate" in the CudfAggregate
  def aggregate(inputIndices: Array[Int]): Array[GroupByAggregationOnColumn]

  /**
   * If true, "aggregateAdvanced" will be executed instead of "aggregate" to get more
   * control on the aggregate computation. Otherwise, "aggregate" is always called.
   */
  def supportAdvanced: Boolean = false

  /**
   * An advanced version of aggregate giving more control on the aggregate computation
   *  to perform custom aggregation on data that has been grouped by keys.
   * The data is grouped, with offsets indicating group boundaries.
   *
   * @param keyOffsets  A ColumnVector containing the start offset for each group.
   *                    The end offset for group i is `keyOffsets[i+1]` (or total
   *                    rows for the last group).
   * @param groupedData An array of ColumnVectors containing the actual data
   *                    columns, sorted and organized by the grouping keys.
   * @return An array of ColumnVectors with one row per group, containing the
   *         aggregated results.
   */
  def aggregateAdvanced(
      keyOffsets: ColumnVector,
      groupedData: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    throw new UnsupportedOperationException("Children should override this if " +
      "setting 'supportAdvanced' to true")
  }

  /**
   * Do some optional post-process after executing the "reduce" or "aggregateXXX".
   * The output will be return to Spark, so it should match the aggregate buffer schema.
   *
   * It plays the role similar as "postUpdate" or "postMerge" in a GpuAggregateFunction.
   */
  def postStepAndClose(
      numRows: Int,
      aggregatedData: Array[GpuColumnVector]): Array[GpuColumnVector] = aggregatedData
}

/**
 * An aggregation function that supports to customize the aggregate computations for
 * almost all the core stages of the GPU hash aggregate process.
 *
 * This is designed for UDAF support on GPU, but it is not a good idea to put things named
 * "xxxUDAFxxx" directly into the GpuHashAggregateExec.
 */
trait GpuAdvancedAggregateFunction extends GpuAggregateFunction with UserDefinedExpression
    with Serializable {
  // Similar as "initialValues" in the GpuAggregateFunction
  def defaultValues: Array[GpuScalar]
  // Similar as "inputProjection" in the GpuAggregateFunction
  def preProcessAndClose(numRows: Int, args: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    args
  }
  // Similar as "updateAggregates" in the GpuAggregateFunction
  def updateAggregate(): AdvancedCudfAggregate
  // Similar as "mergeAggregates" in the GpuAggregateFunction
  def mergeAggregate(): AdvancedCudfAggregate
  // Similar as "evaluateExpression" in the GpuAggregateFunction
  def postProcess(numRows: Int, args: Array[GpuColumnVector]): GpuColumnVector

  override final lazy val inputProjection: Seq[Expression] = children

  override final lazy val initialValues: Seq[Expression] = {
    throw new UnsupportedOperationException("Gpu advanced aggregate function" +
      " does not support 'initialValues', call 'defaultValues' instead.")
  }
  override final lazy val updateAggregates: Seq[CudfAggregate] = {
    throw new UnsupportedOperationException("Gpu advanced aggregate function" +
      " does not support 'updateAggregates', call 'updateAggregate' instead.")
  }
  override final lazy val mergeAggregates: Seq[CudfAggregate] = {
    throw new UnsupportedOperationException("Gpu advanced aggregate function" +
      " does not support 'mergeAggregates', call 'mergeAggregate' instead.")
  }
  override final lazy val evaluateExpression: Expression = {
    throw new UnsupportedOperationException("Gpu advanced aggregate function" +
      " does not support 'evaluateExpression', call 'postProcess' instead.")
  }
}

/**
 * The wrapper of a RapidsUDAFGroupByAggregation to interact with the GPU hash
 * aggregate process via GPU columns or scalars.
 */
private[aggregate] class UDAFCudfAggregate(
    aggBufferTypes: Array[DataType],
    udafAgg: RapidsUDAFGroupByAggregation) extends AdvancedCudfAggregate {

  // Type of UDAF check is done by initialing this field when constructing an instance.
  override val supportAdvanced: Boolean = udafAgg match {
    case _: RapidsAdvancedGroupByAggregation => true
    case _: RapidsSimpleGroupByAggregation => false
    case u =>
      throw new UnsupportedOperationException(s"${u.getClass} is NOT a child of " +
        "'RapidsSimpleGroupByAggregation' or 'RapidsAdvancedGroupByAggregation'.")
  }

  override def preStepAndClose(
      numRows: Int, args: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    closeOnExcept(udafAgg.preStep(numRows, args.map(_.getBase))) { preCols =>
      val inputTypes = args.map(_.dataType())
      try {
        // try the input types first
        preCols.zip(inputTypes).map { case (cudfCol, dt) =>
          GpuColumnVector.fromChecked(cudfCol, dt)
        }
      } catch {
        case _: IllegalArgumentException =>
          // some transformation is made, so infer the types from the outputs
          preCols.map { cudfCol =>
            GpuColumnVector.fromChecked(cudfCol, AdvAggTypeUtils.infer(cudfCol))
          }
      }
    }
  }

  override def reduce(numRows: Int, preStepData: Array[GpuColumnVector]): Array[GpuScalar] = {
    closeOnExcept(udafAgg.reduce(numRows, preStepData.map(_.getBase))) { reducedRet =>
      reducedRet.safeMap { cuScalar =>
        GpuScalar(cuScalar, AdvAggTypeUtils.infer(cuScalar))
      }
    }
  }

  override def aggregateAdvanced(
      keyOffsets: ColumnVector,
      groupedData: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    val advUdafAgg = udafAgg.asInstanceOf[RapidsAdvancedGroupByAggregation]
    closeOnExcept(advUdafAgg.aggregateGrouped(keyOffsets, groupedData.map(_.getBase))) { ret =>
      ret.map { cudfCol =>
        GpuColumnVector.from(cudfCol, AdvAggTypeUtils.infer(cudfCol))
      }
    }
  }

  override def aggregate(inputIndices: Array[Int]): Array[GroupByAggregationOnColumn] = {
    udafAgg.asInstanceOf[RapidsSimpleGroupByAggregation].aggregate(inputIndices)
  }

  override def postStepAndClose(
      numRows: Int,
      aggregatedData: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    closeOnExcept(udafAgg.postStep(aggregatedData.map(_.getBase))) { postCols =>
      require(postCols.length == aggBufferTypes.length,
        "The sizes of the 'postStep' and 'aggregationBufferTypes' outputs does " +
          s"not match. Sizes: ${postCols.length} vs ${aggBufferTypes.length}")
      try {
        postCols.zip(aggBufferTypes).map { case (cudfCol, dt) =>
          GpuColumnVector.fromChecked(cudfCol, dt)
        }
      } catch {
        case iae: IllegalArgumentException =>
          throw new RuntimeException("The output of the 'postStep' does not match " +
            "the given aggregate buffer types", iae)
      }
    }
  }
}

object AdvAggTypeUtils {
  /**
   * Infer the Spark type from the given cuDF ColumnView.
   *
   * This returned Spark type can not be used to interact with the Spark world, but
   * only for the GPU process internally when asking for a ColumnarBatch without given
   * Spark type. Because it may not always reflect the
   * original Spark type. e.g.
   *   A List of Struct column in cuDF may be either from MapType or the real List
   *   of Struct type in Spark.
   *   A INT32 column in cuDF may be from either YearMonthIntervalType or IntegerType
   *   in Spark.
   *
   *  It is designed for the "preStep" and "reduce/aggregate" operations in our GPU
   *  advanced aggregates.
   */
  def infer(col: ColumnView): DataType = col.getType match {
    case DType.LIST =>
      val childType = withResource(col.getChildColumnView(0))(infer)
      ArrayType(childType, col.getNullCount > 0)
    case DType.STRUCT =>
      val fields = (0 until col.getNumChildren).map { i =>
        withResource(col.getChildColumnView(i)) { chdView =>
          val chdType = infer(chdView)
          StructField(s"_cudf_${chdView.getType}_$i", chdType, chdView.getNullCount > 0)
        }
      }
      StructType(fields)
    case nonNested => fromNonNested(nonNested)
  }

  /**
   * Infer the Spark type from the given cuDF Scalar, similar as infer(ColumnView).
   */
  def infer(scalar: Scalar): DataType = scalar.getType match {
    case DType.LIST =>
      val childType = withResource(scalar.getListAsColumnView)(infer)
      ArrayType(childType, !scalar.isValid)
    case DType.STRUCT =>
      val fields = withResource(scalar.getChildrenFromStructScalar) { childrenViews =>
        childrenViews.zipWithIndex.map { case (chdView, i) =>
          val chdType = infer(chdView)
          StructField(s"_cudf_${chdView.getType}_$i", chdType, chdView.getNullCount > 0)
        }
      }
      StructType(fields)
    case nonNested => fromNonNested(nonNested)
  }

  private def fromNonNested(dType: DType): DataType = dType match {
    case DType.BOOL8 => BooleanType
    case DType.INT8 => ByteType
    case DType.INT16 => ShortType
    case DType.INT32 => IntegerType
    case DType.INT64 => LongType
    case DType.FLOAT32 => FloatType
    case DType.FLOAT64 => DoubleType
    case DType.TIMESTAMP_DAYS => DateType
    case DType.TIMESTAMP_MICROSECONDS => TimestampType
    case DType.STRING => StringType
    case DType.UINT32 => GpuUnsignedIntegerType
    case DType.UINT64 => GpuUnsignedLongType
    case dType if dType.isDecimalType =>
      val precision = dType.getTypeId match {
        case DType.DTypeEnum.DECIMAL32 => 9
        case DType.DTypeEnum.DECIMAL64 => 18
        case DType.DTypeEnum.DECIMAL128 => 38
        case _ => throw new IllegalArgumentException(s"Unsupported decimal type: $dType")
      }
      DecimalType(precision, -dType.getScale)
    case _ => throw new IllegalArgumentException(s"Unsupported DType: $dType")
  }
}

/** Common implementation for all the types of GPU UDAF interface. */
trait GpuUDAFFunctionBase extends GpuAdvancedAggregateFunction
  with UserDefinedExpression {

  /** User's UDAF instance */
  protected def function: RapidsUDAF

  protected lazy val aggBufferTypes: Array[DataType] = function.aggBufferTypes()

  override def defaultValues: Array[GpuScalar] = {
    closeOnExcept(function.getDefaultValue) { udafDefValues =>
      require(udafDefValues.length == aggBufferTypes.length,
        s"The default values number (${udafDefValues.length}) is NOT equal to " +
          s"the aggregation buffers number(${aggBufferTypes.length})")
      udafDefValues.zip(aggBufferTypes).map { case (scalar, dt) =>
        GpuScalar(scalar, dt)
      }
    }
  }

  override def preProcessAndClose(
      numRows: Int, args: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    closeOnExcept(function.preProcess(numRows, args.map(_.getBase))) { preCols =>
      val inputTypes = args.map(_.dataType())
      try {
        // try the input types first
        preCols.zip(inputTypes).map { case (cudfCol, dt) =>
          GpuColumnVector.fromChecked(cudfCol, dt)
        }
      } catch {
        case _: IllegalArgumentException =>
          // some transformation is made, so infer the types from the outputs
          preCols.map { cudfCol =>
            GpuColumnVector.fromChecked(cudfCol, AdvAggTypeUtils.infer(cudfCol))
          }
      }
    }
  }

  override def postProcess(numRows: Int, args: Array[GpuColumnVector]): GpuColumnVector = {
    closeOnExcept(function.postProcess(numRows, args.map(_.getBase))) { postCol =>
      try {
        GpuColumnVector.fromChecked(postCol, dataType)
      } catch {
        case iae: IllegalArgumentException =>
          throw new RuntimeException("The output of the 'postProcess' does not match " +
            "the UDAF result type", iae)
      }
    }
  }

  override def updateAggregate(): AdvancedCudfAggregate = {
    new UDAFCudfAggregate(aggBufferTypes, function.updateAggregation())
  }

  override def mergeAggregate(): AdvancedCudfAggregate = {
    // merge will leverage the "preStepAndClose" method of the AdvancedCudfAggregate,
    // so specify the 'preProcessOutLen' to None.
    new UDAFCudfAggregate(aggBufferTypes, function.mergeAggregation())
  }
}

case class GpuScalaUDAF(
    function: RapidsUDAF,
    dataType: DataType,
    children: Seq[Expression],
    udafName: Option[String],
    nullable: Boolean) extends GpuUDAFFunctionBase {

  override val name: String = udafName.getOrElse(function.getClass.getSimpleName)

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    aggBufferTypes.zipWithIndex.map { case (dt, id) =>
      AttributeReference(s"${name}_$id", dt)()
    }
}

case class GpuScalaAggregator(
    function: RapidsUDAF,
    children: Seq[Expression],
    dataType: DataType,
    nullable: Boolean,
    aggregatorName: Option[String]) extends GpuUDAFFunctionBase {

  override val name: String = aggregatorName.getOrElse(function.getClass.getSimpleName)

  override lazy val aggBufferAttributes: Seq[AttributeReference] = {
    // TODO make it compatible with the Spark one by leveraging TypedImperativeAggExprMeta.
    // https://github.com/NVIDIA/spark-rapids/issues/13452
    // The Spark ScalaAggregator returns only a BinaryType column as the aggregate buffer,
    // so the current implementation is not compatible with the Spark one.
    aggBufferTypes.zipWithIndex.map { case (dt, id) =>
      AttributeReference(s"${name}_$id", dt)()
    }
  }
}

object GpuUDAFMeta {
  def scalaUDAFMeta: ExprRule[ScalaUDAF] = GpuOverrides.expr[ScalaUDAF](
    "User Defined Aggregate Function, the UDAF can choose to implement a RAPIDS" +
      " accelerated interface to get better performance.",
    ExprChecks.reductionAndGroupByAgg(
      GpuUserDefinedFunction.udfTypeSig,
      TypeSig.all,
      repeatingParamCheck =
        Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
    (expr, conf, p, r) => new ExprMeta(expr, conf, p, r) {
      private val opRapidsUDAF = GpuScalaUDF.getRapidsUDFInstance[RapidsUDAF](expr.udaf)

      override def tagExprForGpu(): Unit = {
        if (opRapidsUDAF.isEmpty) {
          val udfClass = expr.udaf.getClass
          willNotWorkOnGpu(s"${expr.name} implemented by $udfClass does not " +
            s"provide a GPU implementation")
        }
      }

      override def convertToGpu(): GpuExpression = {
        require(opRapidsUDAF.isDefined)
        GpuScalaUDAF(
          opRapidsUDAF.get,
          expr.dataType,
          childExprs.map(_.convertToGpu()),
          expr.udafName,
          expr.nullable)
      }
    }
  )

  def scalaAggregatorMeta[IN, BUF, OUT]: ExprRule[ScalaAggregator[IN, BUF, OUT]] =
    GpuOverrides.expr[ScalaAggregator[IN, BUF, OUT]](
      "User Defined Aggregator, it can choose to implement a RAPIDS" +
        " accelerated interface to get better performance.",
      ExprChecks.reductionAndGroupByAgg(
        GpuUserDefinedFunction.udfTypeSig,
        TypeSig.all,
        repeatingParamCheck =
          Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
      (expr, conf, p, r) => new ExprMeta(expr, conf, p, r) {
        private val opRapidsUDAF = GpuScalaUDF.getRapidsUDFInstance[RapidsUDAF](expr.agg)

        override def tagExprForGpu(): Unit = {
          if (opRapidsUDAF.isEmpty) {
            val udfClass = expr.agg.getClass
            willNotWorkOnGpu(s"${expr.name} implemented by $udfClass does not provide " +
              s"a GPU implementation")
          }
        }

        override def convertToGpu(): GpuExpression = {
          require(opRapidsUDAF.isDefined)
          GpuScalaAggregator(
            opRapidsUDAF.get,
            childExprs.map(_.convertToGpu()),
            expr.dataType,
            expr.nullable,
            expr.aggregatorName)
        }
      }
    )
}

