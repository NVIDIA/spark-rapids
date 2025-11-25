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

import ai.rapids.cudf.{ColumnView, DType, GroupByAggregationOnColumn, Scalar}
import com.nvidia.spark.{RapidsSimpleGroupByAggregation, RapidsUDAF, RapidsUDAFGroupByAggregation}
import com.nvidia.spark.rapids.{ExprChecks, ExprRule, GpuColumnVector, GpuExpression, GpuLiteral, GpuOverrides, GpuScalar, GpuUnsignedIntegerType, GpuUnsignedLongType, GpuUserDefinedFunction, ImperativeAggExprMeta, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, UserDefinedExpression}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.rapids.GpuScalaUDF
import org.apache.spark.sql.types._

object GpuUDAFUtils {
  /**
   * Infer the Spark type from a given cuDF ColumnView.
   *
   * The type returned can not be used to interact with the Spark world, only for
   * the GPU process internally when asking for a ColumnarBatch without given the
   * Spark type. Because it may not always reflect the original Spark type.
   * e.g.
   *   A List of Struct column in cuDF may be either from MapType or a real List
   *   of Struct type in Spark.
   *   A INT32 column in cuDF may be from either YearMonthIntervalType or IntegerType
   *   in Spark.
   *
   *  It is designed for the "preStep" operation in GPU UDAF aggregates.
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
   * Infer the Spark type from a given cuDF Scalar, similar as infer(ColumnView).
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

  type UDAFEvalFunc = (Int, Array[GpuColumnVector]) => GpuColumnVector
  type UDAFEvalHandler = (UDAFEvalFunc, Int, Int)
}

/**
 * The wrapper of a RapidsUDAFGroupByAggregation to interact with the GPU
 * aggregate framework via cuDF columns and scalars instead of Spark Expressions.
 *
 * This class is introduced because the current CudfAggregate does not support multiple
 * inputs for a single "reduce" and "aggregate" call that UDAFs require. It also means
 * "preStep" with variable length output should be supported. And the output sizes
 * and types of the "preStep" can not be determined until the first batch is processed,
 * so the Expression (need to get the column type in advance) way in the current
 * GpuAggregateFunction can not be used for UDAF things.
 */
class UDAFAggregate(
    aggBufferTypes: Array[DataType],
    udafAgg: RapidsUDAFGroupByAggregation) extends Serializable {

  require(udafAgg.isInstanceOf[RapidsSimpleGroupByAggregation],
    "Only 'RapidsSimpleGroupByAggregation' is supported.")

  // Similar as the "inputProjection/preMerge" of GpuAggregateFunction
  def preStepAndClose(numRows: Int, args: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    closeOnExcept(udafAgg.preStep(numRows, args.map(_.getBase))) { preRet =>
      if (preRet.length == args.length) {
        try {
          // try the input types first
          return preRet.zip(args.map(_.dataType())).map { case (cudfCol, dt) =>
            GpuColumnVector.fromChecked(cudfCol, dt)
          }
        } catch {
          case _: IllegalArgumentException => // Some change in column types
        }
      }
      // Some transformation is made, so infer the types from the output.
      preRet.map(col => GpuColumnVector.fromChecked(col, GpuUDAFUtils.infer(col)))
    }
  }

  // Similar as the "reductionAggregate" of CudfAggregate
  def reduce(numRows: Int, args: Array[GpuColumnVector]): Array[GpuScalar] = {
    closeOnExcept(udafAgg.reduce(numRows, args.map(_.getBase))) { reducedRet =>
      if (reducedRet.length == args.length) {
        try {
          // try the input types first
          return reducedRet.zip(args.map(_.dataType())).map { case (cuScalar, dt) =>
            GpuScalar(cuScalar, dt)
          }
        } catch {
          case _: IllegalArgumentException => // Some change in Scalar types
        }
      }
      // Type or size changes, infer the types from the output.
      reducedRet.map(scalar => GpuScalar(scalar, GpuUDAFUtils.infer(scalar)))
    }
  }

  // Similar as the "groupByAggregate" of CudfAggregate
  def aggregate(inputIds: Array[Int]): Array[GroupByAggregationOnColumn] = {
    udafAgg.asInstanceOf[RapidsSimpleGroupByAggregation].aggregate(inputIds)
  }

  // Similar as the "postUpdate/postMerge" of GpuAggregateFunction
  def postStepAndClose(numRows: Int, args: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    closeOnExcept(udafAgg.postStep(numRows, args.map(_.getBase))) { postCols =>
      require(postCols.length == aggBufferTypes.length,
        "The output sizes of the 'postStep' and 'bufferTypes' does not match. " +
          s"Sizes: ${postCols.length} vs ${aggBufferTypes.length}")
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

/**
 * Common implementation for all the types of GPU UDAF interfaces.
 * It is also a wrapper of a RapidUDAF to interact with the GPU
 * aggregate framework via cuDF columns and scalars instead of Spark expressions.
 */
trait GpuUserDefinedAggregateFunction extends GpuAggregateFunction
  with UserDefinedExpression
  with Serializable {

  /** User's UDAF instance */
  protected def function: RapidsUDAF

  protected lazy val aggBufferTypes: Array[DataType] = function.bufferTypes()

  // similar as "evaluateExpression" in GpuAggregateFunction
  def resultEvalAndClose(numRows: Int, args: Array[GpuColumnVector]): GpuColumnVector = {
    closeOnExcept(function.getResult(numRows, args.map(_.getBase), dataType)) { postCol =>
      try {
        GpuColumnVector.fromChecked(postCol, dataType)
      } catch {
        case iae: IllegalArgumentException =>
          throw new RuntimeException("The output of the 'postProcess' does not match " +
            "the UDAF result type", iae)
      }
    }
  }

  // similar as "updateAggregates" in GpuAggregateFunction
  def updateAggregate(): UDAFAggregate = {
    new UDAFAggregate(aggBufferTypes, function.updateAggregation())
  }

  // similar as "mergeAggregates" in GpuAggregateFunction
  def mergeAggregate(): UDAFAggregate = {
    new UDAFAggregate(aggBufferTypes, function.mergeAggregation())
  }

  // UDAF arguments
  override final lazy val inputProjection: Seq[Expression] = children

  override final lazy val initialValues: Seq[Expression] = {
    closeOnExcept(function.getDefaultValue) { udafDefValues =>
      require(udafDefValues.length == aggBufferTypes.length,
        s"The default values number (${udafDefValues.length}) is NOT equal to " +
          s"the aggregation buffers number(${aggBufferTypes.length})")
      udafDefValues.zip(aggBufferTypes).map { case (scalar, dt) =>
        GpuLiteral(scalar, dt)
      }
    }
  }

  override final lazy val updateAggregates: Seq[CudfAggregate] = {
    throw new UnsupportedOperationException("GPU user defined aggregate function" +
      " does not support 'updateAggregates', call 'updateAggregate' instead.")
  }
  override final lazy val mergeAggregates: Seq[CudfAggregate] = {
    throw new UnsupportedOperationException("GPU user defined aggregate function" +
      " does not support 'mergeAggregates', call 'mergeAggregate' instead.")
  }
  override final lazy val evaluateExpression: Expression = {
    throw new UnsupportedOperationException("GPU user defined aggregate function" +
      " does not support 'evaluateExpression', call 'resultEvalAndClose' instead.")
  }
}

case class GpuScalaUDAF(
    function: RapidsUDAF,
    dataType: DataType,
    children: Seq[Expression],
    udafName: Option[String],
    nullable: Boolean) extends GpuUserDefinedAggregateFunction {
  override val name: String = udafName.getOrElse(function.getClass.getSimpleName)

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    aggBufferTypes.zipWithIndex.map { case (dt, id) =>
      AttributeReference(s"_${name}_agg_buffer_$id", dt)()
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
    (sUdaf, conf, p, r) => new ImperativeAggExprMeta(sUdaf, conf, p, r) {
      private val opRapidsUDAF = GpuScalaUDF.getRapidsUDFInstance[RapidsUDAF](sUdaf.udaf)

      override def tagAggForGpu(): Unit = {
        if (opRapidsUDAF.isEmpty) {
          willNotWorkOnGpu(s"${sUdaf.name} implemented by ${sUdaf.udaf.getClass} " +
            s"does not provide a GPU implementation")
        }
      }

      override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
        require(opRapidsUDAF.isDefined)
        GpuScalaUDAF(opRapidsUDAF.get, sUdaf.dataType, childExprs, sUdaf.udafName,
          sUdaf.nullable)
      }
    }
  )
}
