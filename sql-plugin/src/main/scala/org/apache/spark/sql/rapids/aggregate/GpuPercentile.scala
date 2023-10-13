/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{DType, GroupByAggregation, ReductionAggregation}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression
import com.nvidia.spark.rapids.jni.Histogram
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression,  UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CudfHistogram(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) => input.reduce(ReductionAggregation.histogram(), DType.LIST)
  override lazy val groupByAggregate: GroupByAggregation = GroupByAggregation.histogram()
  override val name: String = "CudfHistogram"
}

case class CudfMergeHistogram(override val dataType: DataType)
  extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) =>
      input.getType match {
        // This is called from updateAggregate in GpuPercentileWithFrequency.
        case DType.STRUCT => input.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)

        // This is always called from mergeAggregate.
        case DType.LIST => withResource(input.getChildColumnView(0)) { histogram =>
          histogram.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)
        }

        case _ => throw new IllegalStateException("Invalid input in CudfMergeHistogram.")
      }

  override lazy val groupByAggregate: GroupByAggregation = GroupByAggregation.mergeHistogram()
  override val name: String = "CudfMergeHistogram"
}

case class GpuPercentileEvaluation(child: Expression,
                                   percentage: Either[Double, Array[Double]],
                                   outputType: DataType,
                                   isReduction: Boolean)
  extends GpuExpression with ShimExpression {
  override def dataType: DataType = outputType
  override def prettyName: String = "percentile_evaluation"
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(child)

  private lazy val percentageArray = percentage match {
    case Left(p) => Array(p)
    case Right(p) => p
  }

  private lazy val outputAsList = outputType match {
    case _: ArrayType => true
    case _ => false
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(child.columnarEval(batch)) { histogramArray =>

      val percentiles = Histogram.percentileFromHistogram(histogramArray.getBase,
        percentageArray, outputAsList)
      GpuColumnVector.from(percentiles, outputType)
    }
  }
}

abstract class GpuPercentile(childExpr: Expression,
                             percentageLit: GpuLiteral, isReduction: Boolean)
  extends GpuAggregateFunction with Serializable {
  private val valueExpr = childExpr

  val aggregationBufferType: DataType = ArrayType(StructType(Seq(
    StructField("value", valueExpr.dataType),
    StructField("frequency", LongType))), containsNull = false)

  protected lazy val mergeHistogram = new CudfMergeHistogram(aggregationBufferType)
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeHistogram)
  override lazy val evaluateExpression: Expression =
    GpuPercentileEvaluation(histogramBuff, percentage, finalDataType, isReduction)

  protected final lazy val histogramBuff: AttributeReference =
    AttributeReference("histogramBuff", aggregationBufferType)()

  override def dataType: DataType = aggregationBufferType

  private def makeArray(v: Any): (Boolean, Either[Double, Array[Double]]) = v match {
  // Rule ImplicitTypeCasts can cast other numeric types to double
  case null => (false, Right(Array()))
  case num: Double => (false, Left(num))
  case arrayData: ArrayData => (true, Right(arrayData.toDoubleArray()))
  case other => throw new IllegalStateException(s"Invalid percentile expression $other")
}


  private lazy val (returnPercentileArray, percentage) = makeArray(percentageLit.value)

  lazy val finalDataType: DataType =
    if (returnPercentileArray) {
      ArrayType(DoubleType, containsNull = false)
    } else {
      DoubleType
    }

  override def aggBufferAttributes: Seq[AttributeReference] = histogramBuff :: Nil
  override def prettyName: String = "percentile"
  override def nullable: Boolean = true

  override val initialValues: Seq[Expression] = {
    Seq(GpuLiteral.create(new GenericArrayData(Array.empty[Any]), aggregationBufferType))
  }
  override def children: Seq[Expression] = Seq(childExpr, percentageLit)
}

case class GpuCreateHistogramIfValid(valuesExpr: Expression, frequenciesExpr: Expression,
                                    isReduction: Boolean, dataType: DataType)
  extends GpuExpression {
  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(valuesExpr.columnarEvalAny(batch)) {
      case valuesCV: GpuColumnVector =>
      withResourceIfAllowed(frequenciesExpr.columnarEvalAny(batch)) {
        case frequenciesCV: GpuColumnVector => {
          val histograms = Histogram.createHistogramIfValid(valuesCV.getBase,
            frequenciesCV.getBase, !isReduction)
          GpuColumnVector.from(histograms, dataType)
        }

        case other =>
          throw new
              IllegalArgumentException(s"Got an unexpected type out of columnarEvalAny $other")
        }
      case other =>
        throw new
            IllegalArgumentException(s"Got an unexpected type out of columnarEvalAny $other")
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    GpuExpressionsUtils.resolveColumnVector(columnarEvalAny(batch), batch.numRows())
  override def nullable: Boolean = false
  override def children: Seq[Expression] = Seq(valuesExpr, frequenciesExpr)
}

/**
 * Compute percentile of the input number(s).
 */
case class GpuPercentileDefault(childExpr: Expression,
                                percentage: GpuLiteral, isReduction: Boolean)
  extends GpuPercentile(childExpr, percentage, isReduction) {

  override val inputProjection: Seq[Expression] = Seq(childExpr)

  val updateHistogram =new CudfHistogram(aggregationBufferType)
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(updateHistogram)
}

/**
 * Compute percentile of the input number(s) associated with frequencies.
 */
case class GpuPercentileWithFrequency(childExpr: Expression, percentage: GpuLiteral,
                                      frequencyExpr: Expression, isReduction: Boolean)
  extends GpuPercentile(childExpr, percentage, isReduction) {
  override val inputProjection: Seq[Expression] = {
    if(isReduction) {
      val outputType: DataType = StructType(Seq(
        StructField("value", childExpr.dataType),
        StructField("frequency", LongType)))
      Seq(GpuCreateHistogramIfValid(childExpr, frequencyExpr, isReduction, outputType))
    } else {
      Seq(GpuCreateHistogramIfValid(childExpr, frequencyExpr, isReduction, aggregationBufferType))
    }
  }
  override lazy val updateAggregates: Seq[CudfAggregate] =  Seq(new CudfMergeHistogram(dataType))
}

object GpuPercentile{
  def apply(childExpr: Expression,
            percentageLit: GpuLiteral,
            frequencyExpr: Expression,
            isReduction: Boolean): GpuPercentile = {
    frequencyExpr match {
      case GpuLiteral(freq, LongType) if freq == 1 =>
        GpuPercentileDefault(childExpr, percentageLit, isReduction)
      case _  =>
        GpuPercentileWithFrequency(childExpr, percentageLit, frequencyExpr, isReduction)
    }
  }
}


class CpuToGpuPercentileBufferConverter(elementType: DataType)
  extends CpuToGpuAggregateBufferConverter {
  def createExpression(child: Expression): CpuToGpuBufferTransition = {
    CpuToGpuPercentileBufferTransition(child, elementType)
  }
}

case class CpuToGpuPercentileBufferTransition(override val child: Expression, elementType: DataType)
  extends CpuToGpuBufferTransition {
  override def dataType: DataType = ArrayType(StructType(Seq(
    StructField("value", elementType),
    StructField("frequency", LongType))), containsNull = false)
  override protected def nullSafeEval(input: Any): ArrayData = {
    // Deserialization from the input byte stream into the internal buffer format.
    val bytes = input.asInstanceOf[Array[Byte]]
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)

//    System.err.println("Deserializing to GPU..., length = " + bytes.length
//    + ", elementType: " + elementType + ", child.dataType: " + child.dataType)

    try {
      // Store a column of STRUCT<element, count>
      val histogram = ArrayBuffer[InternalRow]()
      val row = new UnsafeRow(2)
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        row.pointTo(bs, sizeOfNextRow)
        val element = row.get(0, elementType)
        val count = row.get(1, LongType).asInstanceOf[Long]

//        System.err.println("  -- Deserializing e-c: " + element + " --- " + count +
//          " , avaiable byte : " + ins.available)

        histogram.append(InternalRow.apply(element, count))
//        histogram.append(row.copy())
        sizeOfNextRow = ins.readInt()
      }
      ArrayData.toArrayData(histogram)
    } finally {
      ins.close()
      bis.close()
    }
  }
}

class GpuToCpuPercentileBufferConverter(elementType: DataType)
  extends GpuToCpuAggregateBufferConverter {
  def createExpression(child: Expression): GpuToCpuBufferTransition = {
    GpuToCpuPercentileBufferTransition(child, elementType)
  }
}

case class GpuToCpuPercentileBufferTransition(override val child: Expression, elementType: DataType)
  extends GpuToCpuBufferTransition {

  override protected def nullSafeEval(input: Any): Array[Byte] = {
    val buffer = new Array[Byte](4 << 10) // 4K
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)



    try {
      val histogram = input.asInstanceOf[UnsafeArrayData]
      val projection = UnsafeProjection.create(Array[DataType](elementType, LongType))
      (0 until histogram.numElements()).foreach { i =>
        val row = histogram.getStruct(i, 2)

        val element = row.get(0, elementType)

//        val count = row.get(1, LongType).asInstanceOf[Long]
//        System.err.println("  -- Serializing e-c: " + element + " --- " + count)
        if(element!= null) {
          val unsafeRow = projection.apply(row)
          out.writeInt(unsafeRow.getSizeInBytes)
          unsafeRow.writeToStream(out, buffer)
        }
      }
      out.writeInt(-1)
      out.flush()
      bos.toByteArray
    } finally {
      out.close()
      bos.close()
    }
  }
}
