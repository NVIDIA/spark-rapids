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

package org.apache.spark.sql.rapids

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
    (col: cudf.ColumnVector) =>{
//      TableDebug.get().debug("historam: ", col);
  col.reduce(ReductionAggregation.histogram(), DType.LIST)
    }
  override lazy val groupByAggregate: GroupByAggregation = GroupByAggregation.histogram()
  override val name: String = "CudfHistogram"
}

case class CudfMergeHistogram(override val dataType: DataType)
  extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => {

//      TableDebug.get().debug("merge historam: ", col);

//      withResource(col.getChildColumnView(0)) { listChild =>
//        listChild.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)
//      }
      col.getType match {
        // This is called from updateAggregate in GpuPercentileWithFrequency.
        case DType.STRUCT => {
          // Check the input frequencies to make sure they are non-negative to comply with Spark.
//          col.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)
          withResource(col.getChildColumnView(0)) { values =>
//            TableDebug.get().debug("value: ", values);

            withResource(col.getChildColumnView(1)) { frequencies =>
//              TableDebug.get().debug("frequencies: ", frequencies);

              withResource(Histogram.createHistogramIfValid(values,
                frequencies, false)) {
                histograms =>
                  histograms.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)

              }
            }
          }
        }

        // This is always called from mergeAggregate.
        case DType.LIST => withResource(col.getChildColumnView(0)) { listChild =>
          listChild.reduce(ReductionAggregation.mergeHistogram(), DType.LIST)
        }

        case _ => throw new IllegalStateException("Unexpected DType for histogram input")
      }
    }
  override lazy val groupByAggregate: GroupByAggregation = GroupByAggregation.mergeHistogram()
  override val name: String = "CudfMergeHistogram"
}

case class GpuPercentileEvaluation(child: Expression,
                                   percentage: Either[Double, Array[Double]],
                                   dataType: DataType,
                                   isReduction: Boolean)
  extends GpuExpression with ShimExpression {
  override def prettyName: String = "percentile_evaluation"



  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(child.columnarEval(batch)) { histogramArray =>

//      TableDebug.get().debug("evaluating histogramArray: ", histogramArray.getBase);

        val percentageArray = percentage match {
          case Left(p) => Array(p)
          case Right(p) => p
        }
        val outputAsList = dataType match {
          case _: ArrayType => true
          case _ => false
        }


//      withResource(histogramArray.getBase.getChildColumnView(0)) { histogram =>
//        System.err.println("evaluation, input size  = " + histogram.getRowCount)
//
//        val percentiles = Histogram.percentileFromHistogram(
//          histogram, percentageArray, outputAsList)
//        GpuColumnVector.from(percentiles, dataType)
//      }
          val percentiles = Histogram.percentileFromHistogram(
            histogramArray.getBase, percentageArray, outputAsList)
          GpuColumnVector.from(percentiles, dataType)

    }
  }

  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(child)
}

/**
 * Compute percentile of the input number(s).
 *
 * The two 'offset' parameters are not used by GPU version, but are here for the compatibility
 * with the CPU version and automated checks.
 */
abstract class GpuPercentile(childExpr: Expression,
                             percentageLit: GpuLiteral, isReduction: Boolean,
                             mutableAggBufferOffset: Int,
                             inputAggBufferOffset: Int)
  extends GpuAggregateFunction with Serializable {

  private val valueExpr = childExpr

  // Output type of the aggregations.
  val aggregationOutputType: DataType = ArrayType(StructType(Seq(
    StructField("value", valueExpr.dataType),
    StructField("frequency", LongType))), containsNull = false)

  protected lazy val mergeHistogram = new CudfMergeHistogram(aggregationOutputType)
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeHistogram)
  override lazy val evaluateExpression: Expression =
    GpuPercentileEvaluation(histogramBuff, percentage, finalDataType, isReduction)

  // Type of data stored in the buffer after postUpdate.
//  private val histogramBufferType: DataType = StructType(Seq(
//    StructField("value", valueExpr.dataType),
//    StructField("frequency", LongType)))
  protected final lazy val histogramBuff: AttributeReference =
    AttributeReference("histogramBuff", aggregationOutputType)()

  // Output type of percentile.
  override def dataType: DataType = aggregationOutputType
//    percentageExpr.dataType match {
//    case _: ArrayType => {
//      System.err.println("output array")
//      ArrayType(DoubleType, containsNull = false)
//    }
//    case _ => {
//      System.err.println("output double")
//      DoubleType
//    }
//  }
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
    Seq(GpuLiteral.create(new GenericArrayData(Array.empty[Any]), aggregationOutputType))
  }
//  override val initialValues: Seq[Expression] = Seq(GpuLiteral.create(null,
//  aggregationOutputType))
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

case class GpuNothing(child: Expression, msg: String) extends GpuExpression {
  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    System.err.println(msg)
    withResourceIfAllowed(child.columnarEvalAny(batch)) {
      case cv: GpuColumnVector => {
        withResource(cv.getBase.getChildColumnView(0)) { listChild =>
          System.err.println("   " + msg + ", data size = " + listChild.getRowCount)
        }
        cv.incRefCount()
        //GpuColumnVector.from(cv.getBase, cv.dataType())
        cv
      }


      case other =>
        throw new
            IllegalArgumentException(s"Got an unexpected type out of columnarEvalAny $other")
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    GpuExpressionsUtils.resolveColumnVector(columnarEvalAny(batch), batch.numRows())


  override def nullable: Boolean = child.nullable

  // Output type should be the element type of the input array.
  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType

  override def children: Seq[Expression] = Seq(child)
}

/**
 * Compute percentile of the input number(s).
 */
case class GpuPercentileDefault(childExpr: Expression,
                                percentage: GpuLiteral, isReduction: Boolean,
                                mutableAggBufferOffset: Int,
                                inputAggBufferOffset: Int)
  extends GpuPercentile(childExpr, percentage, isReduction, mutableAggBufferOffset,
    inputAggBufferOffset) {


   def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): GpuPercentile = {
//    val copy = this.getClass.getConstructor(
//        classOf[Expression], classOf[GpuLiteral], classOf[Boolean], classOf[Int], classOf[Int])
//      .newInstance(childExpr, percentage, isReduction, newMutableAggBufferOffset,
//        inputAggBufferOffset)
//      .asInstanceOf[GpuPercentile]

    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): GpuPercentile =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val inputProjection: Seq[Expression] = Seq(childExpr)

  val updateHistogram =new CudfHistogram(aggregationOutputType)
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(updateHistogram)

    override lazy val postUpdate: Seq[Expression] =
      Seq(GpuNothing(updateHistogram.attr, "postUpdate"))
    override lazy val preMerge: Seq[Expression] =
      Seq(GpuNothing(histogramBuff, "preMerge"))
    override lazy val postMerge: Seq[Expression] =
      Seq(GpuNothing(mergeHistogram.attr, "postMerge"))
}

/**
 * Compute percentile of the input number(s) associated with frequencies.
 */
case class GpuPercentileWithFrequency(childExpr: Expression, percentage: GpuLiteral,
                                      frequencyExpr: Expression, isReduction: Boolean,
                                      mutableAggBufferOffset: Int,
                                      inputAggBufferOffset: Int)
  extends GpuPercentile(childExpr, percentage, isReduction, mutableAggBufferOffset,
    inputAggBufferOffset) {
  override val inputProjection: Seq[Expression] = {
    if(isReduction) {
      val childrenWithNames = GpuLiteral("value", StringType) :: childExpr ::
        GpuLiteral("frequency", StringType) :: frequencyExpr :: Nil
      GpuCreateNamedStruct(childrenWithNames) :: Nil
    } else {
      Seq(GpuCreateHistogramIfValid(childExpr, frequencyExpr, isReduction, aggregationOutputType))
    }
  }
  override lazy val updateAggregates: Seq[CudfAggregate] =  Seq(new CudfMergeHistogram(dataType))
}

object GpuPercentile{
  def apply(childExpr: Expression,
            percentageLit: GpuLiteral,
            frequencyExpr: Expression,
            isReduction: Boolean,
            mutableAggBufferOffset: Int = 0,
            inputAggBufferOffset: Int = 0): GpuPercentile = {
    frequencyExpr match {
      case GpuLiteral(freq, LongType) if freq == 1 =>
        GpuPercentileDefault(childExpr, percentageLit, isReduction,
          mutableAggBufferOffset, inputAggBufferOffset)
      case _  =>
        GpuPercentileWithFrequency(childExpr, percentageLit, frequencyExpr, isReduction,
          mutableAggBufferOffset, inputAggBufferOffset)
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
