/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, HostColumnVector, NvtxColor, NvtxRange}
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UserDefinedExpression
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

/** Common implementation across all RAPIDS accelerated UDF types */
trait GpuUserDefinedFunction extends GpuExpression
    with ShimExpression
    with UserDefinedExpression with Serializable {
  /** name of the UDF function */
  val name: String

  /** User's UDF instance */
  val function: RapidsUDF

  /** True if the UDF is deterministic */
  val udfDeterministic: Boolean

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  private[this] val nvtxRangeName = s"UDF: $name"
  private[this] lazy val funcCls = TrampolineUtil.getSimpleName(function.getClass)
  protected lazy val inputTypesString = children.map(_.dataType.catalogString).mkString(", ")
  protected lazy val outputType = dataType.catalogString

  override def columnarEval(batch: ColumnarBatch): Any = {
    val cols = children.safeMap(GpuExpressionsUtils.columnarEvalToColumn(_, batch))
    withResource(cols) { exprResults =>
      val funcInputs = exprResults.map(_.getBase()).toArray
      withResource(new NvtxRange(nvtxRangeName, NvtxColor.PURPLE)) { _ =>
        try {
          closeOnExcept(function.evaluateColumnar(funcInputs: _*)) { resultColumn =>
            if (batch.numRows() != resultColumn.getRowCount) {
              throw new IllegalStateException("UDF returned a different row count than the " +
                  s"input, expected ${batch.numRows} found ${resultColumn.getRowCount}")
            }
            GpuColumnVector.fromChecked(resultColumn, dataType)
          }
        } catch {
          case e: Exception =>
            throw new SparkException("Failed to execute user defined function " +
                s"($funcCls: ($inputTypesString) => $outputType)", e)
        }
      }
    }
  }
}

object GpuUserDefinedFunction {
  // UDFs can support all types except UDT which does not have a clear columnar representation.
  val udfTypeSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.NULL +
      TypeSig.BINARY + TypeSig.CALENDAR + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()
}

/**
 * Try to execute an UDF efficiently by
 *   1 running the UDF on GPU if it is an instance of RapidsUDF. Otherwise,
 *   2 pull back only the columns the UDF needs to host and do the processing on CPU.
 */
trait RapidsUserDefinedFunction extends GpuUserDefinedFunction with Logging {

  /** Whether this UDF implements the RapidsUDF interface */
  protected def rapidsFunc: Option[RapidsUDF]

  /** The row based function of the UDF. */
  protected def evaluateRow(childrenRow: InternalRow): Any

  override final val function: RapidsUDF = rapidsFunc.orNull

  override def columnarEval(batch: ColumnarBatch): Any = {
    rapidsFunc.map { _ =>
      // It is a RapidsUDF instance.
      super.columnarEval(batch)
    }.getOrElse {
      logInfo(s"Begin to execute the UDF($name) row by row.")
      // It is only a CPU based UDF
      // These child columns will be closed by `ColumnarToRowIterator`.
      val argCols = children.safeMap(GpuExpressionsUtils.columnarEvalToColumn(_, batch))
      try {
        // 1 Convert the argument columns to row.
        // 2 Evaluate the CPU UDF row by row and cache the result.
        // 3 Build a result column from the cache.
        closeOnExcept(new RowDataBuffer(dataType, batch.numRows)) { buffer =>
          new ColumnarToRowIterator(
              Iterator.single(new ColumnarBatch(argCols.toArray, batch.numRows())),
              NoopMetric,
              NoopMetric,
              NoopMetric,
              NoopMetric).foreach { row =>
            buffer.append(evaluateRow(row))
          }
          closeOnExcept(buffer.copyToDevice()) { resultCol =>
            GpuColumnVector.from(resultCol, dataType)
          }
        }
      } catch {
        case e: Exception =>
          throw new SparkException("Failed to execute user defined function: " +
            s"($name: ($inputTypesString) => $outputType)", e)
      }
    }
  } // end of `columnarEval`

}

/** The wrapper of a ColumnBuilder to support appending an `Any`. */
class RowDataBuffer(rowType: DataType, estimatedRows: Int) extends AutoCloseable {
  private val builder =
    new HostColumnVector.ColumnBuilder(GpuScalar.resolveElementType(rowType), estimatedRows)

  /**
   * Append a value of Catalyst type to this buffer.
   * The value `row` should be compatible with the `rowType` used to create this buffer.
   */
  def append(row: Any): this.type = {
    if (row == null) {
      builder.appendNull()
    } else {
      // A boxed version of primitive types will be unboxed, and it is safe for non-nulls here.
      rowType match {
        case NullType => builder.appendNull()
        case ByteType => builder.append(row.asInstanceOf[Byte])
        case ShortType => builder.append(row.asInstanceOf[Short])
        case FloatType => builder.append(row.asInstanceOf[Float])
        case DoubleType => builder.append(row.asInstanceOf[Double])
        case BooleanType => builder.append(row.asInstanceOf[Boolean])
        case IntegerType | DateType => builder.append(row.asInstanceOf[Int])
        case LongType | TimestampType => builder.append(row.asInstanceOf[Long])
        case StringType => builder.appendUTF8String(row.asInstanceOf[UTF8String].getBytes)
        case dt: DecimalType =>
          GpuScalar.convertDecimalTo(row.asInstanceOf[Decimal], dt) match {
            case Left(dec32AsInt) => builder.append(dec32AsInt)
            case Right(dec64AsLong) => builder.append(dec64AsLong)
          }
        case StructType(_) =>
          val dataAsStruct = GpuScalar.convertElementTo(row, rowType)
          builder.appendStructValues(dataAsStruct.asInstanceOf[HostColumnVector.StructData])
        case ArrayType(_, _) | MapType(_, _, _) =>
          val dataAsList = GpuScalar.convertElementTo(row, rowType)
          builder.appendLists(dataAsList.asInstanceOf[java.util.List[_]])
        case ot =>
          throw new IllegalArgumentException(s"Failed to append the value ('$row'), " +
            s"because of unsupported data type ($ot).")
      }
    }
    this
  }

  /**
   * Copy the data collected so far to device.
   * The returned column should be closed after done.
   */
  def copyToDevice(): ColumnVector = builder.buildAndPutOnDevice()

  override def close(): Unit = builder.close()
}
