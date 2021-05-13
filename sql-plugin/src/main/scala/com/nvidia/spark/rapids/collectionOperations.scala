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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ColumnView}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

case class GpuConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {

  @transient override lazy val dataType: DataType = {
    if (children.isEmpty) {
      StringType
    } else {
      super.dataType
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): Any = dataType match {
    case StringType => stringConcat(batch)
    case ArrayType(_, _) => listConcat(batch)
  }

  private def stringConcat(batch: ColumnarBatch): GpuColumnVector = {
    val rows = batch.numRows()

    withResource(ArrayBuffer.empty[ColumnVector]) { buffer =>
      withResource(GpuScalar.from(null, StringType)) { nullScalar =>
        // build input buffer
        children.foreach { child =>
          child.columnarEval(batch) match {
            case cv: GpuColumnVector =>
              buffer += cv.getBase
            case null =>
              buffer += GpuColumnVector.from(nullScalar, rows, StringType).getBase
            case sv: Any =>
              val scalar = GpuScalar.from(sv.asInstanceOf[UTF8String].toString, StringType)
              withResource(scalar) { scalar =>
                buffer += GpuColumnVector.from(scalar, rows, StringType).getBase
              }
          }
        }
        // run string concatenate
        withResource(GpuScalar.from("", StringType)) { emptyScalar =>
          GpuColumnVector.from(ColumnVector.stringConcatenate(emptyScalar, nullScalar,
            buffer.toArray[ColumnView]), StringType)
        }
      }
    }
  }

  private def listConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(ArrayBuffer[ColumnVector]()) { buffer =>
      // build input buffer
      children.foreach { child =>
        child.columnarEval(batch) match {
          case cv: GpuColumnVector => buffer += cv.getBase
          case _ => throw new UnsupportedOperationException("Unsupported GpuScalar of List")
        }
      }
      // run list concatenate
      GpuColumnVector.from(ColumnVector.listConcatenateByRow(buffer: _*), dataType)
    }
  }
}

case class GpuSize(child: Expression, legacySizeOfNull: Boolean)
  extends GpuUnaryExpression {

  require(child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType],
    s"The size function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = IntegerType
  override def nullable: Boolean = if (legacySizeOfNull) false else super.nullable

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {

    // Compute sizes of cuDF.ListType to get sizes of each ArrayData or MapData, considering
    // MapData is represented as List of Struct in terms of cuDF.
    withResource(input.getBase.countElements()) { collectionSize =>
      if (legacySizeOfNull) {
        withResource(GpuScalar.from(-1)) { nullScalar =>
          withResource(input.getBase.isNull) { inputIsNull =>
            inputIsNull.ifElse(nullScalar, collectionSize)
          }
        }
      } else {
        collectionSize.incRefCount()
      }
    }
  }
}
