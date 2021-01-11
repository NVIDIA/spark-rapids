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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuScalar}
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FUNC_ALIAS
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuCreateArray(children: Seq[Expression], useStringTypeWhenEmpty: Boolean)
    extends GpuExpression {

  def this(children: Seq[Expression]) = {
    this(children, SQLConf.get.getConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE))
  }

  override def foldable: Boolean = children.forall(_.foldable)

  override def stringArgs: Iterator[Any] = super.stringArgs.take(1)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), s"function $prettyName")
  }

  private val defaultElementType: DataType = {
    if (useStringTypeWhenEmpty) {
      StringType
    } else {
      NullType
    }
  }

  override def dataType: ArrayType = {
    ArrayType(
      TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(children.map(_.dataType))
          .getOrElse(defaultElementType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def prettyName: String = "array"

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(new Array[ColumnVector](children.size)) { columns =>
      val numRows = batch.numRows()
      children.indices.foreach { index =>
        children(index).columnarEval(batch) match {
          case cv: GpuColumnVector =>
            columns(index) = cv.getBase
          case other =>
            val dt = dataType.elementType
            withResource(GpuScalar.from(other, dt)) { scalar =>
              columns(index) = ColumnVector.fromScalar(scalar, numRows)
            }
        }
      }
      GpuColumnVector.from(ColumnVector.makeList(numRows,
        GpuColumnVector.getNonNestedRapidsType(dataType.elementType),
        columns: _*), dataType)
    }
  }
}

case class GpuCreateNamedStruct(children: Seq[Expression]) extends GpuExpression {
  lazy val (nameExprs, valExprs) = children.grouped(2).map {
    case Seq(name, value) => (name, value)
  }.toList.unzip

  private lazy val names = nameExprs.map {
    case g: GpuExpression => g.columnarEval(null)
    case e => e.eval(EmptyRow)
  }

  override def nullable: Boolean = false

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map {
      case (name, expr) =>
        val metadata = expr match {
          case ne: NamedExpression => ne.metadata
          case _ => Metadata.empty
        }
        StructField(name.toString, expr.dataType, expr.nullable, metadata)
    }
    StructType(fields)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
    } else {
      val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType == StringType)
      if (invalidNames.nonEmpty) {
        TypeCheckResult.TypeCheckFailure(
          s"Only foldable ${StringType.catalogString} expressions are allowed to appear at odd" +
              s" position, got: ${invalidNames.mkString(",")}")
      } else if (!names.contains(null)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure("Field name should not be null")
      }
    }
  }

  // There is an alias set at `CreateStruct.create`. If there is an alias,
  // this is the struct function explicitly called by a user and we should
  // respect it in the SQL string as `struct(...)`.
  override def prettyName: String = getTagValue(FUNC_ALIAS).getOrElse("named_struct")

  override def sql: String = getTagValue(FUNC_ALIAS).map { alias =>
    val childrenSQL = children.indices.filter(_ % 2 == 1).map(children(_).sql).mkString(", ")
    s"$alias($childrenSQL)"
  }.getOrElse(super.sql)

  override def columnarEval(batch: ColumnarBatch): Any = {
    // The names are only used for the type. Here we really just care about the data
    withResource(new Array[ColumnVector](valExprs.size)) { columns =>
      val numRows = batch.numRows()
      valExprs.indices.foreach { index =>
        valExprs(index).columnarEval(batch) match {
          case cv: GpuColumnVector =>
            columns(index) = cv.getBase
          case other =>
            val dt = dataType.fields(index).dataType
            withResource(GpuScalar.from(other, dt)) { scalar =>
              columns(index) = ColumnVector.fromScalar(scalar, numRows)
            }
        }
      }
      GpuColumnVector.from(ColumnVector.makeStruct(numRows, columns: _*), dataType)
    }
  }
}