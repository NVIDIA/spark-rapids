/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuScalar}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF

import org.apache.spark.sql.catalyst.expressions.{Expression, UserDefinedExpression}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuHiveUDFBase(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean) extends GpuExpression with UserDefinedExpression with Serializable {

  private[this] val nvtxRangeName = "UDF: " + name

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = true

  override def foldable: Boolean = udfDeterministic && children.forall(_.foldable)

  @transient
  val function: RapidsUDF

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name

  private[this] def evalExpr(expr: Expression, batch: ColumnarBatch): GpuColumnVector = {
    expr.columnarEval(batch) match {
      case v: GpuColumnVector => v
      case other =>
        withResource(GpuScalar.from(other, expr.dataType)) { s =>
          GpuColumnVector.from(s, batch.numRows(), expr.dataType)
        }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(children.safeMap(evalExpr(_, batch))) { exprResults =>
      val funcInputs = exprResults.map(_.getBase()).toArray
      withResource(new NvtxRange(nvtxRangeName, NvtxColor.PURPLE)) { _ =>
        closeOnExcept(function.evaluateColumnar(funcInputs: _*)) { resultColumn =>
          if (batch.numRows() != resultColumn.getRowCount) {
            throw new IllegalStateException("UDF returned a different row count than the input, " +
                s"expected ${batch.numRows} found ${resultColumn.getRowCount}")
          }
          GpuColumnVector.fromChecked(resultColumn, dataType)
        }
      }
    }
  }
}

/** GPU-accelerated version of Spark's `HiveSimpleUDF` */
case class GpuHiveSimpleUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean)
    extends GpuHiveUDFBase(name, funcWrapper, children, dataType, udfDeterministic) {
  @transient
  override lazy val function: RapidsUDF = funcWrapper.createFunction[UDF]().asInstanceOf[RapidsUDF]

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

/** GPU-accelerated version of Spark's `HiveGenericUDF` */
case class GpuHiveGenericUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean,
    override val foldable: Boolean)
    extends GpuHiveUDFBase(name, funcWrapper, children, dataType, udfDeterministic) {
  @transient
  override lazy val function: RapidsUDF = funcWrapper.createFunction[GenericUDF]()
      .asInstanceOf[RapidsUDF]
}
