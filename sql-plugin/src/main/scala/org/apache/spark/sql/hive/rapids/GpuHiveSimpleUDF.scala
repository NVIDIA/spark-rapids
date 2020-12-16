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
import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.hive.ql.exec.UDF

import org.apache.spark.sql.catalyst.expressions.{Expression, UserDefinedExpression}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.HiveSimpleUDF
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** GPU-accelerated version of Spark's `HiveSimpleUDF` */
case class GpuHiveSimpleUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean) extends GpuExpression with UserDefinedExpression {

  private[this] val nvtxRangeName = "UDF: " + name

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = true

  override def foldable: Boolean = udfDeterministic && children.forall(_.foldable)

  @transient
  lazy val function: RapidsUDF = funcWrapper.createFunction[UDF]().asInstanceOf[RapidsUDF]

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"

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

object GpuHiveSimpleUDF {
  /**
   * Potentially builds a mapping from `HiveSimpleUDF` to a GPU replacement rule.
   * Returns an empty map if the Spark distribution does not contain Hive support.
   */
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      Map(classOf[HiveSimpleUDF] -> buildRule())
    } catch {
      case e: NoClassDefFoundError =>
        // This is likely caused by the Spark distribution not containing Hive support.
        if (e.getMessage != null && e.getMessage.contains("HiveSimpleUDF")) {
          Map.empty
        } else {
          throw e
        }
    }
  }

  /**
   * Builds an expression rule for `HiveSimpleUDF`. This code is here to obtain access
   * to the `HiveSimpleUDF` class that is normally hidden.
   */
  private def buildRule(): ExprRule[HiveSimpleUDF] = GpuOverrides.expr[HiveSimpleUDF](
    "Hive UDF, support requires the UDF to implement a RAPIDS-accelerated interface",
    ExprChecks.projectNotLambda(
      TypeSig.commonCudfTypes + TypeSig.ARRAY.nested(TypeSig.commonCudfTypes),
      TypeSig.all,
      repeatingParamCheck = Some(RepeatingParamCheck(
        "param",
        TypeSig.commonCudfTypes,
        TypeSig.all))),
    (a, conf, p, r) => new ExprMeta[HiveSimpleUDF](a, conf, p, r) {
      override def tagExprForGpu(): Unit = {
        a.function match {
          case _: RapidsUDF =>
          case _ =>
            willNotWorkOnGpu(s"Hive UDF ${a.name} implemented by " +
                s"${a.funcWrapper.functionClassName} does not provide a GPU implementation")
        }
      }

      override def convertToGpu(): GpuExpression = {
        // To avoid adding a Hive dependency just to check if the UDF function is deterministic,
        // we use the original HiveSimpleUDF `deterministic` method as a proxy.
        GpuHiveSimpleUDF(
          a.name,
          a.funcWrapper,
          childExprs.map(_.convertToGpu()),
          a.dataType,
          a.deterministic)
      }
    })
}
