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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Expression, UserDefinedExpression}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Common implementation across all RAPIDS accelerated UDF types */
trait GpuUserDefinedFunction extends GpuExpression with UserDefinedExpression with Serializable {
  /** name of the UDF function */
  val name: String

  /** User's UDF instance */
  val function: RapidsUDF

  /** True if the UDF is deterministic */
  val udfDeterministic: Boolean

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  private[this] val nvtxRangeName = s"UDF: $name"
  private[this] lazy val funcCls = TrampolineUtil.getSimpleName(function.getClass)
  private[this] lazy val inputTypesString = children.map(_.dataType.catalogString).mkString(", ")
  private[this] lazy val outputType = dataType.catalogString

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
  val udfTypeSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL +
      TypeSig.BINARY + TypeSig.CALENDAR + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()
}
