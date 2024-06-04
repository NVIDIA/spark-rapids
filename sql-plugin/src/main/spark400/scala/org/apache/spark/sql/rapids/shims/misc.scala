/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, NullType, StringType}
import org.apache.spark.sql.internal.types.StringTypeAnyCollation

case class GpuRaiseError(
   errorClass: Expression,
   errorParams: Expression) extends GpuBinaryExpression with ImplicitCastInputTypes {

  def this(str: Expression) = {
    this(Literal(
      if (SQLConf.get.getConf(SQLConf.LEGACY_RAISE_ERROR_WITHOUT_ERROR_CLASS)) {
        "_LEGACY_ERROR_USER_RAISED_EXCEPTION"
      } else {
        "USER_RAISED_EXCEPTION"
      }),
      CreateMap(Seq(Literal("errorMessage"), str)))
  }

  def this(errorClass: Expression, errorParms: Expression) = {
    this(errorClass, errorParms)
  }

  override def dataType: DataType = NullType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, MapType(StringType, StringType))

  /** Could evaluating this expression cause side-effects, such as throwing an exception? */
  override def hasSideEffects: Boolean = true

  override protected def doColumnar(
     lhs: GpuColumnVector,
     rhs: GpuColumnVector): ColumnVector = {
    if (lhs.getRowCount <= 0) {
      // For the case: when(condition, raise_error(col("a"))
      return GpuColumnVector.columnVectorFromNull(0, NullType)
    }

    // Take the first one as the error message
    withResource(lhs.getBase.getScalarElement(0)) { errorClass =>
      if (!errorClass.isValid()) {
        throw new RuntimeException()
      } else {
        withResource(rhs.getBase.getScalarElement(0)) { errorParams =>
          if (!errorParams.isValid()) {
            throw new RuntimeException()
          } else {
            if (errorClass.getJavaString.equals("USER_RAISED_EXCEPTION") ||
              errorClass.getJavaString.equals("_LEGACY_ERROR_USER_RAISED_EXCEPTION")) {
              val strMessage = GpuGetMapValue(errorParams, "errorMessage", false)
              throw new RapidsAnalysisException(strMessage)
            } else {
              RapidsSparkThrowableHelper()
            }
        }
      }
    }
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, MapType(StringType, StringType))

  override def left: Expression = errorClass

  override def right: Expression = errorParms

  override def prettyName: String = "raise_error"

  override protected def withNewChildrenInternal(
     newLeft: Expression, newRight: Expression): RaiseError = {
    copy(errorClass = newLeft, errorParms = newRight)
  }
}
