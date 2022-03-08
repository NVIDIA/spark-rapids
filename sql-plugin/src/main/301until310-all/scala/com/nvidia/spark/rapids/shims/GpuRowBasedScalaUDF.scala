/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprChecks, ExprRule, GpuOverrides, GpuUserDefinedFunction, RepeatingParamCheck, TypeSig}

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.rapids.{GpuRowBasedScalaUDFBase, ScalaUDFMetaBase}
import org.apache.spark.sql.types.DataType

/** Run a row-based UDF in a GPU operation */
case class GpuRowBasedScalaUDF(
    sparkFunc: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]],
    udfName: Option[String],
    nullable: Boolean,
    udfDeterministic: Boolean)
  extends GpuRowBasedScalaUDFBase(sparkFunc, dataType, children, inputEncoders, None, udfName) {

  override def createInputConverter(i: Int, dataType: DataType): Any => Any = {
    if (inputEncoders.isEmpty) {
      // for untyped Scala UDF
      CatalystTypeConverters.createToScalaConverter(dataType)
    } else {
      val encoder = inputEncoders(i)
      if (encoder.isDefined && encoder.get.isSerializedAsStructForTopLevel) {
        val fromRow = encoder.get.resolveAndBind().createDeserializer()
        row: Any => fromRow(row.asInstanceOf[InternalRow])
      } else {
        CatalystTypeConverters.createToScalaConverter(dataType)
      }
    }
  }

  override val checkNull: Boolean = false
}

object GpuScalaUDFMeta {
  def exprMeta: ExprRule[ScalaUDF] = GpuOverrides.expr[ScalaUDF](
    "User Defined Function, the UDF can choose to implement a RAPIDS accelerated interface " +
      "to get better performance.",
    ExprChecks.projectOnly(
      GpuUserDefinedFunction.udfTypeSig,
      TypeSig.all,
      repeatingParamCheck =
        Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
    (expr, conf, p, r) => new ScalaUDFMetaBase(expr, conf, p, r) {
      override protected def rowBasedScalaUDF: GpuRowBasedScalaUDFBase =
        GpuRowBasedScalaUDF(
          expr.function,
          expr.dataType,
          childExprs.map(_.convertToGpu()),
          expr.inputEncoders,
          expr.udfName,
          expr.nullable,
          expr.udfDeterministic)
    })
}
