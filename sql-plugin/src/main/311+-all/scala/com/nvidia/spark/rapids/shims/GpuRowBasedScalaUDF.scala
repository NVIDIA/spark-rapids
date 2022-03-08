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
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ScalaUDF}
import org.apache.spark.sql.rapids.{GpuRowBasedScalaUDFBase, ScalaUDFMetaBase}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/** Run a row-based UDF in a GPU operation */
case class GpuRowBasedScalaUDF(
    sparkFunc: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]],
    outputEncoder: Option[ExpressionEncoder[_]],
    udfName: Option[String],
    nullable: Boolean,
    udfDeterministic: Boolean)
  extends GpuRowBasedScalaUDFBase(sparkFunc, dataType, children, inputEncoders, outputEncoder,
    udfName) {

  override def createInputConverter(i: Int, dataType: DataType): Any => Any =
    scalaConverter(i, dataType)._1

  /**
   * Create the converter which converts the catalyst data type to the scala data type.
   * We use `CatalystTypeConverters` to create the converter for:
   *   - UDF which doesn't provide inputEncoders, e.g., untyped Scala UDF and Java UDF
   *   - type which isn't supported by `ExpressionEncoder`, e.g., Any
   *   - primitive types, in order to use `identity` for better performance
   * For other cases like case class, Option[T], we use `ExpressionEncoder` instead since
   * `CatalystTypeConverters` doesn't support these data types.
   *
   * @param i the index of the child
   * @param dataType the output data type of the i-th child
   * @return the converter and a boolean value to indicate whether the converter is
   *         created by using `ExpressionEncoder`.
   */
  private def scalaConverter(i: Int, dataType: DataType): (Any => Any, Boolean) = {
    val useEncoder =
      !(inputEncoders.isEmpty || // for untyped Scala UDF and Java UDF
        inputEncoders(i).isEmpty || // for types aren't supported by encoder, e.g. Any
        inputPrimitives(i)) // for primitive types

    if (useEncoder) {
      val enc = inputEncoders(i).get
      val fromRow = enc.createDeserializer()
      val converter = if (enc.isSerializedAsStructForTopLevel) {
        row: Any => fromRow(row.asInstanceOf[InternalRow])
      } else {
        val inputRow = new GenericInternalRow(1)
        value: Any => inputRow.update(0, value); fromRow(inputRow)
      }
      (converter, true)
    } else { // use CatalystTypeConverters
      (CatalystTypeConverters.createToScalaConverter(dataType), false)
    }
  }

  /**
   *  Need nulls check when there are array types with nulls in the input.
   *  This is for `https://github.com/NVIDIA/spark-rapids/issues/3942`.
   */
  override val checkNull: Boolean = children.exists(child => hasArrayWithNulls(child.dataType))

  private def hasArrayWithNulls(dt: DataType): Boolean = dt match {
    case ArrayType(et, hasNull) => hasNull || hasArrayWithNulls(et)
    case MapType(kt, vt, _) => hasArrayWithNulls(kt) || hasArrayWithNulls(vt)
    case StructType(fields) => fields.exists(f => hasArrayWithNulls(f.dataType))
    case _ => false
  }
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
          expr.outputEncoder,
          expr.udfName,
          expr.nullable,
          expr.udfDeterministic)
    })
}