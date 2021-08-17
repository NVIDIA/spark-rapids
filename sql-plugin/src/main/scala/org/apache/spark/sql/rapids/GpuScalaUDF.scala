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

import java.lang.invoke.SerializedLambda

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuExpression, GpuOverrides, GpuUserDefinedFunction, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.GpuUserDefinedFunction.udfTypeSig

import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType

case class GpuScalaUDF(
    function: RapidsUDF,
    dataType: DataType,
    children: Seq[Expression],
    udfName: Option[String],
    nullable: Boolean,
    udfDeterministic: Boolean) extends GpuUserDefinedFunction {
  override def toString: String = s"${udfName.getOrElse("UDF")}(${children.mkString(", ")})"

  /** name of the UDF function */
  override val name: String = udfName.getOrElse("???")
}

object GpuScalaUDF {
  def exprMeta: ExprRule[ScalaUDF] = GpuOverrides.expr[ScalaUDF](
    "User Defined Function, support requires the UDF to implement a RAPIDS accelerated interface",
    ExprChecks.projectNotLambda(
      udfTypeSig,
      TypeSig.all,
      repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
    (a, conf, p, r) => new ExprMeta[ScalaUDF](a, conf, p, r) {
      override def tagExprForGpu(): Unit = {
        if (getRapidsUDFInstance(a.function).isEmpty) {
          val udfName = a.udfName.getOrElse("UDF")
          val udfClass = a.function.getClass
          willNotWorkOnGpu(s"$udfName implemented by $udfClass does not provide " +
              "a GPU implementation")
        }
      }

      override def convertToGpu(): GpuExpression = {
        GpuScalaUDF(
          getRapidsUDFInstance(a.function).get,
          a.dataType,
          childExprs.map(_.convertToGpu()),
          a.udfName,
          a.nullable,
          a.udfDeterministic)
      }
    })

  /**
   * Determine if the UDF function implements the [[com.nvidia.spark.RapidsUDF]] interface,
   * returning the instance if it does. The lambda wrapper that Spark applies to Java UDFs will be
   * inspected if necessary to locate the user's UDF instance.
   */
  def getRapidsUDFInstance(function: AnyRef): Option[RapidsUDF] = {
    function match {
      case f: RapidsUDF => Some(f)
      case f =>
        try {
          // This may be a lambda that Spark's UDFRegistration wrapped around a Java UDF instance.
          val clazz = f.getClass
          if (TrampolineUtil.getSimpleName(clazz).toLowerCase().contains("lambda")) {
            // Try to find a `writeReplace` method, further indicating it is likely a lambda
            // instance, and invoke it to serialize the lambda. Once serialized, captured arguments
            // can be examine to locate the Java UDF instance.
            // Note this relies on implementation details of Spark's UDFRegistration class.
            val writeReplace = clazz.getDeclaredMethod("writeReplace")
            writeReplace.setAccessible(true)
            val serializedLambda = writeReplace.invoke(f).asInstanceOf[SerializedLambda]
            if (serializedLambda.getCapturedArgCount == 1) {
              serializedLambda.getCapturedArg(0) match {
                case c: RapidsUDF => Some(c)
                case _ => None
              }
            } else {
              None
            }
          } else {
            None
          }
        } catch {
          case _: ClassCastException | _: NoSuchMethodException | _: SecurityException => None
        }
    }
  }
}
