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

package org.apache.spark.sql.rapids

import java.lang.invoke.SerializedLambda

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ScalaUDF, SpecializedGetters}
import org.apache.spark.sql.execution.TrampolineUtil
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, DataType, MapType, StructType}

object GpuScalaUDFMeta {
  def exprMeta: ExprRule[ScalaUDF] = GpuOverrides.expr[ScalaUDF](
    "User Defined Function, the UDF can choose to implement a RAPIDS accelerated interface " +
        "to get better performance.",
    ExprChecks.projectOnly(
      GpuUserDefinedFunction.udfTypeSig,
      TypeSig.all,
      repeatingParamCheck =
        Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
    (expr, conf, p, r) => new ExprMeta(expr, conf, p, r) {
      lazy val opRapidsFunc = GpuScalaUDF.getRapidsUDFInstance(expr.function)

      override def tagExprForGpu(): Unit = {
        if (opRapidsFunc.isEmpty && !conf.isCpuBasedUDFEnabled) {
          val udfName = expr.udfName.getOrElse("UDF")
          val udfClass = expr.function.getClass
          willNotWorkOnGpu(s"neither $udfName implemented by $udfClass provides " +
              s"a GPU implementation, nor the conf `${RapidsConf.ENABLE_CPU_BASED_UDF.key}` " +
              s"is enabled")
        }
      }
    })
}

object GpuScalaUDF {
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
