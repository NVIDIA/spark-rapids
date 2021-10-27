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
import com.nvidia.spark.rapids.{DataFromReplacementRule, ExprChecks, ExprMeta, ExprRule, GpuExpression, GpuOverrides, GpuRowBasedUserDefinedFunction, GpuUserDefinedFunction, RapidsConf, RapidsMeta, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.GpuUserDefinedFunction.udfTypeSig

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ScalaUDF, SpecializedGetters}
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

case class GpuRowBasedScalaUDF(
    sparkFunc: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]],
    outputEncoder: Option[ExpressionEncoder[_]],
    udfName: Option[String],
    nullable: Boolean,
    udfDeterministic: Boolean) extends GpuRowBasedUserDefinedFunction {
  override def toString: String = s"$name(${children.mkString(", ")})"

  /** name of the UDF function */
  override val name: String = udfName.getOrElse(getClass.getSimpleName)

  /** The input `row` consists of only child columns. */
  override protected def evaluateRow(childrenRow: InternalRow): Any =
    catalystConverter(wrappedFunc(childrenRow))

  /** Unfortunately we need to copy the following code from Spark for input/output type
   * conversions between Scala type and Catalyst type.
   *
   * NOTE the expression encoder for input does not work here. So disable it for now.
   * Please find "GPU DIFF" for details.
   *
   * == Copy begin == */

  /**
   * The analyzer should be aware of Scala primitive types so as to make the
   * UDF return null if there is any null input value of these types. On the
   * other hand, Java UDFs can only have boxed types, thus this will return
   * Nil(has same effect with all false) and analyzer will skip null-handling
   * on them.
   */
  lazy val inputPrimitives: Seq[Boolean] = {
    inputEncoders.map { encoderOpt =>
      // It's possible that some of the inputs don't have a specific encoder(e.g. `Any`)
      encoderOpt.exists { encoder =>
        if (encoder.isSerializedAsStruct) {
          // struct type is not primitive
          false
        } else {
          // `nullable` is false iff the type is primitive
          !encoder.schema.head.nullable
        }
      }
    }
  }

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

    // GPU DIFF
    // Getting the errors as below when using the encoder, so disable it now.
    //   > "catalyst.analysis.UnresolvedException: Invalid call to nullable on unresolved object"
    // Tracked by https://github.com/NVIDIA/spark-rapids/issues/3924
    if (false /* useEncoder */) {
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
   * Create the converter which converts the scala data type to the catalyst data type for
   * the return data type of udf function. We'd use `ExpressionEncoder` to create the
   * converter for typed ScalaUDF only, since its the only case where we know the type tag
   * of the return data type of udf function.
   */
  private def catalystConverter: Any => Any = outputEncoder.map { enc =>
    val toRow = enc.createSerializer().asInstanceOf[Any => Any]
    if (enc.isSerializedAsStructForTopLevel) {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow]
    } else {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow].get(0, dataType)
    }
  }.getOrElse(CatalystTypeConverters.createToCatalystConverter(dataType))
  /** == Copy end == */

  /** Some refactor to simplify the Spark code of executing the function */

  /** Build an accessor for each child to read the data from a row and do type conversion */
  private lazy val childAccessors: Seq[SpecializedGetters => Any] =
    children.zipWithIndex.map { case (child, i) =>
      val accessor = InternalRow.getAccessor(child.dataType, child.nullable)
      val converter = scalaConverter(i, child.dataType)._1
      row: SpecializedGetters => converter(accessor(row, i))
    }

  private lazy val argsParser: InternalRow => Seq[Any] =
    row => children.indices.map(childAccessors(_)(row))

  // scalastyle:off line.size.limit
  /**
   * Spark Scala UDF supports at most 22 parameters.
   */
  private lazy val wrappedFunc: InternalRow => Any = {
    children.size match {
      case 0 =>
        val f = sparkFunc.asInstanceOf[() => Any]
        (row: InternalRow) => f()
      case 1 =>
        val f = sparkFunc.asInstanceOf[(Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head)
        }
      case 2 =>
        val f = sparkFunc.asInstanceOf[(Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1))
        }
      case 3 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2))
        }
      case 4 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3))
        }
      case 5 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4))
        }
      case 6 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5))
        }
      case 7 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6))
        }
      case 8 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7))
        }
      case 9 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))
        }
      case 10 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9))
        }
      case 11 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10))
        }
      case 12 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11))
        }
      case 13 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12))
        }
      case 14 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13))
        }
      case 15 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14))
        }
      case 16 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15))
        }
      case 17 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16))
        }
      case 18 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16),
            args(17))
        }
      case 19 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16),
            args(17), args(18))
        }
      case 20 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16),
            args(17), args(18), args(19))
        }
      case 21 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16),
            args(17), args(18), args(19), args(20))
        }
      case 22 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val args = argsParser(row)
          f(args.head, args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8),
            args(9), args(10), args(11), args(12), args(13), args(14), args(15), args(16),
            args(17), args(18), args(19), args(20), args(21))
        }
    }
  } // end of wrappedFunc
  // scalastyle:on line.size.limit

}

abstract class BaseScalaUDFMeta(
    expr: ScalaUDF,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends ExprMeta(expr, conf, parent, rule) {

  protected def outputEncoder: Option[ExpressionEncoder[_]]

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

  override def convertToGpu(): GpuExpression = {
    // It can come here only when at least one option as below is true.
    //   1. UDF implements a RAPIDS accelerated interface.
    //   2. The conf "spark.rapids.sql.rowBasedUDF.enabled" is enabled.
    opRapidsFunc.map { rapidsFunc =>
      GpuScalaUDF(
        rapidsFunc,
        expr.dataType,
        childExprs.map(_.convertToGpu()),
        expr.udfName,
        expr.nullable,
        expr.udfDeterministic)
    }.getOrElse {
      // This `require` is just for double check.
      require(conf.isCpuBasedUDFEnabled)
      GpuRowBasedScalaUDF(
        expr.function,
        expr.dataType,
        childExprs.map(_.convertToGpu()),
        expr.inputEncoders,
        outputEncoder,
        expr.udfName,
        expr.nullable,
        expr.udfDeterministic)
    }
  }
}

object GpuScalaUDF {
  def exprMeta30X: ExprRule[ScalaUDF] = GpuOverrides.expr[ScalaUDF](
    "User Defined Function, the UDF can choose to implement a RAPIDS accelerated interface " +
      "to get better performance.",
    ExprChecks.projectOnly(
      udfTypeSig,
      TypeSig.all,
      repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
    (a, conf, p, r) => new BaseScalaUDFMeta(a, conf, p, r) {
      override protected def outputEncoder: Option[ExpressionEncoder[_]] = None
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
