/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import CatalystExpressionBuilder.simplify
import java.nio.charset.Charset

import javassist.bytecode.{CodeIterator, Opcode}
import org.apache.spark.SparkException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


private object Repr {

  abstract class CompilerInternal(name: String) extends Expression {
    override def dataType: DataType = {
      throw new SparkException(s"Compiler internal representation of " +
          s"${name} cannot be evaluated")
    }

    override def doGenCode(ctx: codegen.CodegenContext, ev: codegen.ExprCode): codegen.ExprCode = {
      throw new SparkException(s"Cannot generate code for compiler internal " +
          s"representation of ${name}")
    }

    override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = {
      throw new SparkException(s"Compiler internal representation of " +
          s"${name} cannot be evaluated")
    }

    override def nullable: Boolean = {
      throw new SparkException(s"Compiler internal representation of " +
          s"${name} cannot be evaluated")
    }

    override def children: Seq[Expression] = {
      throw new SparkException(s"Compiler internal representation of " +
          s"${name} cannot be evaluated")
    }
  }

  // Internal representation of java.lang.StringBuilder.
  case class StringBuilder() extends CompilerInternal("java.lang.StringBuilder") {
    def invoke(methodName: String, args: List[Expression]): Expression = {
      methodName match {
        case "StringBuilder" => this
        case "append" => string = Concat(string :: args)
          this
        case "toString" => string
        case _ =>
          throw new SparkException(s"Unsupported StringBuilder op ${methodName}")
      }
    }

    var string: Expression = Literal.default(StringType)
  }

  case class DateTimeFormatter private (private[Repr] val pattern: Expression) extends CompilerInternal("java.time.format.DateTimeFormatter") {
    def invoke(methodName: String, args: List[Expression]): Expression = {
      methodName match {
        case _ =>
          throw new SparkException(s"Unsupported DateTimeFormatter op ${methodName}")
      }
    }
  }
  object DateTimeFormatter {
    private def apply(pattern: Expression): DateTimeFormatter = new DateTimeFormatter(pattern)
    def ofPattern(pattern: Expression): DateTimeFormatter = DateTimeFormatter(pattern)
  }

  case class LocalDateTime private (private val dateTime: Expression) extends CompilerInternal("java.time.LocalDateTime") {
    def invoke(methodName: String, args: List[Expression]): Expression = {
      methodName match {
        case "getYear" => Year(dateTime)
        case "getMonthValue" => Month(dateTime)
        case "getDayOfMonth" => DayOfMonth(dateTime)
        case "getHour" => Hour(dateTime)
        case "getMinute" => Minute(dateTime)
        case "getSecond" => Second(dateTime)
        case _ =>
          throw new SparkException(s"Unsupported DateTimeFormatter op ${methodName}")
      }
    }
  }
  object LocalDateTime {
    private def apply(pattern: Expression): LocalDateTime = { new LocalDateTime(pattern) }
    def parse(text: Expression, formatter: DateTimeFormatter): LocalDateTime = {
      LocalDateTime(new ParseToTimestamp(text, formatter.pattern))
    }
  }
}

/**
 *
 * @param opcode
 * @param operand
 */
case class Instruction(opcode: Int, operand: Int, instructionStr: String) extends Logging {
  def makeState(lambdaReflection: LambdaReflection, basicBlock: BB, state: State): State = {
    val st = opcode match {
      case Opcode.ALOAD_0 | Opcode.DLOAD_0 | Opcode.FLOAD_0 |
           Opcode.ILOAD_0 | Opcode.LLOAD_0 => load(state, 0)
      case Opcode.ALOAD_1 | Opcode.DLOAD_1 | Opcode.FLOAD_1 |
           Opcode.ILOAD_1 | Opcode.LLOAD_1 => load(state, 1)
      case Opcode.ALOAD_2 | Opcode.DLOAD_2 | Opcode.FLOAD_2 |
           Opcode.ILOAD_2 | Opcode.LLOAD_2 => load(state, 2)
      case Opcode.ALOAD_3 | Opcode.DLOAD_3 | Opcode.FLOAD_3 |
           Opcode.ILOAD_3 | Opcode.LLOAD_3 => load(state, 3)
      case Opcode.ALOAD | Opcode.DLOAD | Opcode.FLOAD |
           Opcode.ILOAD | Opcode.LLOAD => load(state, operand)
      case Opcode.ASTORE_0 | Opcode.DSTORE_0 | Opcode.FSTORE_0 |
           Opcode.ISTORE_0 | Opcode.LSTORE_0 => store(state, 0)
      case Opcode.ASTORE_1 | Opcode.DSTORE_1 | Opcode.FSTORE_1 |
           Opcode.ISTORE_1 | Opcode.LSTORE_1 => store(state, 1)
      case Opcode.ASTORE_2 | Opcode.DSTORE_2 | Opcode.FSTORE_2 |
           Opcode.ISTORE_2 | Opcode.LSTORE_2 => store(state, 2)
      case Opcode.ASTORE_3 | Opcode.DSTORE_3 | Opcode.FSTORE_3 |
           Opcode.ISTORE_3 | Opcode.LSTORE_3 => store(state, 3)
      case Opcode.ASTORE | Opcode.DSTORE | Opcode.FSTORE |
           Opcode.ISTORE | Opcode.LSTORE => store(state, operand)
      case Opcode.DCONST_0 | Opcode.DCONST_1 =>
        const(state, (opcode - Opcode.DCONST_0).asInstanceOf[Double])
      case Opcode.FCONST_0 | Opcode.FCONST_1 | Opcode.FCONST_2 =>
        const(state, (opcode - Opcode.FCONST_0).asInstanceOf[Float])
      case Opcode.BIPUSH | Opcode.SIPUSH =>
        const(state, operand)
      case Opcode.ICONST_M1 |
           Opcode.ICONST_0 | Opcode.ICONST_1 | Opcode.ICONST_2 |
           Opcode.ICONST_3 | Opcode.ICONST_4 | Opcode.ICONST_5 =>
        const(state, (opcode - Opcode.ICONST_0).asInstanceOf[Int])
      case Opcode.LCONST_0 | Opcode.LCONST_1 =>
        const(state, (opcode - Opcode.LCONST_0).asInstanceOf[Long])
      case Opcode.DADD | Opcode.FADD | Opcode.IADD | Opcode.LADD => binary(state, Add(_, _))
      case Opcode.DSUB | Opcode.FSUB | Opcode.ISUB | Opcode.LSUB => binary(state, Subtract(_, _))
      case Opcode.DMUL | Opcode.FMUL | Opcode.IMUL | Opcode.LMUL => binary(state, Multiply(_, _))
      case Opcode.DDIV | Opcode.FDIV => binary(state, Divide(_, _))
      case Opcode.IDIV | Opcode.LDIV => binary(state, IntegralDivide(_, _))
      case Opcode.DREM | Opcode.FREM | Opcode.IREM | Opcode.LREM => binary(state, Remainder(_, _))
      case Opcode.IAND | Opcode.LAND => binary(state, BitwiseAnd(_, _))
      case Opcode.IOR | Opcode.LOR => binary(state, BitwiseOr(_, _))
      case Opcode.IXOR | Opcode.LXOR => binary(state, BitwiseXor(_, _))
      case Opcode.ISHL | Opcode.LSHL => binary(state, ShiftLeft(_, _))
      case Opcode.ISHR | Opcode.LSHR => binary(state, ShiftRight(_, _))
      case Opcode.IUSHR | Opcode.LUSHR => binary(state, ShiftRightUnsigned(_, _))
      case Opcode.DNEG | Opcode.FNEG | Opcode.INEG | Opcode.LNEG => neg(state)
      case Opcode.DCMPL | Opcode.FCMPL => cmp(state, -1)
      case Opcode.DCMPG | Opcode.FCMPG => cmp(state, 1)
      case Opcode.LCMP => cmp(state)
      case Opcode.LDC | Opcode.LDC_W | Opcode.LDC2_W => ldc(lambdaReflection, state)
      case Opcode.DUP => dup(state)
      case Opcode.GETSTATIC => getstatic(state)
      case Opcode.NEW => newObj(lambdaReflection, state)
      // Cast instructions
      case Opcode.I2B => cast(state, ByteType)
      case Opcode.I2C =>
        throw new SparkException("Opcode.I2C unsupported: no corresponding Catalyst expression")
      case Opcode.F2D | Opcode.I2D | Opcode.L2D => cast(state, DoubleType)
      case Opcode.D2F | Opcode.I2F | Opcode.L2F => cast(state, FloatType)
      case Opcode.D2I | Opcode.F2I | Opcode.L2I => cast(state, IntegerType)
      case Opcode.D2L | Opcode.F2L | Opcode.I2L => cast(state, LongType)
      case Opcode.I2S => cast(state, ShortType)
      // Branching instructions
      // if_acmp<cond> isn't supported.
      case Opcode.IF_ICMPEQ => ifCmp(state, (x, y) => simplify(EqualTo(x, y)))
      case Opcode.IF_ICMPNE => ifCmp(state, (x, y) => simplify(Not(EqualTo(x, y))))
      case Opcode.IF_ICMPLT => ifCmp(state, (x, y) => simplify(LessThan(x, y)))
      case Opcode.IF_ICMPGE => ifCmp(state, (x, y) => simplify(GreaterThanOrEqual(x, y)))
      case Opcode.IF_ICMPGT => ifCmp(state, (x, y) => simplify(GreaterThan(x, y)))
      case Opcode.IF_ICMPLE => ifCmp(state, (x, y) => simplify(LessThanOrEqual(x, y)))
      case Opcode.IFLT => ifOp(state, x => simplify(LessThan(x, Literal(0))))
      case Opcode.IFLE => ifOp(state, x => simplify(LessThanOrEqual(x, Literal(0))))
      case Opcode.IFGT => ifOp(state, x => simplify(GreaterThan(x, Literal(0))))
      case Opcode.IFGE => ifOp(state, x => simplify(GreaterThanOrEqual(x, Literal(0))))
      case Opcode.IFEQ => ifOp(state, x => simplify(EqualTo(x, Literal(0))))
      case Opcode.IFNE => ifOp(state, x => simplify(Not(EqualTo(x, Literal(0)))))
      case Opcode.IFNULL => ifOp(state, x => simplify(IsNull(x)))
      case Opcode.IFNONNULL => ifOp(state, x => simplify(IsNotNull(x)))
      case Opcode.TABLESWITCH | Opcode.LOOKUPSWITCH => switch(state)
      case Opcode.GOTO => state
      case Opcode.IRETURN | Opcode.LRETURN | Opcode.FRETURN | Opcode.DRETURN |
           Opcode.ARETURN | Opcode.RETURN =>
        state.copy(expr = Some(state.stack.head))
      // Call instructions
      case Opcode.INVOKESTATIC =>
        invoke(lambdaReflection, state,
          (stack, n) => {
            val (args, rest) = stack.splitAt(n)
            (args.reverse, rest)
          })
      case Opcode.INVOKEVIRTUAL | Opcode.INVOKESPECIAL =>
        invoke(lambdaReflection, state,
          (stack, n) => {
            val (args, rest) = stack.splitAt(n + 1)
            (args.reverse, rest)
          })
      case _ => throw new SparkException("Unsupported instruction: " + instructionStr)
    }
    logDebug(s"[Instruction] ${instructionStr} got new state: ${st} from state: ${state}")
    st
  }

  def isReturn: Boolean = opcode match {
    case Opcode.IRETURN | Opcode.LRETURN | Opcode.FRETURN | Opcode.DRETURN |
         Opcode.ARETURN | Opcode.RETURN => true
    case _ => false
  }

  //
  // Handle instructions
  //
  private def load(state: State, localsIndex: Int): State = {
    val State(locals, stack, cond, expr) = state
    State(locals, locals(localsIndex) :: stack, cond, expr)
  }

  private def store(state: State, localsIndex: Int): State = {
    val State(locals, top :: rest, cond, expr) = state
    State(locals.updated(localsIndex, top), rest, cond, expr)
  }

  private def const(state: State, value: Any): State = {
    val State(locals, stack, cond, expr) = state
    State(locals, Literal(value) :: stack, cond, expr)
  }

  private def binary(state: State, op: (Expression, Expression) => Expression): State = {
    val State(locals, op2 :: op1 :: rest, cond, expr) = state
    State(locals, op(op1, op2) :: rest, cond, expr)
  }

  private def neg(state: State): State = {
    val State(locals, top :: rest, cond, expr) = state
    State(locals, UnaryMinus(top) :: rest, cond, expr)
  }

  private def ldc(lambdaReflection: LambdaReflection, state: State): State = {
    val State(locals, stack, cond, expr) = state
    val constant = Literal(lambdaReflection.lookupConstant(operand))
    State(locals, constant :: stack, cond, expr)
  }

  private def dup(state: State): State = {
    val State(locals, top :: rest, cond, expr) = state
    State(locals, top :: top :: rest, cond, expr)
  }

  private def newObj(lambdaReflection: LambdaReflection,
      state: State): State = {
    val typeName = lambdaReflection.lookupClassName(operand)
    if (typeName.equals("java.lang.StringBuilder")) {
      val State(locals, stack, cond, expr) = state
      State(locals, Repr.StringBuilder() :: stack, cond, expr)
    } else {
      throw new SparkException("Unsupported type for new:" + typeName)
    }
  }

  private def getstatic(state: State): State = {
    val State(locals, stack, cond, expr) = state
    State(locals, Literal(operand) :: stack, cond, expr)
  }

  private def cmp(state: State, default: Int): State = {
    val State(locals, op2 :: op1 :: rest, cond, expr) = state
    val conditional =
      If(Or(IsNaN(op1), IsNaN(op2)),
        Literal(default),
        If(GreaterThan(op1, op2),
          Literal(1),
          If(LessThan(op1, op2),
            Literal(-1),
            Literal(0))))
    State(locals, conditional :: rest, cond, expr)
  }

  private def cmp(state: State): State = {
    val State(locals, op2 :: op1 :: rest, cond, expr) = state
    val conditional =
      If(GreaterThan(op1, op2),
        Literal(1),
        If(LessThan(op1, op2),
          Literal(-1),
          Literal(0)))
    State(locals, conditional :: rest, cond, expr)
  }

  private def cast(
      state: State,
      dataType: DataType): State = {
    val State(locals, top :: rest, cond, expr) = state
    State(locals, Cast(top, dataType) :: rest, cond, expr)
  }

  private def ifCmp(state: State,
      predicate: (Expression, Expression) => Expression): State = {
    val State(locals, op2 :: op1 :: rest, cond, expr) = state
    State(locals, rest, cond, Some(predicate(op1, op2)))
  }

  private def ifOp(
      state: State,
      predicate: Expression => Expression): State = {
    val State(locals, top :: rest, cond, expr) = state
    State(locals, rest, cond, Some(predicate(top)))
  }

  private def switch(state: State): State = {
    val State(locals, top :: rest, cond, expr) = state
    State(locals, rest, cond, Some(top))
  }

  private def invoke(lambdaReflection: LambdaReflection, state: State,
      getArgs: (List[Expression], Int) =>
          (List[Expression], List[Expression])): State = {
    val State(locals, stack, cond, expr) = state
    val method = lambdaReflection.lookupBehavior(operand)
    val declaringClassName = method.getDeclaringClass.getName
    val paramTypes = method.getParameterTypes
    val (args, rest) = getArgs(stack, paramTypes.length)
    // We don't support arbitrary calls.
    // We support only some math and string methods.
    if (declaringClassName.equals("scala.math.package$")) {
      State(locals,
        mathOp(lambdaReflection, method.getName, args) :: rest,
        cond,
        expr)
    } else if (declaringClassName.equals("scala.Predef$")) {
      State(locals,
        predefOp(lambdaReflection, method.getName, args) :: rest,
        cond,
        expr)
    } else if (declaringClassName.equals("java.lang.Double")) {
      State(locals, doubleOp(method.getName, args) :: rest, cond, expr)
    } else if (declaringClassName.equals("java.lang.Float")) {
      State(locals, floatOp(method.getName, args) :: rest, cond, expr)
    } else if (declaringClassName.equals("java.lang.String")) {
      State(locals, stringOp(method.getName, args) :: rest, cond, expr)
    } else if (declaringClassName.equals("java.lang.StringBuilder")) {
      if (!args.head.isInstanceOf[Repr.StringBuilder]) {
        throw new SparkException("Internal error with StringBuilder")
      }
      val retval = args.head.asInstanceOf[Repr.StringBuilder]
          .invoke(method.getName, args.tail)
      State(locals, retval :: rest, cond, expr)
    } else if (declaringClassName.equals("java.time.format.DateTimeFormatter")) {
      State(locals, dateTimeFormatterOp(method.getName, args) :: rest, cond, expr)
    } else if (declaringClassName.equals("java.time.LocalDateTime")) {
      State(locals, localDateTimeOp(method.getName, args) :: rest, cond, expr)
    } else {
      // Other functions
      throw new SparkException(s"Unsupported instruction: ${Opcode.INVOKEVIRTUAL} ${declaringClassName}")
    }
  }

  private def checkArgs(methodName: String,
                        expectedTypes: List[DataType],
                        args: List[Expression]): Unit = {
    if (args.length != expectedTypes.length) {
      throw new SparkException(
        s"${methodName} operation expects ${expectedTypes.length} " +
            s"argument(s), including an objref, but instead got ${args.length} " +
            s"argument(s)")
    }
    args.view.zip(expectedTypes.view).foreach { case (arg, expectedType) =>
      if (arg.dataType != expectedType) {
        throw new SparkException(s"${arg.dataType} argument found for " +
            s"${methodName} where " +
            s"${expectedType} argument is expected.")
      }
    }
  }

  private def mathOp(lambdaReflection: LambdaReflection,
      methodName: String, args: List[Expression]): Expression = {
    // Math unary functions
    if (args.length != 2) {
      throw new SparkException(
        s"Unary math operation expects 1 argument and an objref, but " +
            s"instead got ${args.length - 1} arguments and an objref.")
    }
    // Make sure that the objref is scala.math.package$.
    args.head match {
      case IntegerLiteral(index) =>
        if (!lambdaReflection.lookupField(index.asInstanceOf[Int])
            .getType.getName.equals("scala.math.package$")) {
          throw new SparkException("Unsupported math function objref: " + args.head)
        }
      case _ =>
        throw new SparkException("Unsupported math function objref: " + args.head)
    }
    // Translate to Catalyst
    val arg = args.last
    methodName match {
      case "abs" => Abs(arg)
      case "acos" => Acos(arg)
      case "asin" => Asin(arg)
      case "atan" => Atan(arg)
      case "cos" => Cos(arg)
      case "cosh" => Cosh(arg)
      case "sin" => Sin(arg)
      case "tan" => Tan(arg)
      case "tanh" => Tanh(arg)
      case "ceil" => Ceil(arg)
      case "floor" => Floor(arg)
      case "exp" => Exp(arg)
      case "log" => Log(arg)
      case "log10" => Log10(arg)
      case "sqrt" => Sqrt(arg)
      case _ => throw new SparkException("Unsupported math function: " + methodName)
    }
  }

  private def predefOp(lambdaReflection: LambdaReflection,
      methodName: String, args: List[Expression]): Expression = {
    // Make sure that the objref is scala.math.package$.
    args.head match {
      case IntegerLiteral(index) =>
        if (!lambdaReflection.lookupField(index.asInstanceOf[Int])
            .getType.getName.equals("scala.Predef$")) {
          throw new SparkException("Unsupported predef function objref: " + args.head)
        }
      case _ =>
        throw new SparkException("Unsupported predef function objref: " + args.head)
    }
    // Translate to Catalyst
    methodName match {
      case "double2Double" =>
        checkArgs(methodName, List(IntegerType, DoubleType), args)
        args.last
      case "float2Float" =>
        checkArgs(methodName, List(IntegerType, FloatType), args)
        args.last
      case _ => throw new SparkException("Unsupported predef function: " + methodName)
    }
  }

 private def doubleOp(methodName: String, args: List[Expression]): Expression = {
    methodName match {
      case "isNaN" =>
        checkArgs(methodName, List(DoubleType), args)
        IsNaN(args.head)
      case _ =>
        throw new SparkException(s"Unsupported Double function: " +
            s"Double.${methodName}")
    }
  }

 private def floatOp(methodName: String, args: List[Expression]): Expression = {
    methodName match {
      case "isNaN" =>
        checkArgs(methodName, List(FloatType), args)
        IsNaN(args.head)
      case _ =>
        throw new SparkException(s"Unsupported Float function: " +
            s"Float.${methodName}")
    }
  }

 private def stringOp(methodName: String, args: List[Expression]): Expression = {
    methodName match {
      case "concat" =>
        checkArgs(methodName, List(StringType, StringType), args)
        Concat(args)
      case "contains" =>
        checkArgs(methodName, List(StringType, StringType), args)
        Contains(args.head, args.last)
      case "endsWith" =>
        checkArgs(methodName, List(StringType, StringType), args)
        EndsWith(args.head, args.last)
      case "equals" =>
        checkArgs(methodName, List(StringType, StringType), args)
        Cast(EqualNullSafe(args.head, args.last), IntegerType)
      case "equalsIgnoreCase" =>
        checkArgs(methodName, List(StringType, StringType), args)
        Cast(EqualNullSafe(Upper(args.head), Upper(args.last)), IntegerType)
      case "isEmpty" =>
        checkArgs(methodName, List(StringType), args)
        Cast(EqualTo(Length(args.head), Literal(0)), IntegerType)
      case "length" =>
        checkArgs(methodName, List(StringType), args)
        Length(args.head)
      case "startsWith" =>
        checkArgs(methodName, List(StringType, StringType), args)
        StartsWith(args.head, args.last)
      case "toLowerCase" =>
        checkArgs(methodName, List(StringType), args)
        Lower(args.head)
      case "toUpperCase" =>
        checkArgs(methodName, List(StringType), args)
        Upper(args.head)
      case "trim" =>
        checkArgs(methodName, List(StringType), args)
        StringTrim(args.head)
      case "replace" =>
        if (args.length != 3) {
          throw new SparkException(
            s"String.${methodName} operation expects 3 argument(s), " +
                s"including an objref, but instead got ${args.length} " +
                s"argument(s)")
        }
        if (args(1).dataType == StringType &&
            args(2).dataType == StringType) {
          StringReplace(args(0), args(1), args(2))
        } else if (args(1).dataType == IntegerType &&
            args(2).dataType == IntegerType) {
          StringReplace(args(0), Chr(args(1)), Chr(args(2)))
        } else {
          throw new SparkException(s"Unsupported argument type for " +
              s"String.${methodName}: " +
              s"${args(0).dataType}, " +
              s"${args(1).dataType}, and " +
              s"${args(2).dataType}")
        }
      case "substring" =>
        checkArgs(methodName, StringType :: List.fill(args.length - 1)(IntegerType), args)
        Substring(args(0),
          Add(args(1), Literal(1)),
          Subtract(if (args.length == 3) args(2) else Length(args(0)),
            args(1)))
      case "valueOf" =>
        val supportedArgs = List(BooleanType, DoubleType, FloatType,
          IntegerType, LongType)
        if (args.length != 1) {
          throw new SparkException(
            s"String.${methodName} operation expects 1 " +
                s"argument(s), including an objref, but instead got ${args.length} " +
                s"argument(s)")
        }
        if (!supportedArgs.contains(args.head.dataType)) {
          throw new SparkException(s"Unsupported argument type for " +
              s"String.${methodName}: " +
              s"${args.head.dataType}")
        }
        Cast(args.head, StringType)
      case "indexOf" =>
        if (args.length == 2) {
          if (args(1).dataType == StringType) {
            Subtract(StringInstr(args(0), args(1)), Literal(1))
          } else {
            throw new SparkException(s"Unsupported argument type for " +
                s"String.${methodName}: " +
                s"${args(0).dataType} and " +
                s"${args(1).dataType}")
          }
        } else if (args.length == 3) {
          if (args(1).dataType == StringType &&
              args(2).dataType == IntegerType) {
            Subtract(StringLocate(args(1), args(0), Add(args(2), Literal(1))),
              Literal(1))
          } else {
            throw new SparkException(s"Unsupported argument type for " +
                s"String.${methodName}: " +
                s"${args(0).dataType}, " +
                s"${args(1).dataType}, and " +
                s"${args(2).dataType}")
          }
        } else {
          throw new SparkException(
            s"String.${methodName} operation expects 2 or 3 argument(s), " +
                s"including an objref, but instead got ${args.length} " +
                s"argument(s)")
        }
      case "replaceAll" =>
        checkArgs(methodName, List(StringType, StringType, StringType), args)
        RegExpReplace(args(0), args(1), args(2))
      case "split" =>
        if (args.length == 2) {
          checkArgs(methodName, List(StringType, StringType), args)
          StringSplit(args(0), args(1), Literal(-1))
        } else if (args.length == 3) {
          checkArgs(methodName, List(StringType, StringType, IntegerType), args)
          StringSplit(args(0), args(1), args(2))
        } else {
          throw new SparkException(
            s"String.${methodName} operation expects 2 or 3 argument(s), " +
                s"including an objref, but instead got ${args.length} " +
                s"argument(s)")
        }
      case "getBytes" =>
        if (args.length == 1) {
          checkArgs(methodName, List(StringType), args)
          Encode(args.head, Literal(Charset.defaultCharset.toString))
        } else if (args.length == 2) {
          checkArgs(methodName, List(StringType, StringType), args)
          Encode(args.head, args.last)
        } else {
          throw new SparkException(
            s"String.${methodName} operation expects 1 or 2 argument(s), " +
                s"including an objref, but instead got ${args.length} " +
                s"argument(s)")
        }
      case _ =>
        throw new SparkException(s"Unsupported string function: " +
            s"String.${methodName}")
    }
  }

  private def dateTimeFormatterOp(methodName: String, args: List[Expression]): Expression = {
    def checkPattern(pattern: String): Boolean = {
      pattern.foldLeft(false){
        case (escapedText, '\'') => !escapedText
        case (false, c) if "VzOXxZ".exists(_ == c) =>
          // The pattern isn't timezone agnostic.
          throw new SparkException("Unsupported pattern: " +
            "only timezone agnostic patterns are supported")
        case (escapedText, _) => escapedText
      }
    }
    methodName match {
      case "ofPattern" =>
        checkArgs(methodName, List(StringType), args)
        // The pattern needs to be known at compile time as we need to check
        // whether the pattern is timezone agnostic.  If it isn't, it needs
        // to fall back to JVM.
        args.head match {
          case StringLiteral(pattern) =>
            checkPattern(pattern)
            Repr.DateTimeFormatter.ofPattern(args.head)
          case _ =>
            // The pattern isn't known at compile time.
            throw new SparkException("Unsupported pattern: only string literals are supported")
        }
      case _ =>
        throw new SparkException(s"Unsupported function: " +
            s"DateTimeFormatter.${methodName}")
    }
  }

  private def localDateTimeOp(methodName: String, args: List[Expression]): Expression = {
    methodName match {
      case "parse" =>
        checkArgs(methodName, List(StringType), List(args.head))
        if (!args.last.isInstanceOf[Repr.DateTimeFormatter]) {
          throw new SparkException("Unexpected argument for LocalDateTime.parse")
        }
        Repr.LocalDateTime.parse(args.head, args.last.asInstanceOf[Repr.DateTimeFormatter])
      case "getYear" | "getMonthValue" | "getDayOfMonth" |
           "getHour" | "getMinute" | "getSecond" =>
        args.head.asInstanceOf[Repr.LocalDateTime].invoke(methodName, args.tail)
      case _ =>
        throw new SparkException(s"Unsupported function: " +
            s"DateTimeFormatter.${methodName}")
    }
  }
}

/**
 * Ultimately, every opcode will have to be covered here.
 */
object Instruction {
  def apply(codeIterator: CodeIterator, offset: Int, instructionStr: String): Instruction = {
    val opcode: Int = codeIterator.byteAt(offset)
    val operand: Int = opcode match {
      case Opcode.ALOAD | Opcode.DLOAD | Opcode.FLOAD |
           Opcode.ILOAD | Opcode.LLOAD | Opcode.LDC |
           Opcode.ASTORE | Opcode.DSTORE | Opcode.FSTORE |
           Opcode.ISTORE | Opcode.LSTORE =>
        codeIterator.byteAt(offset + 1)
      case Opcode.BIPUSH =>
        codeIterator.signedByteAt(offset + 1)
      case Opcode.LDC_W | Opcode.LDC2_W | Opcode.NEW |
           Opcode.INVOKESTATIC | Opcode.INVOKEVIRTUAL | Opcode.INVOKEINTERFACE |
           Opcode.INVOKESPECIAL | Opcode.GETSTATIC =>
        codeIterator.u16bitAt(offset + 1)
      case Opcode.GOTO |
           Opcode.IFEQ | Opcode.IFNE | Opcode.IFLT |
           Opcode.IFGE | Opcode.IFGT | Opcode.IFLE |
           Opcode.IFNULL | Opcode.IFNONNULL |
           Opcode.SIPUSH =>
        codeIterator.s16bitAt(offset + 1)
      case _ => 0
    }
    Instruction(opcode, operand, instructionStr)
  }
}
