/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import java.io.{File, FileOutputStream}
import java.time.ZoneId

import ai.rapids.cudf.DType

import org.apache.spark.sql.catalyst.expressions.{Attribute, CaseWhen, Expression, UnaryExpression, WindowSpecDefinition}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/**
 * The level of support that the plugin has for a given type.  Used for documentation generation.
 */
sealed abstract class SupportLevel {
  def htmlTag: String
}

/**
 * N/A neither spark nor the plugin supports this.
 */
object NotApplicable extends SupportLevel {
  override def htmlTag: String = "<td> </td>"
}

/**
 * Spark supports this but the plugin does not.
 */
object NotSupported extends SupportLevel {
  override def htmlTag: String = "<td><b>NS</b></td>"
}

/**
 * Both Spark and the plugin support this.
 * @param asterisks true if we need to include an asterisks because for Decimal or Timestamp
 *                  types because they are not 100% supported.
 */
class Supported(val asterisks: Boolean = false) extends SupportLevel {
  override def htmlTag: String =
    if (asterisks) {
      "<td>S*</td>"
    } else {
      "<td>S</td>"
    }
}

/**
 * The plugin partially supports this type.
 * @param asterisks true if we need to include an asterisks for Decimal or Timestamp types because
 *                  they re not 100% supported.
 * @param missingNestedTypes nested types that are not supported
 * @param needsLitWarning true if we need to warn that we only support a literal value when Spark
 *                        does not.
 * @param note any other notes we want to include about not complete support.
 */
class PartiallySupported(
    val asterisks: Boolean = false,
    val missingNestedTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    val needsLitWarning: Boolean = false,
    val note: Option[String] = None) extends SupportLevel {
  override def htmlTag: String = {
    val typeStr = if (missingNestedTypes.isEmpty) {
      None
    } else {
      Some("missing nested " + missingNestedTypes.mkString(", "))
    }
    val litOnly = if (needsLitWarning) {
      Some("Literal value only")
    } else {
      None
    }
    val extraInfo = (note.toSeq ++ litOnly.toSeq ++ typeStr.toSeq).mkString("; ")
    if (asterisks) {
      "<td><em>PS* (" + extraInfo + ")</em></td>"
    } else {
      "<td><em>PS (" + extraInfo + ")</em></td>"
    }
  }
}

/**
 * The Supported Types. The TypeSig API should be preferred for this, except in a few cases when
 * TypeSig asks for a TypeEnum.
 */
object TypeEnum extends Enumeration {
  type TypeEnum = Value

  val BOOLEAN: Value = Value
  val BYTE: Value = Value
  val SHORT: Value = Value
  val INT: Value = Value
  val LONG: Value = Value
  val FLOAT: Value = Value
  val DOUBLE: Value = Value
  val DATE: Value = Value
  val TIMESTAMP: Value = Value
  val STRING: Value = Value
  val DECIMAL: Value = Value
  val NULL: Value = Value
  val BINARY: Value = Value
  val CALENDAR: Value = Value
  val ARRAY: Value = Value
  val MAP: Value = Value
  val STRUCT: Value = Value
  val UDT: Value = Value
}

/**
 * A type signature.  This is a bit limited in what it supports right now, but can express
 * a set of base types and a separate set of types that can be nested under the base types.
 * It can also express if a particular base type has to be a literal or not.
 */
final class TypeSig private(
    private val initialTypes: TypeEnum.ValueSet,
    private val nestedTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    private val litOnlyTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    private val notes: Map[TypeEnum.Value, String] = Map.empty) {

  import TypeSig._

  /**
   * Add a literal restriction to the signature
   * @param dataType the type that has to be literal.  Will be added if it does not already exist.
   * @return the new signature.
   */
  def withLit(dataType: TypeEnum.Value): TypeSig = {
    val it = initialTypes + dataType
    val lt = litOnlyTypes + dataType
    new TypeSig(it, nestedTypes, lt, notes)
  }

  /**
   * All currently supported types can only be literal values.
   * @return the new signature.
   */
  def withAllLit(): TypeSig = {
    // don't need to combine initialTypes with litOnlyTypes because litOnly should be a subset
    new TypeSig(initialTypes, nestedTypes, initialTypes, notes)
  }

  /**
   * Combine two type signatures together.  Base types and nested types will be the union of
   * both as will limitations on literal values.
   * @param other what to combine with.
   * @return the new signature
   */
  def + (other: TypeSig): TypeSig = {
    val it = initialTypes ++ other.initialTypes
    val nt = nestedTypes ++ other.nestedTypes
    val lt = litOnlyTypes ++ other.litOnlyTypes
    // TODO nested types is not always going to do what you want, so we might want to warn
    val nts = notes ++ other.notes
    new TypeSig(it, nt, lt, nts)
  }

  /**
   * Remove a type signature. The reverse of +
   * @param other what to remove
   * @return the new signature
   */
  def - (other: TypeSig): TypeSig = {
    val it = initialTypes -- other.initialTypes
    val nt = nestedTypes -- other.nestedTypes
    val lt = litOnlyTypes -- other.litOnlyTypes
    val nts = notes -- other.notes.keySet
    new TypeSig(it, nt, lt, nts)
  }

  /**
   * Add nested types to this type signature. Note that these do not stack so if nesting has
   * nested types too they are ignored.
   * @param nesting the basic types to add.
   * @return the new type signature
   */
  def nested(nesting: TypeSig): TypeSig = {
    new TypeSig(initialTypes, nestedTypes ++ nesting.initialTypes, litOnlyTypes, notes)
  }

  /**
   * Update this type signature to be nested with the initial types too.
   * @return the update type signature
   */
  def nested(): TypeSig = {
    new TypeSig(initialTypes, initialTypes ++ nestedTypes, litOnlyTypes, notes)
  }

  /**
   * Add a note about a given type that marks it as partially supported.
   * @param dataType the type this note is for.
   * @param note the note itself
   * @return the updated TypeSignature.
   */
  def withPsNote(dataType: TypeEnum.Value, note: String): TypeSig = {
    new TypeSig(initialTypes + dataType, nestedTypes, litOnlyTypes, notes.+((dataType, note)))
  }

  private def isSupportedType(dataType: TypeEnum.Value): Boolean = initialTypes.contains(dataType)

  /**
   * Given an expression tag the associated meta for it to be supported or not.
   * @param meta the meta that gets marked for support or not.
   * @param expr the expression to check against.
   * @param name the name of the expression (typically a parameter name)
   */
  def tagExprParam(meta: RapidsMeta[_, _, _], expr: Expression, name: String): Unit = {
    // This is for a parameter so skip it if there is no data type for the expression
    getDataType(expr).foreach { dt =>
        val allowDecimal = meta.conf.decimalTypeEnabled
      if (!isSupportedByPlugin(dt, allowDecimal)) {
        meta.willNotWorkOnGpu(s"expression ${expr.getClass.getSimpleName} $expr " +
            s"produces an unsupported type $dt")
      } else if (isLitOnly(dt) && !GpuOverrides.isLit(expr)) {
        meta.willNotWorkOnGpu(s"$name only supports $dt if it is a literal value")
      }
    }
  }

  /**
   * Check if this type is supported by the plugin or not.
   * @param dataType the data type to be checked
   * @param allowDecimal if decimal support is enabled
   * @return true if it is allowed else false.
   */
  def isSupportedByPlugin(dataType: DataType, allowDecimal: Boolean): Boolean =
    isSupportedByPlugin(initialTypes, dataType, allowDecimal)

  private [this] def isLitOnly(dataType: DataType): Boolean = dataType match {
    case BooleanType => litOnlyTypes.contains(TypeEnum.BOOLEAN)
    case ByteType => litOnlyTypes.contains(TypeEnum.BYTE)
    case ShortType => litOnlyTypes.contains(TypeEnum.SHORT)
    case IntegerType => litOnlyTypes.contains(TypeEnum.INT)
    case LongType => litOnlyTypes.contains(TypeEnum.LONG)
    case FloatType => litOnlyTypes.contains(TypeEnum.FLOAT)
    case DoubleType => litOnlyTypes.contains(TypeEnum.DOUBLE)
    case DateType => litOnlyTypes.contains(TypeEnum.DATE)
    case TimestampType => litOnlyTypes.contains(TypeEnum.TIMESTAMP)
    case StringType => litOnlyTypes.contains(TypeEnum.STRING)
    case _: DecimalType => litOnlyTypes.contains(TypeEnum.DECIMAL)
    case NullType => litOnlyTypes.contains(TypeEnum.NULL)
    case BinaryType => litOnlyTypes.contains(TypeEnum.BINARY)
    case CalendarIntervalType => litOnlyTypes.contains(TypeEnum.CALENDAR)
    case _: ArrayType => litOnlyTypes.contains(TypeEnum.ARRAY)
    case _: MapType => litOnlyTypes.contains(TypeEnum.MAP)
    case _: StructType => litOnlyTypes.contains(TypeEnum.STRUCT)
    case _ => false
  }

  def isSupportedBySpark(dataType: DataType): Boolean =
    isSupportedBySpark(initialTypes, dataType)

  private[this] def isSupportedBySpark(check: TypeEnum.ValueSet, dataType: DataType): Boolean =
    dataType match {
      case BooleanType => check.contains(TypeEnum.BOOLEAN)
      case ByteType => check.contains(TypeEnum.BYTE)
      case ShortType => check.contains(TypeEnum.SHORT)
      case IntegerType => check.contains(TypeEnum.INT)
      case LongType => check.contains(TypeEnum.LONG)
      case FloatType => check.contains(TypeEnum.FLOAT)
      case DoubleType => check.contains(TypeEnum.DOUBLE)
      case DateType => check.contains(TypeEnum.DATE)
      case TimestampType => check.contains(TypeEnum.TIMESTAMP)
      case StringType => check.contains(TypeEnum.STRING)
      case _: DecimalType => check.contains(TypeEnum.DECIMAL)
      case NullType => check.contains(TypeEnum.NULL)
      case BinaryType => check.contains(TypeEnum.BINARY)
      case CalendarIntervalType => check.contains(TypeEnum.CALENDAR)
      case ArrayType(elementType, _) if check.contains(TypeEnum.ARRAY) =>
        isSupportedBySpark(nestedTypes, elementType)
      case MapType(keyType, valueType, _) if check.contains(TypeEnum.MAP) =>
        isSupportedBySpark(nestedTypes, keyType) &&
            isSupportedBySpark(nestedTypes, valueType)
      case StructType(fields) if check.contains(TypeEnum.STRUCT) =>
        fields.map(_.dataType).forall { t =>
          isSupportedBySpark(nestedTypes, t)
        }
      case _ => false
    }

  private[this] def isSupportedByPlugin(
      check: TypeEnum.ValueSet,
      dataType: DataType,
      allowDecimal: Boolean): Boolean =
    dataType match {
      case BooleanType => check.contains(TypeEnum.BOOLEAN)
      case ByteType => check.contains(TypeEnum.BYTE)
      case ShortType => check.contains(TypeEnum.SHORT)
      case IntegerType => check.contains(TypeEnum.INT)
      case LongType => check.contains(TypeEnum.LONG)
      case FloatType => check.contains(TypeEnum.FLOAT)
      case DoubleType => check.contains(TypeEnum.DOUBLE)
      case DateType => check.contains(TypeEnum.DATE)
      case TimestampType if check.contains(TypeEnum.TIMESTAMP) =>
          ZoneId.systemDefault().normalized() == GpuOverrides.UTC_TIMEZONE_ID
      case StringType => check.contains(TypeEnum.STRING)
      case dt: DecimalType if check.contains(TypeEnum.DECIMAL) && allowDecimal =>
          dt.precision <= DType.DECIMAL64_MAX_PRECISION
      case NullType => check.contains(TypeEnum.NULL)
      case BinaryType => check.contains(TypeEnum.BINARY)
      case CalendarIntervalType => check.contains(TypeEnum.CALENDAR)
      case ArrayType(elementType, _) if check.contains(TypeEnum.ARRAY) =>
        isSupportedByPlugin(nestedTypes, elementType, allowDecimal)
      case MapType(keyType, valueType, _) if check.contains(TypeEnum.MAP) =>
        isSupportedByPlugin(nestedTypes, keyType, allowDecimal) &&
            isSupportedByPlugin(nestedTypes, valueType, allowDecimal)
      case StructType(fields) if check.contains(TypeEnum.STRUCT) =>
        fields.map(_.dataType).forall { t =>
          isSupportedByPlugin(nestedTypes, t, allowDecimal)
        }
      case _ => false
    }

  def areAllSupportedByPlugin(types: Seq[DataType], allowDecimal: Boolean): Boolean =
    types.forall(isSupportedByPlugin(_, allowDecimal))

  /**
   * Get the level of support for a given type compared to what Spark supports.
   * Used for documentation.
   */
  def getSupportLevel(dataType: TypeEnum.Value, allowed: TypeSig): SupportLevel = {
    if (!allowed.isSupportedType(dataType)) {
      NotApplicable
    } else if (!isSupportedType(dataType)) {
      NotSupported
    } else {
      val note = notes.get(dataType)
      val needsLitWarning = litOnlyTypes.contains(dataType) &&
          !allowed.litOnlyTypes.contains(dataType)
      val needsAsterisks = dataType == TypeEnum.DECIMAL || dataType == TypeEnum.TIMESTAMP
      dataType match {
        case TypeEnum.ARRAY | TypeEnum.MAP | TypeEnum.STRUCT =>
          val needsAsterisksFromSubTypes = nestedTypes.contains(TypeEnum.DECIMAL) ||
              nestedTypes.contains(TypeEnum.TIMESTAMP)
          val subTypesMissing = allowed.nestedTypes -- nestedTypes
          if (subTypesMissing.isEmpty && note.isEmpty && !needsLitWarning) {
            new Supported(needsAsterisks || needsAsterisksFromSubTypes)
          } else {
            new PartiallySupported(needsAsterisks || needsAsterisksFromSubTypes,
              missingNestedTypes = subTypesMissing,
              needsLitWarning = needsLitWarning,
              note = note)
          }
        case _ if note.isDefined || needsLitWarning =>
          new PartiallySupported(needsAsterisks, needsLitWarning = needsLitWarning, note = note)
        case _ =>
          new Supported(needsAsterisks)
      }
    }
  }
}

object TypeSig {
  /**
   * Create a TypeSig that only supports a literal of the given type.
   */
  def lit(dataType: TypeEnum.Value): TypeSig =
    TypeSig.none.withLit(dataType)

  /**
   * Create a TypeSig that has partial support for the given type.
   */
  def psNote(dataType: TypeEnum.Value, note: String): TypeSig =
    TypeSig.none.withPsNote(dataType, note)

  /**
   * All types nested and not nested
   */
  val all: TypeSig = new TypeSig(TypeEnum.values, TypeEnum.values)

  /**
   * No types supported at all
   */
  val none: TypeSig = new TypeSig(TypeEnum.ValueSet())

  val BOOLEAN: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.BOOLEAN))
  val BYTE: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.BYTE))
  val SHORT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.SHORT))
  val INT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.INT))
  val LONG: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.LONG))
  val FLOAT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.FLOAT))
  val DOUBLE: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.DOUBLE))
  val DATE: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.DATE))
  val TIMESTAMP: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.TIMESTAMP))
  val STRING: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.STRING))
  val DECIMAL: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.DECIMAL))
  val NULL: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.NULL))
  val BINARY: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.BINARY))
  val CALENDAR: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.CALENDAR))
  /**
   * ARRAY type support, but not very useful on its own because no nested types under
   * it are supported
   */
  val ARRAY: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.ARRAY))
  /**
   * MAP type support, but not very useful on its own because no nested types under
   * it are supported
   */
  val MAP: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.MAP))
  /**
   * STRUCT type support, but only matches empty structs unless you add nested types to it.
   */
  val STRUCT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.STRUCT))
  /**
   * User Defined Type (We don't support these in the plugin yet)
   */
  val UDT: TypeSig = new TypeSig(TypeEnum.ValueSet(TypeEnum.UDT))

  /**
   * A signature for types that are generally supported by the plugin/CUDF. Please make sure to
   * check what Spark actually supports instead of blindly using this in a signature.
   */
  val commonCudfTypes: TypeSig = BOOLEAN + BYTE + SHORT + INT + LONG + FLOAT + DOUBLE + DATE +
      TIMESTAMP + STRING

  /**
   * All floating point types
   */
  val fp: TypeSig = FLOAT + DOUBLE

  /**
   * All integer types
   */
  val integral: TypeSig = BYTE + SHORT + INT + LONG

  /**
   * All numeric types fp + integral + DECIMAL
   */
  val numeric: TypeSig = integral + fp + DECIMAL

  /**
   * All values that correspond to Spark's AtomicType
   */
  val atomics: TypeSig = numeric + BINARY + BOOLEAN + DATE + STRING + TIMESTAMP

  /**
   * numeric + CALENDAR
   */
  val numericAndInterval: TypeSig = numeric + CALENDAR

  /**
   * All types that Spark supports sorting/ordering on (really everything but MAP)
   */
  val orderable: TypeSig = (BOOLEAN + BYTE + SHORT + INT + LONG + FLOAT + DOUBLE + DATE +
      TIMESTAMP + STRING + DECIMAL + NULL + BINARY + CALENDAR + ARRAY + STRUCT + UDT).nested()

  /**
   * Different types of Pandas UDF support different sets of output type. Please refer to
   *   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
   * for more details.
   *
   * It is impossible to specify the exact type signature for each Pandas UDF type in a single
   * expression 'PythonUDF'.
   *
   * So here comes the union of all the sets of supported type, to cover all the cases.
   */
  val unionOfPandasUdfOut = (commonCudfTypes + BINARY + DECIMAL + NULL + ARRAY + MAP).nested() +
      STRUCT

  def getDataType(expr: Expression): Option[DataType] = {
    try {
      Some(expr.dataType)
    } catch {
      case _: java.lang.UnsupportedOperationException =>None
    }
  }
}

abstract class TypeChecks[RET] {
  def tag(meta: RapidsMeta[_, _, _]): Unit

  def support(dataType: TypeEnum.Value): RET

  val shown: Boolean = true

  private def stringifyTypeAttributeMap(groupedByType: Map[DataType, Set[String]]): String = {
    groupedByType.map { case (dataType, nameSet) =>
      dataType + " " + nameSet.mkString("[", ", ", "]")
    }.mkString(", ")
  }

  protected def tagUnsupportedTypes(
    meta: RapidsMeta[_, _, _],
    sig: TypeSig,
    allowDecimal: Boolean,
    fields: Seq[StructField],
    msgFormat: String
    ): Unit = {
    val unsupportedTypes: Map[DataType, Set[String]] = fields
      .filterNot(attr => sig.isSupportedByPlugin(attr.dataType, allowDecimal))
      .groupBy(_.dataType)
      .mapValues(_.map(_.name).toSet)

    if (unsupportedTypes.nonEmpty) {
      meta.willNotWorkOnGpu(msgFormat.format(stringifyTypeAttributeMap(unsupportedTypes)))
    }
  }
}

/**
 * Checks a single parameter TypeSig
 */
case class ParamCheck(name: String, cudf: TypeSig, spark: TypeSig)

/**
 * Checks the type signature for a parameter that repeats (Can only be used at the end of a list
 * of parameters)
 */
case class RepeatingParamCheck(name: String, cudf: TypeSig, spark: TypeSig)

/**
 * Checks an expression that have input parameters and a single output.  This is intended to be
 * given for a specific ExpressionContext. If your expression does not meet this pattern you may
 * need to create a custom ExprChecks instance.
 */
case class ContextChecks(
    outputCheck: TypeSig,
    sparkOutputSig: TypeSig,
    paramCheck: Seq[ParamCheck] = Seq.empty,
    repeatingParamCheck: Option[RepeatingParamCheck] = None)
    extends TypeChecks[Map[String, SupportLevel]] {

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    import TypeSig.getDataType
    val expr = meta.wrapped.asInstanceOf[Expression]
    val dt = getDataType(expr)
    if (dt.isEmpty) {
      if (!meta.asInstanceOf[BaseExprMeta[_]].ignoreUnsetDataTypes) {
        meta.willNotWorkOnGpu(s"expression ${expr.getClass.getSimpleName} $expr " +
            s" does not have a corresponding dataType.")
      }
    } else {
      if (!outputCheck.isSupportedByPlugin(dt.get, meta.conf.decimalTypeEnabled)) {
        meta.willNotWorkOnGpu(s"expression ${expr.getClass.getSimpleName} $expr " +
            s"produces an unsupported type ${dt.get}")
      }
    }

    val children = expr.children.toArray
    val fixedChecks = paramCheck.toArray
    assert (fixedChecks.length <= children.length,
      s"${expr.getClass.getSimpleName} expected at least ${fixedChecks.length} but " +
          s"found ${children.length}")
    fixedChecks.zipWithIndex.foreach { case (check, i) =>
      check.cudf.tagExprParam(meta, children(i), check.name)
    }
    if (repeatingParamCheck.isEmpty) {
      assert(fixedChecks.length == children.length,
        s"${expr.getClass.getSimpleName} expected ${fixedChecks.length} but " +
            s"found ${children.length}")
    } else {
      val check = repeatingParamCheck.get
      (fixedChecks.length until children.length).foreach { i =>
        check.cudf.tagExprParam(meta, children(i), check.name)
      }
    }
  }

  override def support(dataType: TypeEnum.Value): Map[String, SupportLevel] = {
    val fixed = paramCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))
    val variable = repeatingParamCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))
    val output = ("result", outputCheck.getSupportLevel(dataType, sparkOutputSig))

    (fixed ++ variable ++ Seq(output)).toMap
  }
}

/**
 * Checks for either a read or a write of a given file format.
 */
class FileFormatChecks private (
    sig: TypeSig,
    sparkSig: TypeSig)
    extends TypeChecks[SupportLevel] {

  def tag(meta: RapidsMeta[_, _, _],
      schema: StructType,
      fileType: FileFormatType,
      op: FileFormatOp): Unit = {
    val allowDecimal = meta.conf.decimalTypeEnabled
    tagUnsupportedTypes(meta, sig, allowDecimal, schema.fields,
      s"unsupported data types %s in $op for $fileType")
  }

  override def support(dataType: TypeEnum.Value): SupportLevel =
    sig.getSupportLevel(dataType, sparkSig)

  override def tag(meta: RapidsMeta[_, _, _]): Unit =
    throw new IllegalStateException("Internal Error not supported")
}

object FileFormatChecks {
  /**
   * File format checks with separate read and write signatures for cudf.
   */
  def apply(
      cudfRead: TypeSig,
      cudfWrite: TypeSig,
      sparkSig: TypeSig): Map[FileFormatOp, FileFormatChecks] = Map(
    (ReadFileOp, new FileFormatChecks(cudfRead, sparkSig)),
    (WriteFileOp, new FileFormatChecks(cudfWrite, sparkSig))
  )

  /**
   * File format checks where read and write have the same signature for cudf.
   */
  def apply(
      cudfReadWrite: TypeSig,
      sparkSig: TypeSig): Map[FileFormatOp, FileFormatChecks] =
    apply(cudfReadWrite, cudfReadWrite, sparkSig)

  def tag(meta: RapidsMeta[_, _, _],
      schema: StructType,
      fileType: FileFormatType,
      op: FileFormatOp): Unit = {
    GpuOverrides.fileFormats(fileType)(op).tag(meta, schema, fileType, op)
  }
}

/**
 * Checks the input and output types supported by a SparkPlan node. We don't currently separate
 * input checks from output checks.  We can add this in if something needs it.
 */
class ExecChecks private(
    check: TypeSig,
    sparkSig: TypeSig,
    override val shown: Boolean = true)
    extends TypeChecks[SupportLevel] {

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val plan = meta.wrapped.asInstanceOf[SparkPlan]
    val allowDecimal = meta.conf.decimalTypeEnabled

    // expression.toString to capture ids in not-on-GPU tags
    def toStructField(a: Attribute) = StructField(name = a.toString(), dataType = a.dataType)

    tagUnsupportedTypes(meta, check, allowDecimal, plan.output.map(toStructField),
      "unsupported data types in output: %s")
    tagUnsupportedTypes(meta, check, allowDecimal,
      plan.children.flatMap(_.output.map(toStructField)),
      "unsupported data types in input: %s")
  }

  override def support(dataType: TypeEnum.Value): SupportLevel =
    check.getSupportLevel(dataType, sparkSig)
}

/**
 * gives users an API to create ExecChecks.
 */
object ExecChecks {
  def apply(check: TypeSig, sparkSig: TypeSig) : ExecChecks = new ExecChecks(check, sparkSig)

  def hiddenHack() = new ExecChecks(TypeSig.all, TypeSig.all, shown = false)
}

/**
 * Base class all Partition checks must follow
 */
abstract class PartChecks extends TypeChecks[Map[String, SupportLevel]]

case class PartChecksImpl(
    paramCheck: Seq[ParamCheck] = Seq.empty,
    repeatingParamCheck: Option[RepeatingParamCheck] = None)
    extends PartChecks {

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val part = meta.wrapped
    val children = meta.childExprs.map(_.wrapped.asInstanceOf[Expression]).toArray

    val fixedChecks = paramCheck.toArray
    assert (fixedChecks.length <= children.length,
      s"${part.getClass.getSimpleName} expected at least ${fixedChecks.length} but " +
          s"found ${children.length}")
    fixedChecks.zipWithIndex.foreach { case (check, i) =>
      check.cudf.tagExprParam(meta, children(i), check.name)
    }
    if (repeatingParamCheck.isEmpty) {
      assert(fixedChecks.length == children.length,
        s"${part.getClass.getSimpleName} expected ${fixedChecks.length} but " +
            s"found ${children.length}")
    } else {
      val check = repeatingParamCheck.get
      (fixedChecks.length until children.length).foreach { i =>
        check.cudf.tagExprParam(meta, children(i), check.name)
      }
    }
  }

  override def support(dataType: TypeEnum.Value): Map[String, SupportLevel] = {
    val fixed = paramCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))
    val variable = repeatingParamCheck.map(check =>
      (check.name, check.cudf.getSupportLevel(dataType, check.spark)))

    (fixed ++ variable).toMap
  }
}

object PartChecks {
  def apply(repeatingParamCheck: RepeatingParamCheck): PartChecks =
    PartChecksImpl(Seq.empty, Some(repeatingParamCheck))

  def apply(): PartChecks = PartChecksImpl()
}

/**
 * Base class all Expression checks must follow.
 */
abstract class ExprChecks extends TypeChecks[Map[ExpressionContext, Map[String, SupportLevel]]]

case class ExprChecksImpl(contexts: Map[ExpressionContext, ContextChecks])
    extends ExprChecks {
  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    val checks = contexts.get(context)
    if (checks.isEmpty) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      checks.get.tag(meta)
    }
  }

  override def support(
      dataType: TypeEnum.Value): Map[ExpressionContext, Map[String, SupportLevel]] = {
    contexts.map {
      case (expContext: ExpressionContext, check: ContextChecks) =>
        (expContext, check.support(dataType))
    }
  }
}

/**
 * This is specific to CaseWhen, because it does not follow the typical parameter convention.
 */
object CaseWhenCheck extends ExprChecks {
  val check: TypeSig = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL +
    TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL)
  val sparkSig: TypeSig = TypeSig.all

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      val cw = meta.wrapped.asInstanceOf[CaseWhen]
      cw.branches.foreach {
        case (pred, value) =>
          TypeSig.BOOLEAN.tagExprParam(meta, pred, "predicate")
          check.tagExprParam(meta, value, "value")
      }
      cw.elseValue.foreach(e => check.tagExprParam(meta, e, "else"))
    }
  }

  override def support(dataType: TypeEnum.Value):
    Map[ExpressionContext, Map[String, SupportLevel]] = {
    val projectSupport = check.getSupportLevel(dataType, sparkSig)
    val lambdaSupport = TypeSig.none.getSupportLevel(dataType, sparkSig)
    val projectPredSupport = TypeSig.BOOLEAN.getSupportLevel(dataType, TypeSig.BOOLEAN)
    val lambdaPredSupport = TypeSig.none.getSupportLevel(dataType, TypeSig.BOOLEAN)
    Map((ProjectExprContext,
        Map(
          ("predicate", projectPredSupport),
          ("value", projectSupport),
          ("result", projectSupport))),
      (LambdaExprContext,
          Map(
            ("predicate", lambdaPredSupport),
            ("value", lambdaSupport),
            ("result", lambdaSupport))))
  }
}

/**
 * This is specific to WidowSpec, because it does not follow the typical parameter convention.
 */
object WindowSpecCheck extends ExprChecks {
  val check: TypeSig = TypeSig.commonCudfTypes + TypeSig.DECIMAL
  val sparkSig: TypeSig = TypeSig.all

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      val win = meta.wrapped.asInstanceOf[WindowSpecDefinition]
      win.partitionSpec.foreach(e => check.tagExprParam(meta, e, "partition"))
      win.orderSpec.foreach(e => check.tagExprParam(meta, e, "order"))
    }
  }

  override def support(dataType: TypeEnum.Value):
  Map[ExpressionContext, Map[String, SupportLevel]] = {
    val projectSupport = check.getSupportLevel(dataType, sparkSig)
    Map((ProjectExprContext,
        Map(
          ("partition", projectSupport),
          ("value", projectSupport),
          ("result", projectSupport))))
  }
}

/**
 * A check for CreateNamedStruct.  The parameter values alternate between one type and another.
 * If this pattern shows up again we can make this more generic at that point.
 */
object CreateNamedStructCheck extends ExprChecks {
  val nameSig: TypeSig = TypeSig.lit(TypeEnum.STRING)
  val sparkNameSig: TypeSig = TypeSig.lit(TypeEnum.STRING)
  val valueSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL +
      TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()
  val sparkValueSig: TypeSig = TypeSig.all
  val resultSig: TypeSig = TypeSig.STRUCT.nested(valueSig)
  val sparkResultSig: TypeSig = TypeSig.STRUCT.nested(sparkValueSig)

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      val origExpr = exprMeta.wrapped.asInstanceOf[Expression]
      val (nameExprs, valExprs) = origExpr.children.grouped(2).map {
        case Seq(name, value) => (name, value)
      }.toList.unzip
      nameExprs.foreach { expr =>
        nameSig.tagExprParam(meta, expr, "name")
      }
      valExprs.foreach { expr =>
        valueSig.tagExprParam(meta, expr, "value")
      }
      if (!resultSig.isSupportedByPlugin(origExpr.dataType, meta.conf.decimalTypeEnabled)) {
        meta.willNotWorkOnGpu(s"unsupported data type in output: ${origExpr.dataType}")
      }
    }
  }

  override def support(dataType: TypeEnum.Value):
  Map[ExpressionContext, Map[String, SupportLevel]] = {
    val nameProjectSupport = nameSig.getSupportLevel(dataType, sparkNameSig)
    val nameLambdaSupport = TypeSig.none.getSupportLevel(dataType, sparkNameSig)
    val valueProjectSupport = valueSig.getSupportLevel(dataType, sparkValueSig)
    val valueLambdaSupport = TypeSig.none.getSupportLevel(dataType, sparkValueSig)
    val resultProjectSupport = resultSig.getSupportLevel(dataType, sparkResultSig)
    val resultLambdaSupport = TypeSig.none.getSupportLevel(dataType, sparkResultSig)
    Map((ProjectExprContext,
        Map(
          ("name", nameProjectSupport),
          ("value", valueProjectSupport),
          ("result", resultProjectSupport))),
      (LambdaExprContext,
          Map(
            ("name", nameLambdaSupport),
            ("value", valueLambdaSupport),
            ("result", resultLambdaSupport))))
  }
}

class CastChecks extends ExprChecks {
  // Don't show this with other operators show it in a different location
  override val shown: Boolean = false

  // When updating these please check child classes too
  import TypeSig._
  val nullChecks: TypeSig = integral + fp + BOOLEAN + TIMESTAMP + DATE + STRING + NULL
  val sparkNullSig: TypeSig = all

  val booleanChecks: TypeSig = integral + fp + BOOLEAN + TIMESTAMP + STRING
  val sparkBooleanSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + STRING

  val integralChecks: TypeSig = numeric + BOOLEAN + TIMESTAMP + STRING + BINARY
  val sparkIntegralSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + STRING + BINARY

  val fpChecks: TypeSig = numeric + BOOLEAN + TIMESTAMP + STRING
  val sparkFpSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + STRING

  val dateChecks: TypeSig = integral + fp + BOOLEAN + TIMESTAMP + DATE + STRING
  val sparkDateSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + DATE + STRING

  val timestampChecks: TypeSig = integral + fp + BOOLEAN + TIMESTAMP + DATE + STRING
  val sparkTimestampSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + DATE + STRING

  val stringChecks: TypeSig = numeric + BOOLEAN + TIMESTAMP + DATE + STRING + BINARY
  val sparkStringSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + DATE + CALENDAR + STRING + BINARY

  val binaryChecks: TypeSig = none
  val sparkBinarySig: TypeSig = STRING + BINARY

  val decimalChecks: TypeSig = DECIMAL + STRING
  val sparkDecimalSig: TypeSig = numeric + BOOLEAN + TIMESTAMP + STRING

  val calendarChecks: TypeSig = none
  val sparkCalendarSig: TypeSig = CALENDAR + STRING

  val arrayChecks: TypeSig = ARRAY.nested(FLOAT + DOUBLE + INT + ARRAY)

  val sparkArraySig: TypeSig = STRING + ARRAY.nested(all)

  val mapChecks: TypeSig = none
  val sparkMapSig: TypeSig = STRING + MAP.nested(all)

  val structChecks: TypeSig = psNote(TypeEnum.STRING, "the struct's children must also support " +
      "being cast to string")
  val sparkStructSig: TypeSig = STRING + STRUCT.nested(all)

  val udtChecks: TypeSig = none
  val sparkUdtSig: TypeSig = STRING + UDT

  private[this] def getChecksAndSigs(from: DataType): (TypeSig, TypeSig) = from match {
    case NullType => (nullChecks, sparkNullSig)
    case BooleanType => (booleanChecks, sparkBooleanSig)
    case ByteType | ShortType | IntegerType | LongType => (integralChecks, sparkIntegralSig)
    case FloatType | DoubleType => (fpChecks, sparkFpSig)
    case DateType => (dateChecks, sparkDateSig)
    case TimestampType => (timestampChecks, sparkTimestampSig)
    case StringType => (stringChecks, sparkStringSig)
    case BinaryType => (binaryChecks, sparkBinarySig)
    case _: DecimalType => (decimalChecks, sparkDecimalSig)
    case CalendarIntervalType => (calendarChecks, sparkCalendarSig)
    case _: ArrayType => (arrayChecks, sparkArraySig)
    case _: MapType => (mapChecks, sparkMapSig)
    case _: StructType => (structChecks, sparkStructSig)
    case _ => (udtChecks, sparkUdtSig)
  }

  private[this] def getChecksAndSigs(from: TypeEnum.Value): (TypeSig, TypeSig) = from match {
    case TypeEnum.NULL => (nullChecks, sparkNullSig)
    case TypeEnum.BOOLEAN => (booleanChecks, sparkBooleanSig)
    case TypeEnum.BYTE | TypeEnum.SHORT | TypeEnum.INT | TypeEnum.LONG =>
      (integralChecks, sparkIntegralSig)
    case TypeEnum.FLOAT | TypeEnum.DOUBLE => (fpChecks, sparkFpSig)
    case TypeEnum.DATE => (dateChecks, sparkDateSig)
    case TypeEnum.TIMESTAMP => (timestampChecks, sparkTimestampSig)
    case TypeEnum.STRING => (stringChecks, sparkStringSig)
    case TypeEnum.BINARY => (binaryChecks, sparkBinarySig)
    case TypeEnum.DECIMAL => (decimalChecks, sparkDecimalSig)
    case TypeEnum.CALENDAR => (calendarChecks, sparkCalendarSig)
    case TypeEnum.ARRAY => (arrayChecks, sparkArraySig)
    case TypeEnum.MAP => (mapChecks, sparkMapSig)
    case TypeEnum.STRUCT => (structChecks, sparkStructSig)
    case TypeEnum.UDT => (udtChecks, sparkUdtSig)
  }

  override def tag(meta: RapidsMeta[_, _, _]): Unit = {
    val exprMeta = meta.asInstanceOf[BaseExprMeta[_]]
    val context = exprMeta.context
    if (context != ProjectExprContext) {
      meta.willNotWorkOnGpu(s"this is not supported in the $context context")
    } else {
      val cast = meta.wrapped.asInstanceOf[UnaryExpression]
      val from = cast.child.dataType
      val to = cast.dataType
      val (checks, _) = getChecksAndSigs(from)
      if (!checks.isSupportedByPlugin(to, meta.conf.decimalTypeEnabled)) {
        meta.willNotWorkOnGpu(
          s"${meta.wrapped.getClass.getSimpleName} from $from to $to is not supported")
      }
    }
  }

  override def support(
      dataType: TypeEnum.Value): Map[ExpressionContext, Map[String, SupportLevel]] = {
    throw new IllegalStateException("support is different for cast")
  }

  def support(from: TypeEnum.Value, to: TypeEnum.Value): SupportLevel = {
    val (checks, sparkSig) = getChecksAndSigs(from)
    checks.getSupportLevel(to, sparkSig)
  }

  def sparkCanCast(from: DataType, to: DataType): Boolean = {
    val (_, sparkSig) = getChecksAndSigs(from)
    sparkSig.isSupportedBySpark(to)
  }

  def gpuCanCast(from: DataType, to: DataType, allowDecimal: Boolean = true): Boolean = {
    val (checks, _) = getChecksAndSigs(from)
    checks.isSupportedByPlugin(to, allowDecimal)
  }
}

object ExprChecks {
  /**
   * A check for an expression that only supports project, both in Spark and in the plugin.
   */
  def projectOnly(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (ProjectExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * A check for a unary expression that only support project both in Spark and the plugin.
   */
  def unaryProject(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      inputCheck: TypeSig,
      sparkInputSig: TypeSig): ExprChecks =
    projectOnly(outputCheck, sparkOutputSig,
      Seq(ParamCheck("input", inputCheck, sparkInputSig)))

  /**
   * A check for an expression that only supports project in the plugin, but Spark also supports
   * this expression in lambda.
   */
  def projectNotLambda(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val project = ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)
    val lambdaParams = paramCheck.map(pc => ParamCheck(pc.name, TypeSig.none, pc.spark))
    val lambdaRepeat = repeatingParamCheck.map(pc =>
      RepeatingParamCheck(pc.name, TypeSig.none, pc.spark))
    val lambda = ContextChecks(TypeSig.none, sparkOutputSig, lambdaParams, lambdaRepeat)
    ExprChecksImpl(Map((ProjectExprContext, project), (LambdaExprContext, lambda)))
  }

  /**
   * A check for a unary expression that only support project, but Spark also supports this
   * expression in lambda.
   */
  def unaryProjectNotLambda(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      inputCheck: TypeSig,
      sparkInputSig: TypeSig): ExprChecks =
    projectNotLambda(outputCheck, sparkOutputSig,
      Seq(ParamCheck("input", inputCheck, sparkInputSig)))

  /**
   * Unary expression checks for project where the input matches the output, but Spark also
   * supports this expression in lambda mode.
   */
  def unaryProjectNotLambdaInputMatchesOutput(check: TypeSig, sparkSig: TypeSig): ExprChecks =
    unaryProjectNotLambda(check, sparkSig, check, sparkSig)

  /**
   * Math unary checks where input and output are both DoubleType. Spark supports these for
   * both project and lambda, but the plugin only support project.
   */
  val mathUnary: ExprChecks = unaryProjectNotLambdaInputMatchesOutput(
    TypeSig.DOUBLE, TypeSig.DOUBLE)

  /**
   * Helper function for a binary expression where the plugin only supports project but Spark
   * support lambda too.
   */
  def binaryProjectNotLambda(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      param1: (String, TypeSig, TypeSig),
      param2: (String, TypeSig, TypeSig)): ExprChecks =
    projectNotLambda(outputCheck, sparkOutputSig,
      Seq(ParamCheck(param1._1, param1._2, param1._3),
        ParamCheck(param2._1, param2._2, param2._3)))

  /**
   * Aggregate operation where only group by agg and reduction is supported in the plugin and in
   * Spark.
   */
  def reductionAndGroupByAgg(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * Aggregate operation where window, reduction, and group by agg are all supported the same.
   */
  def fullAgg(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * For a generic expression that can work as both an aggregation and in the project context.
   * This is really just for PythonUDF.
   */
  def fullAggAndProject(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ProjectExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * An aggregation check where group by and reduction are supported by the plugin, but Spark
   * also supports window operations on these.
   */
  def aggNotWindow(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val windowParamCheck = paramCheck.map { pc =>
      ParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    val windowRepeat = repeatingParamCheck.map { pc =>
      RepeatingParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (ReductionAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck)),
      (WindowAggExprContext,
          ContextChecks(TypeSig.none, sparkOutputSig, windowParamCheck, windowRepeat))))
  }

  /**
   * Window only operations. Spark does not support these operations as anything but a window
   * operation.
   */
  def windowOnly(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks =
    ExprChecksImpl(Map(
      (WindowAggExprContext,
          ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))

  /**
   * An aggregation check where window operations are supported by the plugin, but Spark
   * also supports group by and reduction on these.
   * This is now really for 'collect_list' which is only supported by windowing.
   */
  def aggNotGroupByOrReduction(
      outputCheck: TypeSig,
      sparkOutputSig: TypeSig,
      paramCheck: Seq[ParamCheck] = Seq.empty,
      repeatingParamCheck: Option[RepeatingParamCheck] = None): ExprChecks = {
    val notWindowParamCheck = paramCheck.map { pc =>
      ParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    val notWindowRepeat = repeatingParamCheck.map { pc =>
      RepeatingParamCheck(pc.name, TypeSig.none, pc.spark)
    }
    ExprChecksImpl(Map(
      (GroupByAggExprContext,
        ContextChecks(TypeSig.none, sparkOutputSig, notWindowParamCheck, notWindowRepeat)),
      (ReductionAggExprContext,
        ContextChecks(TypeSig.none, sparkOutputSig, notWindowParamCheck, notWindowRepeat)),
      (WindowAggExprContext,
        ContextChecks(outputCheck, sparkOutputSig, paramCheck, repeatingParamCheck))))
  }
}

/**
 * Used for generating the support docs.
 */
object SupportedOpsDocs {
  private def execChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Executor</th>")
    println("<th>Description</th>")
    println("<th>Notes</th>")
    TypeEnum.values.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  private def exprChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Expression</th>")
    println("<th>SQL Functions(s)</th>")
    println("<th>Description</th>")
    println("<th>Notes</th>")
    println("<th>Context</th>")
    println("<th>Param/Output</th>")
    TypeEnum.values.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  private def partChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Partition</th>")
    println("<th>Description</th>")
    println("<th>Notes</th>")
    println("<th>Param</th>")
    TypeEnum.values.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  private def ioChecksHeaderLine(): Unit = {
    println("<tr>")
    println("<th>Format</th>")
    println("<th>Direction</th>")
    TypeEnum.values.foreach { t =>
      println(s"<th>$t</th>")
    }
    println("</tr>")
  }

  def help(): Unit = {
    val headerEveryNLines = 15
    // scalastyle:off line.size.limit
    println("---")
    println("layout: page")
    println("title: Supported Operators")
    println("nav_order: 6")
    println("---")
    println("<!-- Generated by SupportedOpsDocs.help. DO NOT EDIT! -->")
    println("Apache Spark supports processing various types of data. Not all expressions")
    println("support all data types. The RAPIDS Accelerator for Apache Spark has further")
    println("restrictions on what types are supported for processing. This tries")
    println("to document what operations are supported and what data types each operation supports.")
    println("Because Apache Spark is under active development too and this document was generated")
    println(s"against version ${ShimLoader.getSparkVersion} of Spark. Most of this should still")
    println("apply to other versions of Spark, but there may be slight changes.")
    println()
    println("# General limitations")
    println("## `Decimal`")
    println("The `Decimal` type in Spark supports a precision")
    println("up to 38 digits (128-bits). The RAPIDS Accelerator stores values up to 64-bits and as such only")
    println(s"supports a precision up to ${DType.DECIMAL64_MAX_PRECISION} digits. Note that")
    println("decimals are disabled by default in the plugin, because it is supported by a small")
    println("number of operations presently, which can result in a lot of data movement to and")
    println("from the GPU, slowing down processing in some cases.")
    println("Result `Decimal` precision and scale follow the same rule as CPU mode in Apache Spark:")
    println()
    println("```")
    println(" * In particular, if we have expressions e1 and e2 with precision/scale p1/s1 and p2/s2")
    println(" * respectively, then the following operations have the following precision / scale:")
    println(" *")
    println(" *   Operation    Result Precision                        Result Scale")
    println(" *   ------------------------------------------------------------------------")
    println(" *   e1 + e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)")
    println(" *   e1 - e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)")
    println(" *   e1 * e2      p1 + p2 + 1                             s1 + s2")
    println(" *   e1 / e2      p1 - s1 + s2 + max(6, s1 + p2 + 1)      max(6, s1 + p2 + 1)")
    println(" *   e1 % e2      min(p1-s1, p2-s2) + max(s1, s2)         max(s1, s2)")
    println(" *   e1 union e2  max(s1, s2) + max(p1-s1, p2-s2)         max(s1, s2)")
    println("```")
    println()
    println("However Spark inserts `PromotePrecision` to CAST both sides to the same type.")
    println("GPU mode may fall back to CPU even if the result Decimal precision is within 18 digits.")
    println("For example, `Decimal(8,2)` x `Decimal(6,3)` resulting in `Decimal (15,5)` runs on CPU,")
    println("because due to `PromotePrecision`, GPU mode assumes the result is `Decimal(19,6)`.")
    println()
    println("## `Timestamp`")
    println("Timestamps in Spark will all be converted to the local time zone before processing")
    println("and are often converted to UTC before being stored, like in Parquet or ORC.")
    println("The RAPIDS Accelerator only supports UTC as the time zone for timestamps.")
    println()
    println("## `CalendarInterval`")
    println("In Spark `CalendarInterval`s store three values, months, days, and microseconds.")
    println("Support for this type is still very limited in the accelerator. In some cases")
    println("only a a subset of the type is supported, like window ranges only support days currently.")
    println()
    println("## Configuration")
    println("There are lots of different configuration values that can impact if an operation")
    println("is supported or not. Some of these are a part of the RAPIDS Accelerator and cover")
    println("the level of compatibility with Apache Spark.  Those are covered [here](configs.md).")
    println("Others are a part of Apache Spark itself and those are a bit harder to document.")
    println("The work of updating this to cover that support is still ongoing.")
    println()
    println("In general though if you ever have any question about why an operation is not running")
    println("on the GPU you may set `spark.rapids.sql.explain` to ALL and it will try to give all of")
    println("the reasons why this particular operator or expression is on the CPU or GPU.")
    println()
    println("# Key")
    println("## Types")
    println()
    println("|Type Name|Type Description|")
    println("|---------|----------------|")
    println("|BOOLEAN|Holds true or false values.|")
    println("|BYTE|Signed 8-bit integer value.|")
    println("|SHORT|Signed 16-bit integer value.|")
    println("|INT|Signed 32-bit integer value.|")
    println("|LONG|Signed 64-bit integer value.|")
    println("|FLOAT|32-bit floating point value.|")
    println("|DOUBLE|64-bit floating point value.|")
    println("|DATE|A date with no time component. Stored as 32-bit integer with days since Jan 1, 1970.|")
    println("|TIMESTAMP|A date and time. Stored as 64-bit integer with microseconds since Jan 1, 1970 in the current time zone.|")
    println("|STRING|A text string. Stored as UTF-8 encoded bytes.|")
    println("|DECIMAL|A fixed point decimal value with configurable precision and scale.|")
    println("|NULL|Only stores null values and is typically only used when no other type can be determined from the SQL.|")
    println("|BINARY|An array of non-nullable bytes.|")
    println("|CALENDAR|Represents a period of time.  Stored as months, days and microseconds.|")
    println("|ARRAY|A sequence of elements.|")
    println("|MAP|A set of key value pairs, the keys cannot be null.|")
    println("|STRUCT|A series of named fields.|")
    println("|UDT|User defined types and java Objects. These are not standard SQL types.|")
    println()
    println("## Support")
    println()
    println("|Value|Description|")
    println("|---------|----------------|")
    println("|S| (Supported) Both Apache Spark and the RAPIDS Accelerator support this type.|")
    println("|S*| (Supported with limitations) Typically this refers to general limitations with `Timestamp` or `Decimal`|")
    println("| | (Not Applicable) Neither Spark not the RAPIDS Accelerator support this type in this situation.|")
    println("|_PS_| (Partial Support) Apache Spark supports this type, but the RAPIDS Accelerator only partially supports it. An explanation for what is missing will be included with this.|")
    println("|_PS*_| (Partial Support with limitations) Like regular Partial Support but with general limitations on `Timestamp` or `Decimal` types.|")
    println("|**NS**| (Not Supported) Apache Spark supports this type but the RAPIDS Accelerator does not.")
    println()
    println("# SparkPlan or Executor Nodes")
    println("Apache Spark uses a Directed Acyclic Graph(DAG) of processing to build a query.")
    println("The nodes in this graph are instances of `SparkPlan` and represent various high")
    println("level operations like doing a filter or project. The operations that the RAPIDS")
    println("Accelerator supports are described below.")
    println("<table>")
    execChecksHeaderLine()
    var totalCount = 0
    var nextOutputAt = headerEveryNLines
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.forall(_.shown)) {
        if (totalCount >= nextOutputAt) {
          execChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        println("<tr>")
        println(s"<td>${rule.tag.runtimeClass.getSimpleName}</td>")
        println(s"<td>${rule.description}</td>")
        println(s"<td>${rule.notes().getOrElse("None")}</td>")
        if (checks.isDefined) {
          val exprChecks = checks.get.asInstanceOf[ExecChecks]
          TypeEnum.values.foreach { t =>
            println(exprChecks.support(t).htmlTag)
          }
        } else {
          TypeEnum.values.foreach { _ =>
            println("<td><b>TODO</b></td>")
          }
        }
        println("</tr>")
        totalCount += 1
      }
    }
    println("</table>")
    println("* As was stated previously Decimal is only supported up to a precision of")
    println(s"${DType.DECIMAL64_MAX_PRECISION} and Timestamp is only supported in the")
    println("UTC time zone. Decimals are off by default due to performance impact in")
    println("some cases.")
    println()
    println("# Expression and SQL Functions")
    println("Inside each node in the DAG there can be one or more trees of expressions")
    println("that describe various types of processing that happens in that part of the plan.")
    println("These can be things like adding two numbers together or checking for null.")
    println("These expressions can have multiple input parameters and one output value.")
    println("These expressions also can happen in different contexts. Because of how the")
    println("accelerator works different contexts have different levels of support.")
    println()
    println("The most common expression context is `project`. In this context values from a single")
    println("input row go through the expression and the result will also be use to produce")
    println("something in the same row. Be aware that even in the case of aggregation and window")
    println("operations most of the processing is still done in the project context either before")
    println("or after the other processing happens.")
    println()
    println("Aggregation operations like count or sum can take place in either the `aggregation`,")
    println("`reduction`, or `window` context. `aggregation` is when the operation was done while")
    println("grouping the data by one or more keys. `reduction` is when there is no group by and")
    println("there is a single result for an entire column. `window` is for window operations.")
    println()
    println("The final expression context is `lambda` which happens primarily for higher order")
    println("functions in SQL.")
    println("Accelerator support is described below.")
    println("<table>")
    exprChecksHeaderLine()
    totalCount = 0
    nextOutputAt = headerEveryNLines
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.isDefined && checks.forall(_.shown)) {
        if (totalCount >= nextOutputAt) {
          exprChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        val sqlFunctions =
          ConfHelper.getSqlFunctionsForClass(rule.tag.runtimeClass).map(_.mkString(", "))
        val exprChecks = checks.get.asInstanceOf[ExprChecks]
        // Params can change between contexts, but should not
        val allData = TypeEnum.values.map { t =>
          (t, exprChecks.support(t))
        }.toMap
        // Now we should get the same keys for each type, so we are only going to look at the first
        // type for now
        val totalSpan = allData.values.head.map {
          case (_, m: Map[String, SupportLevel]) => m.size
        }.sum
        val representative = allData.values.head
        println("<tr>")
        println("<td rowSpan=\"" + totalSpan + "\">" +
            s"${rule.tag.runtimeClass.getSimpleName}</td>")
        println("<td rowSpan=\"" + totalSpan + "\">" + s"${sqlFunctions.getOrElse(" ")}</td>")
        println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.description}</td>")
        println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.notes().getOrElse("None")}</td>")
        var count = 0
        representative.foreach {
          case (context, data) =>
            val contextSpan = data.size
            println("<td rowSpan=\"" + contextSpan + "\">" + s"$context</td>")
            data.keys.foreach { param =>
              println(s"<td>$param</td>")
              TypeEnum.values.foreach { t =>
                println(allData(t)(context)(param).htmlTag)
              }
              println("</tr>")
              count += 1
              if (count < totalSpan) {
                println("<tr>")
              }
            }
        }
        totalCount += totalSpan
      }
    }
    println("</table>")
    println("* as was state previously Decimal is only supported up to a precision of")
    println(s"${DType.DECIMAL64_MAX_PRECISION} and Timestamp is only supported in the")
    println("UTC time zone. Decimals are off by default due to performance impact in")
    println("some cases.")
    println()
    println("## Casting")
    println("The above table does not show what is and is not supported for cast.")
    println("This table shows the matrix of supported casts.")
    println("Nested types like MAP, Struct, and Array can only be cast if the child types")
    println("can be cast.")
    println()
    println("Some of the casts to/from string on the GPU are not 100% the same and are disabled")
    println("by default. Please see the configs for more details on these specific cases.")
    println()
    println("Please note that even though casting from one type to another is supported")
    println("by Spark it does not mean they all produce usable results.  For example casting")
    println("from a date to a boolean always produces a null. This is for Hive compatibility")
    println("and the accelerator produces the same result.")
    println()
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      rule.getChecks match {
        case Some(cc: CastChecks) =>
          println(s"### `${rule.tag.runtimeClass.getSimpleName}`")
          println()
          println("<table>")
          val numTypes = TypeEnum.values.size
          println("<tr><th rowSpan=\"2\" colSpan=\"2\"></th><th colSpan=\"" + numTypes + "\">TO</th></tr>")
          println("<tr>")
          TypeEnum.values.foreach { t =>
            println(s"<th>$t</th>")
          }
          println("</tr>")

          println("<tr><th rowSpan=\"" + numTypes + "\">FROM</th>")
          var count = 0
          TypeEnum.values.foreach { from =>
            println(s"<th>$from</th>")
            TypeEnum.values.foreach { to =>
              println(cc.support(from, to).htmlTag)
            }
            println("</tr>")
            count += 1
            if (count < numTypes) {
              println("<tr>")
            }
          }
          println("</table>")
          println()
        case _ => // Nothing
      }
    }
    println()
    println("# Partitioning")
    println("When transferring data between different tasks the data is partitioned in")
    println("specific ways depending on requirements in the plan. Be aware that the types")
    println("included below are only for rows that impact where the data is partitioned.")
    println("So for example if we are doing a join on the column `a` the data would be")
    println("hash partitioned on `a`, but all of the other columns in the same data frame")
    println("as `a` don't show up in the table. They are controlled by the rules for")
    println("`ShuffleExchangeExec` which uses the `Partitioning`.")
    println("<table>")
    partChecksHeaderLine()
    totalCount = 0
    nextOutputAt = headerEveryNLines
    GpuOverrides.parts.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val checks = rule.getChecks
      if (rule.isVisible && checks.isDefined && checks.forall(_.shown)) {
        if (totalCount >= nextOutputAt) {
          partChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        val partChecks = checks.get.asInstanceOf[PartChecks]
        val allData = TypeEnum.values.map { t =>
          (t, partChecks.support(t))
        }.toMap
        // Now we should get the same keys for each type, so we are only going to look at the first
        // type for now
        val totalSpan = allData.values.head.size
        if (totalSpan > 0) {
          val representative = allData.values.head
          println("<tr>")
          println("<td rowSpan=\"" + totalSpan + "\">" +
              s"${rule.tag.runtimeClass.getSimpleName}</td>")
          println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.description}</td>")
          println("<td rowSpan=\"" + totalSpan + "\">" + s"${rule.notes().getOrElse("None")}</td>")
          var count = 0
          representative.keys.foreach { param =>
            println(s"<td>$param</td>")
            TypeEnum.values.foreach { t =>
              println(allData(t)(param).htmlTag)
            }
            println("</tr>")
            count += 1
            if (count < totalSpan) {
              println("<tr>")
            }
          }
          totalCount += totalSpan
        } else {
          // No arguments...
          println("<tr>")
          println(s"<td>${rule.tag.runtimeClass.getSimpleName}</td>")
          println(s"<td>${rule.description}</td>")
          println(s"<td>${rule.notes().getOrElse("None")}</td>")
          println(NotApplicable.htmlTag) // param
          TypeEnum.values.foreach { _ =>
            println(NotApplicable.htmlTag)
          }
          totalCount += 1
        }
      }
    }
    println("</table>")
    println()
    println("## Input/Output")
    println("For Input and Output it is not cleanly exposed what types are supported and which are not.")
    println("This table tries to clarify that. Be aware that some types may be disabled in some")
    println("cases for either reads or writes because of processing limitations, like rebasing")
    println("dates or timestamps, or for a lack of type coercion support.")
    println("<table>")
    ioChecksHeaderLine()
    totalCount = 0
    nextOutputAt = headerEveryNLines
    GpuOverrides.fileFormats.toSeq.sortBy(_._1.toString).foreach {
      case (format, ioMap) =>
        if (totalCount >= nextOutputAt) {
          ioChecksHeaderLine()
          nextOutputAt = totalCount + headerEveryNLines
        }
        val read = ioMap(ReadFileOp)
        val write = ioMap(WriteFileOp)
        println("<tr>")
        println("<th rowSpan=\"2\">" + s"$format</th>")
        println("<th>Read</th>")
        TypeEnum.values.foreach { t =>
          println(read.support(t).htmlTag)
        }
        println("</tr>")
        println("<tr>")
        println("<th>Write</th>")
        TypeEnum.values.foreach { t =>
          println(write.support(t).htmlTag)
        }
        println("</tr>")
        totalCount += 2
    }
    println("</table>")
    // scalastyle:on line.size.limit
  }

  def main(args: Array[String]): Unit = {
    val out = new FileOutputStream(new File(args(0)))
    Console.withOut(out) {
      Console.withErr(out) {
        SupportedOpsDocs.help()
      }
    }
  }
}
