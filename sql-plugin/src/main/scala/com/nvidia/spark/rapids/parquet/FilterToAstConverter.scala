/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.parquet

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.DType
import ai.rapids.cudf.ast.{AstExpression, BinaryOperation, BinaryOperator, ColumnNameReference, ColumnReference, CompiledExpression, Literal, UnaryOperation, UnaryOperator}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * How a set of Spark filters was split for a hybrid scan read.
 *
 * @param ast      compiled AST for `pushed`, or None if nothing was pushed. Owned by the caller.
 * @param pushed   conjuncts the reader applies exactly
 * @param residual conjuncts the reader cannot apply; Spark's filter above the scan applies them
 */
case class FilterPushdownPlan(
    ast: Option[CompiledExpression],
    pushed: Array[Filter],
    residual: Array[Filter])

/**
 * Converts Spark filters into cuDF AST expressions for the Parquet hybrid scan reader, which uses
 * them to prune row groups and filter rows.
 *
 * The reader treats the AST as exact, so a conjunct that cannot be converted exactly is left
 * residual rather than approximated.
 *
 * @param schema          schema of the columns being read, for column index and type lookup
 * @param isCaseSensitive whether column name matching is case-sensitive
 * @param useColumnNames  reference columns by name rather than by projection index
 */
case class FilterToAstConverter(
    schema: StructType,
    isCaseSensitive: Boolean,
    useColumnNames: Boolean) extends Logging {

  private def normalize(name: String): String = {
    if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
  }

  private val columnNameToIndex: Map[String, Int] =
    schema.fields.zipWithIndex.map { case (field, idx) => normalize(field.name) -> idx }.toMap

  private val columnNameToType: Map[String, DataType] =
    schema.fields.map(field => normalize(field.name) -> field.dataType).toMap

  /**
   * Split `filters`, an implicit conjunction, into the conjuncts the reader can evaluate and
   * those it cannot. Only top-level conjuncts can be left residual; an `Or` or `Not` converts
   * only if all of its children do.
   */
  def planPushdown(filters: Array[Filter]): FilterPushdownPlan = {
    val pushed = ArrayBuffer.empty[Filter]
    val residual = ArrayBuffer.empty[Filter]
    val exprs = ArrayBuffer.empty[AstExpression]
    val converted = FilterToAstConverter.splitConjuncts(filters).map { conjunct =>
      conjunct -> convertFilter(conjunct)
    }
    // Don't push an `IsNotNull` that a pushed predicate on the same column already implies (Spark
    // infers one beside most predicates): it changes no result but costs a per-row evaluation and
    // a per-row-group null statistic. Null checks don't vouch for each other, or a pair of them
    // would both be dropped.
    val provenNotNull = converted.collect {
      case (conjunct, Some(_)) if FilterToAstConverter.notNullCheckColumn(conjunct).isEmpty =>
        FilterToAstConverter.columnsProvenNotNull(conjunct).map(normalize)
    }.flatten.toSet
    converted.foreach { case (conjunct, expr) =>
      val redundant = FilterToAstConverter.notNullCheckColumn(conjunct)
          .exists(column => provenNotNull.contains(normalize(column)))
      expr match {
        case Some(_) if redundant =>
          logDebug(s"Hybrid scan is not pushing $conjunct; another pushed conjunct on the same " +
            "column already rules out a null")
          residual += conjunct
        case Some(e) =>
          pushed += conjunct
          exprs += e
        case None =>
          logDebug(s"Hybrid scan cannot push $conjunct; it will be applied after the scan")
          residual += conjunct
      }
    }
    val ast = if (exprs.isEmpty) {
      None
    } else {
      Some(exprs.reduce((left, right) => nullAwareAnd(left, right)).compile())
    }
    FilterPushdownPlan(ast, pushed.toArray, residual.toArray)
  }

  /**
   * Convert a single Spark Filter to a cuDF AST expression, or None if it cannot be
   * represented exactly.
   */
  def convertFilter(filter: Filter): Option[AstExpression] = {
    filter match {
      case EqualTo(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.EQUAL)

      case EqualNullSafe(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.NULL_EQUAL)

      case GreaterThan(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.GREATER)

      case GreaterThanOrEqual(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.GREATER_EQUAL)

      case LessThan(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.LESS)

      case LessThanOrEqual(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.LESS_EQUAL)

      case IsNull(attribute) =>
        makeColumnRef(attribute).map { col =>
          new UnaryOperation(UnaryOperator.IS_NULL, col)
        }

      case IsNotNull(attribute) =>
        makeColumnRef(attribute).map { col =>
          new UnaryOperation(UnaryOperator.NOT,
            new UnaryOperation(UnaryOperator.IS_NULL, col))
        }

      case In(attribute, values) =>
        convertIn(attribute, values)

      case And(left, right) =>
        for {
          leftExpr <- convertFilter(left)
          rightExpr <- convertFilter(right)
        } yield nullAwareAnd(leftExpr, rightExpr)

      case Or(left, right) =>
        for {
          leftExpr <- convertFilter(left)
          rightExpr <- convertFilter(right)
        } yield nullAwareOr(leftExpr, rightExpr)

      case Not(child) =>
        convertFilter(child).map(new UnaryOperation(UnaryOperator.NOT, _))

      case other =>
        // String predicates (StringStartsWith/EndsWith/Contains) land here; libcudf ASTs have
        // no string-matching operators, so they stay residual.
        logDebug(s"Filter not convertible to a cuDF AST: $other")
        None
    }
  }

  /**
   * Kleene `AND`, as Spark means it: under a `NOT`, a propagating `LOGICAL_AND` would turn
   * `NOT(null AND false)` into null rather than true.
   */
  private def nullAwareAnd(left: AstExpression, right: AstExpression): AstExpression = {
    new BinaryOperation(BinaryOperator.NULL_LOGICAL_AND, left, right)
  }

  /** Kleene `OR`. See `nullAwareAnd`. */
  private def nullAwareOr(left: AstExpression, right: AstExpression): AstExpression = {
    new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR, left, right)
  }

  private def convertComparison(
      attribute: String,
      value: Any,
      operator: BinaryOperator): Option[AstExpression] = {
    for {
      col <- makeColumnRef(attribute)
      dataType <- getColumnType(attribute)
      if pushableFloatComparison(dataType, value, operator)
      literal <- convertLiteral(value, dataType)
    } yield new BinaryOperation(operator, col, literal)
  }

  private def isFloatingPoint(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType => true
    case _ => false
  }

  /** Whether a literal compared against a column is a NaN. */
  private def isNaNLiteral(value: Any): Boolean = value match {
    case f: Float => f.isNaN
    case d: Double => d.isNaN
    case _ => false
  }

  /**
   * cuDF ASTs compare floats with IEEE-754 semantics: a NaN equals nothing, not even itself, and
   * loses every ordered comparison. Spark instead orders NaN above every value and equal to
   * itself. The two agree for `=`, `<`, `<=`, and `!=` against a non-NaN literal, so those push;
   * `>`, `>=`, and any NaN literal diverge -- Spark keeps NaN rows the cuDF AST would drop -- so
   * they stay residual for Spark's post-scan filter to evaluate.
   */
  private def pushableFloatComparison(
      dataType: DataType, value: Any, operator: BinaryOperator): Boolean = {
    !isFloatingPoint(dataType) ||
      (!isNaNLiteral(value) &&
        operator != BinaryOperator.GREATER &&
        operator != BinaryOperator.GREATER_EQUAL)
  }

  /** Convert `IN` to OR-ed equalities. Every value must convert. */
  private def convertIn(attribute: String, values: Array[Any]): Option[AstExpression] = {
    // The AST has no "always false" literal for an empty list.
    if (values.isEmpty) {
      None
    } else {
      for {
        col <- makeColumnRef(attribute)
        dataType <- getColumnType(attribute)
        if !isFloatingPoint(dataType) || !values.exists(isNaNLiteral)
        literals <- allOrNothing(values.map(convertLiteral(_, dataType)))
      } yield {
        literals.map { literal =>
          new BinaryOperation(BinaryOperator.EQUAL, col, literal): AstExpression
        }.reduce((left, right) => nullAwareOr(left, right))
      }
    }
  }

  /** Some(values) only if every element is defined. */
  private def allOrNothing[T: ClassTag](converted: Array[Option[T]]): Option[Array[T]] = {
    if (converted.forall(_.isDefined)) Some(converted.map(_.get)) else None
  }

  private def getColumnType(attribute: String): Option[DataType] =
    columnNameToType.get(normalize(attribute))

  /** A reference to `attribute`, or None if it is not one of the columns being read. */
  private def makeColumnRef(attribute: String): Option[AstExpression] = {
    columnNameToIndex.get(normalize(attribute)).map { colIdx =>
      if (useColumnNames) {
        new ColumnNameReference(attribute)
      } else {
        new ColumnReference(colIdx)
      }
    }
  }

  private def convertLiteral(value: Any, dataType: DataType): Option[Literal] = {
    if (value == null) {
      sparkTypeToCudfType(dataType).map(cudfType => Literal.ofNull(cudfType))
    } else {
      try {
        dataType match {
          case BooleanType => Some(Literal.ofBoolean(value.asInstanceOf[Boolean]))
          case ByteType => Some(Literal.ofByte(value.asInstanceOf[Number].byteValue()))
          case ShortType => Some(Literal.ofShort(value.asInstanceOf[Number].shortValue()))
          case IntegerType => Some(Literal.ofInt(value.asInstanceOf[Number].intValue()))
          case LongType => Some(Literal.ofLong(value.asInstanceOf[Number].longValue()))
          case FloatType => Some(Literal.ofFloat(value.asInstanceOf[Number].floatValue()))
          case DoubleType => Some(Literal.ofDouble(value.asInstanceOf[Number].doubleValue()))
          case StringType => Some(Literal.ofString(value.asInstanceOf[String]))
          case DateType => convertDateLiteral(value)
          case TimestampType => convertTimestampLiteral(value)
          case other =>
            // Decimal needs the column's precision and scale; nested types have no AST form.
            logDebug(s"No cuDF AST literal for $other")
            None
        }
      } catch {
        case e: Exception =>
          logDebug(s"Failed to convert literal $value to $dataType: ${e.getMessage}")
          None
      }
    }
  }

  /** Spark pushes dates as days since the epoch, boxed as `Date`, `LocalDate`, or a number. */
  private def convertDateLiteral(value: Any): Option[Literal] = value match {
    case d: Date => Some(Literal.ofTimestampDaysFromInt(d.toLocalDate.toEpochDay.toInt))
    case d: LocalDate => Some(Literal.ofTimestampDaysFromInt(d.toEpochDay.toInt))
    case n: Number => Some(Literal.ofTimestampDaysFromInt(n.intValue()))
    case other =>
      logDebug(s"Unsupported date literal type: ${other.getClass}")
      None
  }

  /** Spark pushes timestamps as microseconds since the epoch. */
  private def convertTimestampLiteral(value: Any): Option[Literal] = value match {
    case ts: Timestamp =>
      // getTime is floored milliseconds; getNanos adds the sub-millisecond part.
      val micros = ts.getTime * 1000L + (ts.getNanos / 1000) % 1000
      Some(Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS, micros))
    case i: Instant =>
      val micros = i.getEpochSecond * 1000000L + i.getNano / 1000
      Some(Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS, micros))
    case n: Number =>
      Some(Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS, n.longValue()))
    case other =>
      logDebug(s"Unsupported timestamp literal type: ${other.getClass}")
      None
  }

  private def sparkTypeToCudfType(dataType: DataType): Option[DType] = dataType match {
    case BooleanType => Some(DType.BOOL8)
    case ByteType => Some(DType.INT8)
    case ShortType => Some(DType.INT16)
    case IntegerType => Some(DType.INT32)
    case LongType => Some(DType.INT64)
    case FloatType => Some(DType.FLOAT32)
    case DoubleType => Some(DType.FLOAT64)
    case StringType => Some(DType.STRING)
    case DateType => Some(DType.TIMESTAMP_DAYS)
    case TimestampType => Some(DType.TIMESTAMP_MICROSECONDS)
    case _ => None
  }
}

object FilterToAstConverter {

  /** Flatten nested `And`s into top-level conjuncts, so each can be pushed or left residual. */
  def splitConjuncts(filters: Array[Filter]): Array[Filter] = {
    def split(filter: Filter): Seq[Filter] = filter match {
      case And(left, right) => split(left) ++ split(right)
      case other => Seq(other)
    }
    filters.flatMap(split)
  }

  /**
   * Whether a filter has a shape the converter handles. A structural check only: `planPushdown`
   * decides what is actually pushed.
   */
  def isSupportedFilter(filter: Filter): Boolean = filter match {
    case _: EqualTo | _: EqualNullSafe => true
    case _: GreaterThan | _: GreaterThanOrEqual => true
    case _: LessThan | _: LessThanOrEqual => true
    case _: IsNull | _: IsNotNull => true
    case In(_, values) => values.nonEmpty
    case And(left, right) => isSupportedFilter(left) && isSupportedFilter(right)
    case Or(left, right) => isSupportedFilter(left) && isSupportedFilter(right)
    case Not(child) => isSupportedFilter(child)
    case _ => false
  }

  /**
   * Whether a filter can rule out a row group, without which the hybrid scan reader only adds
   * cost over parquet-mr.
   *
   * `IsNotNull` counts as never pruning: it needs a chunk that is entirely null, which is rare.
   * An `Or` prunes only if both sides can.
   */
  def canPruneRowGroups(filter: Filter): Boolean = filter match {
    case _: EqualTo | _: EqualNullSafe => true
    case _: GreaterThan | _: GreaterThanOrEqual => true
    case _: LessThan | _: LessThanOrEqual => true
    case _: IsNull => true
    case _: IsNotNull => false
    case In(_, values) => values.nonEmpty
    case Not(IsNull(_)) => false
    case Not(IsNotNull(_)) => true
    case And(left, right) => canPruneRowGroups(left) || canPruneRowGroups(right)
    case Or(left, right) => canPruneRowGroups(left) && canPruneRowGroups(right)
    case Not(child) => canPruneRowGroups(child)
    case _ => false
  }

  /**
   * Whether a filter can rule out a row group using its dictionary pages. libcudf's dictionary
   * filter only acts on `EQUAL` and `NOT_EQUAL`.
   */
  def canPruneWithDictionary(filter: Filter): Boolean = filter match {
    case _: EqualTo | _: EqualNullSafe => true
    case In(_, values) => values.nonEmpty
    case And(left, right) => canPruneWithDictionary(left) || canPruneWithDictionary(right)
    case Or(left, right) => canPruneWithDictionary(left) && canPruneWithDictionary(right)
    case Not(child) => canPruneWithDictionary(child)
    case _ => false
  }

  /** The column a filter checks for being non-null, if checking that is all the filter does. */
  def notNullCheckColumn(filter: Filter): Option[String] = filter match {
    case IsNotNull(attribute) => Some(attribute)
    case Not(IsNull(attribute)) => Some(attribute)
    case _ => None
  }

  /**
   * The columns a filter proves are not null wherever it holds. An `Or` proves only what both
   * sides prove; a negation proves nothing.
   */
  def columnsProvenNotNull(filter: Filter): Set[String] = filter match {
    case IsNotNull(attribute) => Set(attribute)
    case Not(IsNull(attribute)) => Set(attribute)
    case EqualTo(attribute, _) => Set(attribute)
    // `c <=> null` is how a null is matched, so it proves the opposite.
    case EqualNullSafe(attribute, value) if value != null => Set(attribute)
    case GreaterThan(attribute, _) => Set(attribute)
    case GreaterThanOrEqual(attribute, _) => Set(attribute)
    case LessThan(attribute, _) => Set(attribute)
    case LessThanOrEqual(attribute, _) => Set(attribute)
    case In(attribute, values) if values.nonEmpty => Set(attribute)
    case StringStartsWith(attribute, _) => Set(attribute)
    case StringEndsWith(attribute, _) => Set(attribute)
    case StringContains(attribute, _) => Set(attribute)
    case And(left, right) => columnsProvenNotNull(left) ++ columnsProvenNotNull(right)
    case Or(left, right) => columnsProvenNotNull(left).intersect(columnsProvenNotNull(right))
    case _ => Set.empty
  }

  /** The column names a filter references. */
  def getReferencedColumns(filter: Filter): Set[String] = filter match {
    case EqualTo(attribute, _) => Set(attribute)
    case EqualNullSafe(attribute, _) => Set(attribute)
    case GreaterThan(attribute, _) => Set(attribute)
    case GreaterThanOrEqual(attribute, _) => Set(attribute)
    case LessThan(attribute, _) => Set(attribute)
    case LessThanOrEqual(attribute, _) => Set(attribute)
    case IsNull(attribute) => Set(attribute)
    case IsNotNull(attribute) => Set(attribute)
    case In(attribute, _) => Set(attribute)
    case StringStartsWith(attribute, _) => Set(attribute)
    case StringEndsWith(attribute, _) => Set(attribute)
    case StringContains(attribute, _) => Set(attribute)
    case And(left, right) => getReferencedColumns(left) ++ getReferencedColumns(right)
    case Or(left, right) => getReferencedColumns(left) ++ getReferencedColumns(right)
    case Not(child) => getReferencedColumns(child)
    case _ => Set.empty
  }

  /** Whether every residual filter only references columns in `schema`, the scan's output. */
  def residualsEvaluableOn(residual: Array[Filter], schema: StructType,
      isCaseSensitive: Boolean): Boolean = {
    def normalize(name: String): String = {
      if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
    }
    val available = schema.fields.map(field => normalize(field.name)).toSet
    residual.forall(getReferencedColumns(_).forall(col => available.contains(normalize(col))))
  }
}
