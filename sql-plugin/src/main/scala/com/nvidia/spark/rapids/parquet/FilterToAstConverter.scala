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
 * @param ast      compiled AST for the conjuncts in `pushed`, or None if none were pushable.
 *                 The caller owns this and must close it.
 * @param pushed   conjuncts the hybrid scan reader will evaluate. It applies them exactly, so
 *                 they must not be re-applied above the scan.
 * @param residual conjuncts the reader cannot evaluate, so the scan output is a superset of the
 *                 rows they accept. Something above the scan has to apply them: for a file scan
 *                 that is Spark's own filter, which it keeps above the scan because Parquet
 *                 pushdown is never assumed to be exact.
 */
case class FilterPushdownPlan(
    ast: Option[CompiledExpression],
    pushed: Array[Filter],
    residual: Array[Filter])

/**
 * Converts Spark SQL Filter expressions into cuDF AST expressions for the Parquet hybrid scan
 * reader, which uses them to prune row groups and to build the row mask it applies during
 * materialize.
 *
 * Conversion is all-or-nothing per conjunct. A conjunct that cannot be represented exactly is
 * reported as residual rather than approximated, because the reader treats the AST it is given
 * as an exact filter: an AST that accepts fewer rows than the Spark filter would silently drop
 * qualifying rows.
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
   * Split `filters` into the conjuncts the reader can evaluate and those it cannot.
   *
   * The input array is an implicit conjunction, so a conjunct that fails to convert can be
   * left out of the pushed AST and evaluated after the scan instead. That rewrite is only
   * valid at conjunction level: within a disjunction every operand must convert, otherwise
   * pushing the convertible side alone would reject rows the other side accepts. `convertFilter`
   * enforces that by failing an `Or`/`Not` whose children do not fully convert.
   */
  def planPushdown(filters: Array[Filter]): FilterPushdownPlan = {
    val pushed = ArrayBuffer.empty[Filter]
    val residual = ArrayBuffer.empty[Filter]
    val exprs = ArrayBuffer.empty[AstExpression]
    val converted = FilterToAstConverter.splitConjuncts(filters).map { conjunct =>
      conjunct -> convertFilter(conjunct)
    }
    // Spark infers an `IsNotNull` for every column a null-intolerant predicate references, so one
    // usually arrives next to the predicate it was inferred from. It is then strictly redundant:
    // the predicate it came from cannot hold for a null, so leaving the check out cannot change
    // which rows or which row groups survive. Keeping it is not free, though. It adds a node that
    // is evaluated for every row of the filter columns, it makes libcudf build and compare a
    // nullability statistic per row group that nothing else needs, and it is the one thing that
    // pulls that statistic into the pruning expression, which is where a chunk holding a mix of
    // nulls and non-nulls yields an unknown verdict — work spent to learn nothing.
    // A null check with no such sibling is left alone, since on its own it can still rule out a
    // row group whose column is entirely null.
    // Only a conjunct that is being pushed counts as the sibling, and a null check never counts as
    // one, or a pair of them on the same column would each vouch for the other and both be left
    // out, losing the constraint altogether.
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
        convertNegated(child)

      case other =>
        // String predicates (StringStartsWith/EndsWith/Contains) land here; libcudf ASTs have
        // no string-matching operators, so they stay residual.
        logDebug(s"Filter not convertible to a cuDF AST: $other")
        None
    }
  }

  /**
   * HACK: factor the negation through to the comparisons rather than emit a cuDF `NOT`, because
   * libcudf's row group pruning gets a negation wrong. For `NOT(col == 5)` it checks whether any
   * row is 5 and then inverts that answer, pruning every row group holding a 5 anywhere. What the
   * filter means is `col != 5`, which may only prune a row group where every row is 5, so the read
   * silently loses rows. Anything with no negated form stays residual. Remove this once libcudf is
   * fixed.
   *
   * `IsNotNull` still emits a `NOT` over a null check, which that pruning skips rather than
   * inverts. Negating a comparison is exact for nulls too: both forms are null on the same rows.
   */
  private def convertNegated(filter: Filter): Option[AstExpression] = {
    filter match {
      case EqualTo(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.NOT_EQUAL)

      case GreaterThan(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.LESS_EQUAL)

      case GreaterThanOrEqual(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.LESS)

      case LessThan(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.GREATER_EQUAL)

      case LessThanOrEqual(attribute, value) =>
        convertComparison(attribute, value, BinaryOperator.GREATER)

      case IsNull(attribute) =>
        convertFilter(IsNotNull(attribute))

      case IsNotNull(attribute) =>
        convertFilter(IsNull(attribute))

      case In(attribute, values) =>
        convertNotIn(attribute, values)

      case And(left, right) =>
        for {
          leftExpr <- convertNegated(left)
          rightExpr <- convertNegated(right)
        } yield nullAwareOr(leftExpr, rightExpr)

      case Or(left, right) =>
        for {
          leftExpr <- convertNegated(left)
          rightExpr <- convertNegated(right)
        } yield nullAwareAnd(leftExpr, rightExpr)

      case Not(child) =>
        convertFilter(child)

      case other =>
        // No negated form to emit. `EqualNullSafe` lands here because the AST has no null-safe
        // not-equal, and the string predicates because they do not convert at all.
        logDebug(s"Negated filter not convertible to a cuDF AST: $other")
        None
    }
  }

  /**
   * Join two converted filters with a conjunction whose null handling is Kleene rather than
   * propagating. This matters for pruning, not just for the rows: libcudf rewrites the expression
   * against per-row-group statistics, where anything it cannot decide for a row group comes back
   * null, and a plain `LOGICAL_AND` lets that null swallow a sibling that did decide, keeping a row
   * group that could have been pruned. A column holding a mix of nulls and non-nulls makes any
   * `IsNull` or `IsNotNull` on it undecidable that way, and Spark infers an `IsNotNull` beside
   * nearly every predicate it pushes, so the propagating form costs a read all of its statistics
   * pruning. `NULL_LOGICAL_AND` evaluates unknown against false as false, which is also what Spark
   * means by `AND`.
   */
  private def nullAwareAnd(left: AstExpression, right: AstExpression): AstExpression = {
    new BinaryOperation(BinaryOperator.NULL_LOGICAL_AND, left, right)
  }

  /** Join two converted filters with a Kleene disjunction. See `nullAwareAnd`. */
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
      literal <- convertLiteral(value, dataType)
    } yield new BinaryOperation(operator, col, literal)
  }

  /**
   * Convert `IN` to a chain of OR-ed equality comparisons. Every value must convert: dropping
   * one would reject the rows that match it.
   */
  private def convertIn(attribute: String, values: Array[Any]): Option[AstExpression] = {
    // An empty IN list accepts nothing. There is no AST literal for "always false", so leave
    // it residual rather than inventing one.
    if (values.isEmpty) {
      None
    } else {
      for {
        col <- makeColumnRef(attribute)
        dataType <- getColumnType(attribute)
        literals <- allOrNothing(values.map(convertLiteral(_, dataType)))
      } yield {
        literals.map { literal =>
          new BinaryOperation(BinaryOperator.EQUAL, col, literal): AstExpression
        }.reduce((left, right) => nullAwareOr(left, right))
      }
    }
  }

  /**
   * Convert `NOT IN` to a chain of AND-ed inequality comparisons, rather than a negation of the
   * OR-ed equalities `convertIn` builds. See `convertNegated`.
   */
  private def convertNotIn(attribute: String, values: Array[Any]): Option[AstExpression] = {
    // An empty NOT IN list accepts everything. There is no AST literal for "always true", so
    // leave it residual rather than inventing one.
    if (values.isEmpty) {
      None
    } else {
      for {
        col <- makeColumnRef(attribute)
        dataType <- getColumnType(attribute)
        literals <- allOrNothing(values.map(convertLiteral(_, dataType)))
      } yield {
        literals.map { literal =>
          new BinaryOperation(BinaryOperator.NOT_EQUAL, col, literal): AstExpression
        }.reduce((left, right) => nullAwareAnd(left, right))
      }
    }
  }

  /** Some(values) only if every element is defined, so a partial conversion cannot leak out. */
  private def allOrNothing[T: ClassTag](converted: Array[Option[T]]): Option[Array[T]] = {
    if (converted.forall(_.isDefined)) Some(converted.map(_.get)) else None
  }

  private def getColumnType(attribute: String): Option[DataType] =
    columnNameToType.get(normalize(attribute))

  /**
   * Build a reference to `attribute`, or None if it is not one of the columns being read (a
   * filter on a column outside the read schema cannot be evaluated by the reader).
   */
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
            // Decimal needs the column's precision/scale to round-trip, and nested types have
            // no AST representation at all. Both stay residual.
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
      // getTime is whole milliseconds (negative before the epoch, so scale rather than divide
      // to keep floor semantics); getNanos carries the sub-second part.
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

  /**
   * Flatten an implicit conjunction into its top-level conjuncts, so that an unconvertible
   * part of a nested `And` does not force the whole tree to be residual.
   */
  def splitConjuncts(filters: Array[Filter]): Array[Filter] = {
    def split(filter: Filter): Seq[Filter] = filter match {
      case And(left, right) => split(left) ++ split(right)
      case other => Seq(other)
    }
    filters.flatMap(split)
  }

  /**
   * Whether a filter is of a shape the converter can attempt. This is a cheap structural
   * screen for eligibility checks; it does not verify that the referenced columns exist or
   * that the literals convert, so `planPushdown` remains the authority on what is pushed.
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
   * Whether a filter can rule out a row group, which is the only reason to read one with the
   * hybrid scan reader rather than the parquet-mr path. A read that can prune nothing pays every
   * cost hybrid adds — parsing the footer here rather than reusing parquet-mr's, planning byte
   * ranges, the dictionary page phase with its own read and semaphore acquisition — to end up
   * reading exactly what parquet-mr would have read. Two-stage does not save it either: with no
   * row group ruled out, the row mask is nearly all true, so almost no payload page is skipped.
   *
   * `IsNotNull` is the case this rules out, and it is a common one, because Spark infers one for
   * every column a null-intolerant predicate mentions. Ruling out a row group on it means proving
   * the chunk is *entirely* null, which is what libcudf tests — its per-chunk flag is set only
   * when the null count equals the value count — and a chunk with no non-null value at all is
   * rare in real data, so treat this as never pruning rather than as a long shot worth paying for.
   * `IsNull` is the useful direction: it rules out every row group whose column has no nulls.
   *
   * Within a disjunction every operand has to be able to prune, since a row group survives an
   * `Or` unless both sides rule it out.
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
   * Whether a filter can rule out a row group using its dictionary pages, which only tell
   * whether a value is present in a chunk. libcudf's dictionary filter acts on `EQUAL` and
   * `NOT_EQUAL` and treats every other operator as "keep", so reading the dictionary pages for
   * a filter without one of those is pure cost.
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
   * The columns a filter proves are not null wherever it holds, which makes a separate
   * `IsNotNull` on any of them redundant. A comparison qualifies because a null operand makes it
   * unknown rather than true. An `Or` only proves what both of its sides prove, and a negation
   * proves nothing, since `NOT` of an unknown is unknown rather than true.
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

  /**
   * Whether every residual filter can be evaluated against the scan's own output. A residual
   * filter referencing a column outside `schema` cannot be applied after the scan, so such a
   * read must not use the hybrid scan reader at all.
   */
  def residualsEvaluableOn(residual: Array[Filter], schema: StructType,
      isCaseSensitive: Boolean): Boolean = {
    def normalize(name: String): String = {
      if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
    }
    val available = schema.fields.map(field => normalize(field.name)).toSet
    residual.forall(getReferencedColumns(_).forall(col => available.contains(normalize(col))))
  }
}
