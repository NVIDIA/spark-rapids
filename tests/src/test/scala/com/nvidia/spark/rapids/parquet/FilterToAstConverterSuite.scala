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

import ai.rapids.cudf.DType
import ai.rapids.cudf.ast.{AstExpression, BinaryOperation, BinaryOperator, ColumnNameReference, ColumnReference, Literal, UnaryOperation, UnaryOperator}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class FilterToAstConverterSuite extends AnyFunSuite {

  private val testSchema = StructType(Seq(
    StructField("int_col", IntegerType),
    StructField("long_col", LongType),
    StructField("float_col", FloatType),
    StructField("double_col", DoubleType),
    StructField("string_col", StringType),
    StructField("bool_col", BooleanType),
    StructField("date_col", DateType),
    StructField("ts_col", TimestampType),
    StructField("dec_col", DecimalType(10, 2)),
    StructField("byte_col", ByteType),
    StructField("short_col", ShortType)
  ))

  private def converter(caseSensitive: Boolean = true, useColumnNames: Boolean = false) =
    FilterToAstConverter(testSchema, caseSensitive, useColumnNames)

  /** Runs `body` on a pushdown plan, closing the compiled AST it owns. */
  private def withPlan(filters: Filter*)(body: FilterPushdownPlan => Unit): Unit = {
    val plan = converter().planPushdown(filters.toArray)
    withResource(plan.ast) { _ =>
      body(plan)
    }
  }

  test("isSupportedFilter accepts comparisons, null checks and non-empty In") {
    assert(FilterToAstConverter.isSupportedFilter(EqualTo("int_col", 1)))
    assert(FilterToAstConverter.isSupportedFilter(EqualNullSafe("int_col", 1)))
    assert(FilterToAstConverter.isSupportedFilter(GreaterThan("int_col", 1)))
    assert(FilterToAstConverter.isSupportedFilter(GreaterThanOrEqual("int_col", 1)))
    assert(FilterToAstConverter.isSupportedFilter(LessThan("int_col", 1)))
    assert(FilterToAstConverter.isSupportedFilter(LessThanOrEqual("int_col", 1)))
    assert(FilterToAstConverter.isSupportedFilter(IsNull("int_col")))
    assert(FilterToAstConverter.isSupportedFilter(IsNotNull("int_col")))
    assert(FilterToAstConverter.isSupportedFilter(In("int_col", Array(1, 2, 3))))
  }

  test("isSupportedFilter recurses through compound filters") {
    assert(FilterToAstConverter.isSupportedFilter(
      And(EqualTo("int_col", 1), GreaterThan("long_col", 100L))))
    assert(FilterToAstConverter.isSupportedFilter(
      Or(EqualTo("int_col", 1), EqualTo("int_col", 2))))
    assert(FilterToAstConverter.isSupportedFilter(Not(EqualTo("int_col", 1))))
    assert(!FilterToAstConverter.isSupportedFilter(
      And(EqualTo("int_col", 1), StringContains("string_col", "x"))))
  }

  test("isSupportedFilter rejects string predicates and empty In") {
    assert(!FilterToAstConverter.isSupportedFilter(StringStartsWith("string_col", "foo")))
    assert(!FilterToAstConverter.isSupportedFilter(StringEndsWith("string_col", "bar")))
    assert(!FilterToAstConverter.isSupportedFilter(StringContains("string_col", "baz")))
    assert(!FilterToAstConverter.isSupportedFilter(In("int_col", Array.empty)))
  }

  test("canPruneWithDictionary accepts only equality and In") {
    assert(FilterToAstConverter.canPruneWithDictionary(EqualTo("int_col", 1)))
    assert(FilterToAstConverter.canPruneWithDictionary(EqualNullSafe("int_col", 1)))
    assert(FilterToAstConverter.canPruneWithDictionary(In("int_col", Array(1, 2))))
    assert(FilterToAstConverter.canPruneWithDictionary(Not(EqualTo("int_col", 1))))
    assert(!FilterToAstConverter.canPruneWithDictionary(GreaterThan("int_col", 1)))
    assert(!FilterToAstConverter.canPruneWithDictionary(IsNull("int_col")))
    assert(!FilterToAstConverter.canPruneWithDictionary(IsNotNull("int_col")))
    assert(FilterToAstConverter.canPruneWithDictionary(
      And(GreaterThan("int_col", 1), EqualTo("string_col", "a"))))
    assert(!FilterToAstConverter.canPruneWithDictionary(
      Or(EqualTo("int_col", 1), GreaterThan("int_col", 2))))
  }

  test("convertFilter converts every supported literal type") {
    val c = converter()
    assert(c.convertFilter(EqualTo("int_col", 42)).isDefined)
    assert(c.convertFilter(EqualTo("long_col", 100L)).isDefined)
    assert(c.convertFilter(EqualTo("float_col", 3.14f)).isDefined)
    assert(c.convertFilter(EqualTo("double_col", 2.718)).isDefined)
    assert(c.convertFilter(EqualTo("string_col", "test")).isDefined)
    assert(c.convertFilter(EqualTo("bool_col", true)).isDefined)
  }

  test("convertFilter converts every comparison operator and null check") {
    val c = converter()
    assert(c.convertFilter(GreaterThan("int_col", 10)).isDefined)
    assert(c.convertFilter(GreaterThanOrEqual("int_col", 10)).isDefined)
    assert(c.convertFilter(LessThan("int_col", 10)).isDefined)
    assert(c.convertFilter(LessThanOrEqual("int_col", 10)).isDefined)
    assert(c.convertFilter(EqualNullSafe("int_col", 10)).isDefined)
    assert(c.convertFilter(IsNull("int_col")).isDefined)
    assert(c.convertFilter(IsNotNull("int_col")).isDefined)
  }

  test("convertFilter leaves float and double greater-than comparisons residual") {
    // cuDF ASTs use IEEE-754, where NaN loses every ordered comparison; Spark keeps NaN rows for
    // `>`/`>=`, so those must stay residual. `=`, `<`, `<=` and `!=` agree and still push.
    val c = converter()
    for (col <- Seq("float_col", "double_col")) {
      assert(c.convertFilter(GreaterThan(col, 1.0)).isEmpty, s"$col > should be residual")
      assert(c.convertFilter(GreaterThanOrEqual(col, 1.0)).isEmpty, s"$col >= should be residual")
      assert(c.convertFilter(EqualTo(col, 1.0)).isDefined, s"$col = should push")
      assert(c.convertFilter(LessThan(col, 1.0)).isDefined, s"$col < should push")
      assert(c.convertFilter(LessThanOrEqual(col, 1.0)).isDefined, s"$col <= should push")
      assert(c.convertFilter(EqualNullSafe(col, 1.0)).isDefined, s"$col <=> should push")
      assert(c.convertFilter(Not(EqualTo(col, 1.0))).isDefined, s"$col != should push")
      assert(c.convertFilter(IsNull(col)).isDefined, s"$col IsNull should push")
      assert(c.convertFilter(IsNotNull(col)).isDefined, s"$col IsNotNull should push")
    }
    // Integer `>` is unaffected -- the gate is only for floating point.
    assert(c.convertFilter(GreaterThan("int_col", 1)).isDefined)
  }

  test("convertFilter leaves any NaN-literal float or double comparison residual") {
    // Every cuDF comparison against NaN is false, so Spark's `= NaN`, `< NaN`, etc. all diverge.
    val c = converter()
    assert(c.convertFilter(EqualTo("float_col", Float.NaN)).isEmpty)
    assert(c.convertFilter(LessThan("float_col", Float.NaN)).isEmpty)
    assert(c.convertFilter(EqualNullSafe("double_col", Double.NaN)).isEmpty)
    assert(c.convertFilter(In("double_col", Array[Any](1.0, Double.NaN))).isEmpty)
    // A NaN-free In on the same column still pushes.
    assert(c.convertFilter(In("double_col", Array[Any](1.0, 2.0))).isDefined)
  }

  // Structural round-trip: the converted AST must match the one we would build by hand. These
  // compare `toString`, which encodes operator, column (index or name) and literal type+value, so
  // they catch a wrong operator, index, reference mode, or literal conversion -- none of which the
  // `isDefined` checks above can see. No GPU or evaluation is needed.

  // Mirrors FilterToAstConverter's null-aware `=`: NOT(NOT_EQUAL(col, lit)).
  private def eqAst(col: AstExpression, lit: Literal): AstExpression =
    new UnaryOperation(UnaryOperator.NOT, new BinaryOperation(BinaryOperator.NOT_EQUAL, col, lit))

  test("convertFilter maps each comparison operator to the matching cuDF operator") {
    val c = converter()
    def ast(f: Filter): String = c.convertFilter(f).get.toString
    assert(ast(EqualTo("int_col", 1)) ==
      eqAst(new ColumnReference(0), Literal.ofInt(1)).toString)
    assert(ast(EqualNullSafe("int_col", 1)) ==
      new BinaryOperation(BinaryOperator.NULL_EQUAL, new ColumnReference(0),
        Literal.ofInt(1)).toString)
    assert(ast(GreaterThan("int_col", 1)) ==
      new BinaryOperation(BinaryOperator.GREATER, new ColumnReference(0),
        Literal.ofInt(1)).toString)
    assert(ast(GreaterThanOrEqual("int_col", 1)) ==
      new BinaryOperation(BinaryOperator.GREATER_EQUAL, new ColumnReference(0),
        Literal.ofInt(1)).toString)
    assert(ast(LessThan("int_col", 1)) ==
      new BinaryOperation(BinaryOperator.LESS, new ColumnReference(0), Literal.ofInt(1)).toString)
    assert(ast(LessThanOrEqual("int_col", 1)) ==
      new BinaryOperation(BinaryOperator.LESS_EQUAL, new ColumnReference(0),
        Literal.ofInt(1)).toString)
  }

  test("convertFilter builds IsNull and IsNotNull as cuDF unary operations") {
    val c = converter()
    assert(c.convertFilter(IsNull("int_col")).get.toString ==
      new UnaryOperation(UnaryOperator.IS_NULL, new ColumnReference(0)).toString)
    assert(c.convertFilter(IsNotNull("int_col")).get.toString ==
      new UnaryOperation(UnaryOperator.NOT,
        new UnaryOperation(UnaryOperator.IS_NULL, new ColumnReference(0))).toString)
  }

  test("convertFilter builds Not, And and Or with null-aware boolean operators") {
    val c = converter()
    assert(c.convertFilter(Not(EqualTo("int_col", 1))).get.toString ==
      new UnaryOperation(UnaryOperator.NOT,
        eqAst(new ColumnReference(0), Literal.ofInt(1))).toString)
    val eqInt = eqAst(new ColumnReference(0), Literal.ofInt(1))
    val eqLong = eqAst(new ColumnReference(1), Literal.ofLong(2L))
    assert(c.convertFilter(And(EqualTo("int_col", 1), EqualTo("long_col", 2L))).get.toString ==
      new BinaryOperation(BinaryOperator.NULL_LOGICAL_AND, eqInt, eqLong).toString)
    assert(c.convertFilter(Or(EqualTo("int_col", 1), EqualTo("long_col", 2L))).get.toString ==
      new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR, eqInt, eqLong).toString)
  }

  test("convertFilter expands In to a null-aware OR of equalities") {
    val c = converter()
    def eq(v: Int) = eqAst(new ColumnReference(0), Literal.ofInt(v))
    val expected = new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR,
      new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR, eq(1), eq(2)), eq(3))
    assert(c.convertFilter(In("int_col", Array[Any](1, 2, 3))).get.toString == expected.toString)
  }

  test("convertFilter wraps In equalities so a negated In with null rejects a non-member") {
    val c = converter()
    // Not(int_col IN (1, null)) must reject 2: Spark computes the In as null, so Not is null and
    // the row drops. The null-aware equality keeps `2 = null` null rather than false, which a bare
    // cuDF EQUAL would have turned into a kept row.
    val expectedIn = new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR,
      eqAst(new ColumnReference(0), Literal.ofInt(1)),
      eqAst(new ColumnReference(0), Literal.ofNull(DType.INT32)))
    assert(c.convertFilter(In("int_col", Array[Any](1, null))).get.toString == expectedIn.toString)
    assert(c.convertFilter(Not(In("int_col", Array[Any](1, null)))).get.toString ==
      new UnaryOperation(UnaryOperator.NOT, expectedIn).toString)
  }

  test("convertFilter builds a literal of the right cuDF type for each column type") {
    val c = converter()
    def ast(f: Filter): String = c.convertFilter(f).get.toString
    def eq(idx: Int, lit: Literal): String = eqAst(new ColumnReference(idx), lit).toString
    assert(ast(EqualTo("int_col", 42)) == eq(0, Literal.ofInt(42)))
    assert(ast(EqualTo("long_col", 100L)) == eq(1, Literal.ofLong(100L)))
    assert(ast(EqualTo("float_col", 3.14f)) == eq(2, Literal.ofFloat(3.14f)))
    assert(ast(EqualTo("double_col", 2.718)) == eq(3, Literal.ofDouble(2.718)))
    assert(ast(EqualTo("string_col", "x")) == eq(4, Literal.ofString("x")))
    assert(ast(EqualTo("bool_col", true)) == eq(5, Literal.ofBoolean(true)))
    assert(ast(EqualTo("byte_col", 5)) == eq(9, Literal.ofByte(5.toByte)))
    assert(ast(EqualTo("short_col", 5)) == eq(10, Literal.ofShort(5.toShort)))
    // A date becomes days since the epoch (TIMESTAMP_DAYS); a timestamp stays micros.
    assert(ast(EqualTo("date_col", Date.valueOf("2026-08-04"))) ==
      eq(6, Literal.ofTimestampDaysFromInt(LocalDate.of(2026, 8, 4).toEpochDay.toInt)))
    assert(ast(EqualTo("ts_col", 1786000000000000L)) ==
      eq(7, Literal.ofTimestampFromLong(DType.TIMESTAMP_MICROSECONDS, 1786000000000000L)))
  }

  test("convertFilter references a column by index or by name per useColumnNames") {
    // Index mode resolves the attribute to its schema position.
    val byIndex = converter(useColumnNames = false).convertFilter(EqualTo("float_col", 1.0f))
    assert(byIndex.get.toString ==
      eqAst(new ColumnReference(2), Literal.ofFloat(1.0f)).toString)
    // Name mode carries the attribute name instead of an index.
    val byName = converter(useColumnNames = true).convertFilter(EqualTo("int_col", 1))
    assert(byName.get.toString ==
      eqAst(new ColumnNameReference("int_col"), Literal.ofInt(1)).toString)
    // A case-insensitive match still resolves, keeping the original-case name.
    val byNameCi = converter(caseSensitive = false, useColumnNames = true)
        .convertFilter(EqualTo("INT_COL", 1))
    assert(byNameCi.get.toString ==
      eqAst(new ColumnNameReference("INT_COL"), Literal.ofInt(1)).toString)
  }

  test("planPushdown splits a float greater-than out of an And and pushes the rest") {
    withPlan(And(GreaterThan("float_col", 1.0f), EqualTo("int_col", 42))) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(GreaterThan("float_col", 1.0f)))
    }
  }

  test("convertFilter leaves a whole Or residual when a float greater-than side cannot push") {
    assert(converter().convertFilter(
      Or(GreaterThan("float_col", 1.0f), EqualTo("int_col", 42))).isEmpty)
  }

  test("convertFilter keeps a null element in an In as a null literal") {
    // The null converts to a null literal rather than making the whole In residual.
    assert(converter().convertFilter(In("int_col", Array[Any](1, null))).isDefined)
  }

  test("convertFilter converts date literals from Date, LocalDate and day counts") {
    val c = converter()
    assert(c.convertFilter(EqualTo("date_col", Date.valueOf("2026-08-04"))).isDefined)
    assert(c.convertFilter(EqualTo("date_col", LocalDate.of(2026, 8, 4))).isDefined)
    assert(c.convertFilter(EqualTo("date_col", 20669)).isDefined)
  }

  test("convertFilter converts timestamp literals from Timestamp, Instant and micros") {
    val c = converter()
    assert(c.convertFilter(EqualTo("ts_col", Timestamp.valueOf("2026-08-04 12:00:00"))).isDefined)
    assert(c.convertFilter(EqualTo("ts_col", Instant.ofEpochSecond(1786000000L))).isDefined)
    assert(c.convertFilter(EqualTo("ts_col", 1786000000000000L)).isDefined)
  }

  test("convertFilter converts null literals for typed columns") {
    assert(converter().convertFilter(EqualNullSafe("int_col", null)).isDefined)
  }

  test("convertFilter rejects a column outside the read schema") {
    assert(converter().convertFilter(EqualTo("unknown_col", 1)).isEmpty)
  }

  test("convertFilter rejects decimal literals") {
    // A decimal literal needs the column's precision and scale.
    assert(converter().convertFilter(EqualTo("dec_col", BigDecimal(1.5).bigDecimal)).isEmpty)
  }

  test("convertFilter rejects a literal whose type does not match the column") {
    assert(converter().convertFilter(EqualTo("int_col", "not a number")).isEmpty)
  }

  test("convertFilter honors case sensitivity") {
    assert(converter(caseSensitive = true).convertFilter(EqualTo("INT_COL", 1)).isEmpty)
    assert(converter(caseSensitive = false).convertFilter(EqualTo("INT_COL", 1)).isDefined)
  }

  test("convertFilter converts compound filters") {
    val c = converter()
    assert(c.convertFilter(And(EqualTo("int_col", 1), EqualTo("long_col", 100L))).isDefined)
    assert(c.convertFilter(Or(EqualTo("int_col", 1), EqualTo("int_col", 2))).isDefined)
    assert(c.convertFilter(Not(EqualTo("int_col", 1))).isDefined)
  }

  test("convertFilter negates every filter it can convert") {
    val c = converter()
    Seq(
      EqualTo("int_col", 1),
      EqualNullSafe("int_col", 1),
      GreaterThan("int_col", 1),
      GreaterThanOrEqual("int_col", 1),
      LessThan("int_col", 1),
      LessThanOrEqual("int_col", 1),
      IsNull("int_col"),
      IsNotNull("int_col"),
      In("int_col", Array[Any](1, 2, 3)),
      And(EqualTo("int_col", 1), LessThan("long_col", 5L)),
      Or(EqualTo("int_col", 1), LessThan("long_col", 5L)),
      Not(EqualTo("int_col", 1))
    ).foreach { filter =>
      assert(c.convertFilter(Not(filter)).isDefined, s"Not($filter) should convert")
    }
  }

  test("convertFilter leaves a negation of an unconvertible filter residual") {
    val c = converter()
    assert(c.convertFilter(Not(In("int_col", Array.empty[Any]))).isEmpty)
    // One unconvertible side makes the whole negated filter residual.
    val withStringSide = And(EqualTo("int_col", 1), StringContains("string_col", "x"))
    assert(c.convertFilter(Not(withStringSide)).isEmpty)
    val withDecimalSide = Or(EqualTo("int_col", 1),
      EqualTo("dec_col", BigDecimal(1.5).bigDecimal))
    assert(c.convertFilter(Not(withDecimalSide)).isEmpty)
  }

  test("convertFilter rejects a whole Or when either side is unconvertible") {
    // Pushing only one side of an Or would drop rows the other side accepts.
    val c = converter()
    assert(c.convertFilter(
      Or(EqualTo("int_col", 1), StringStartsWith("string_col", "foo"))).isEmpty)
    assert(c.convertFilter(
      Or(StringStartsWith("string_col", "foo"), EqualTo("int_col", 1))).isEmpty)
    assert(c.convertFilter(Not(StringContains("string_col", "foo"))).isEmpty)
  }

  test("convertFilter rejects a whole In when any value is unconvertible") {
    // Dropping a value would reject the rows matching it.
    assert(converter().convertFilter(In("int_col", Array[Any](1, 2, "not a number"))).isEmpty)
    assert(converter().convertFilter(In("int_col", Array[Any](1, 2, 3))).isDefined)
  }

  test("convertFilter rejects an empty In") {
    assert(converter().convertFilter(In("int_col", Array.empty)).isEmpty)
  }

  test("planPushdown returns no AST and no residual for no filters") {
    withPlan() { plan =>
      assert(plan.ast.isEmpty)
      assert(plan.pushed.isEmpty)
      assert(plan.residual.isEmpty)
    }
  }

  test("planPushdown pushes every convertible conjunct") {
    withPlan(EqualTo("int_col", 42), GreaterThan("long_col", 100L)) { plan =>
      assert(plan.ast.isDefined)
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
  }

  test("planPushdown reports an unconvertible conjunct as residual instead of dropping it") {
    val unsupported = StringStartsWith("string_col", "foo")
    withPlan(EqualTo("int_col", 42), unsupported) { plan =>
      assert(plan.ast.isDefined)
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(unsupported))
    }
  }

  test("planPushdown splits a nested And so its convertible half still pushes") {
    // The convertible half of a nested And is still pushed.
    val unsupported = StringContains("string_col", "foo")
    withPlan(And(EqualTo("int_col", 42), unsupported)) { plan =>
      assert(plan.ast.isDefined)
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(unsupported))
    }
  }

  test("planPushdown reports a column outside the read schema as residual") {
    val outside = EqualTo("unknown_col", 1)
    withPlan(EqualTo("int_col", 42), outside) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(outside))
    }
  }

  test("planPushdown returns no AST when nothing is convertible") {
    val filters = Seq[Filter](
      StringStartsWith("string_col", "foo"),
      StringEndsWith("string_col", "bar"))
    withPlan(filters: _*) { plan =>
      assert(plan.ast.isEmpty)
      assert(plan.pushed.isEmpty)
      assert(plan.residual.toSeq == filters)
    }
  }

  test("planPushdown leaves out a null check another pushed conjunct already implies") {
    // Spark infers this pair; `int_col = 42` already rules out a null.
    val redundant = IsNotNull("int_col")
    withPlan(redundant, EqualTo("int_col", 42)) { plan =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(redundant))
    }
    // `In` holds an Array, so compare against the same instances rather than equal copies.
    val notNull = Not(IsNull("int_col"))
    val in = In("int_col", Array[Any](1, 2))
    withPlan(notNull, in) { plan =>
      assert(plan.pushed.toSeq == Seq(in))
      assert(plan.residual.toSeq == Seq(notNull))
    }
  }

  test("planPushdown keeps a null check nothing else implies") {
    // A conjunct on a different column doesn't imply this null check.
    withPlan(IsNotNull("int_col"), EqualTo("long_col", 42L)) { plan =>
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
    // A null check is not implied by a conjunct that a null can satisfy.
    withPlan(IsNotNull("int_col"), EqualNullSafe("int_col", null)) { plan =>
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
    // Only one side of the Or rules out a null, so the Or as a whole does not.
    withPlan(IsNotNull("int_col"), Or(EqualTo("int_col", 1), IsNull("int_col"))) { plan =>
      assert(plan.pushed.length == 2)
      assert(plan.residual.isEmpty)
    }
  }

  test("planPushdown keeps a null check whose implying conjunct is not itself pushed") {
    // `startswith` stays residual, so it can't stand in for the null check.
    val unsupported = StringStartsWith("string_col", "foo")
    withPlan(IsNotNull("string_col"), unsupported) { plan =>
      assert(plan.pushed.toSeq == Seq(IsNotNull("string_col")))
      assert(plan.residual.toSeq == Seq(unsupported))
    }
  }

  test("planPushdown matches the null check column case-insensitively when asked to") {
    val plan = converter(caseSensitive = false)
        .planPushdown(Array[Filter](IsNotNull("INT_COL"), EqualTo("int_col", 42)))
    withResource(plan.ast) { _ =>
      assert(plan.pushed.toSeq == Seq(EqualTo("int_col", 42)))
      assert(plan.residual.toSeq == Seq(IsNotNull("INT_COL")))
    }
  }

  test("splitConjuncts flattens nested Ands but leaves Or and Not intact") {
    val or = Or(EqualTo("int_col", 1), EqualTo("int_col", 2))
    val not = Not(EqualTo("long_col", 3L))
    val split = FilterToAstConverter.splitConjuncts(
      Array(And(And(EqualTo("int_col", 1), IsNotNull("long_col")), or), not))
    assert(split.toSeq == Seq(EqualTo("int_col", 1), IsNotNull("long_col"), or, not))
  }

  test("residualsEvaluableOn requires every residual column to be read") {
    assert(FilterToAstConverter.residualsEvaluableOn(
      Array(StringContains("string_col", "x")), testSchema, isCaseSensitive = true))
    assert(!FilterToAstConverter.residualsEvaluableOn(
      Array(StringContains("absent_col", "x")), testSchema, isCaseSensitive = true))
    assert(FilterToAstConverter.residualsEvaluableOn(
      Array.empty, testSchema, isCaseSensitive = true))
  }

  test("residualsEvaluableOn honors case sensitivity") {
    val residual = Array[Filter](StringContains("STRING_COL", "x"))
    assert(!FilterToAstConverter.residualsEvaluableOn(residual, testSchema,
      isCaseSensitive = true))
    assert(FilterToAstConverter.residualsEvaluableOn(residual, testSchema,
      isCaseSensitive = false))
  }

  test("getReferencedColumns collects columns through compound filters") {
    assert(FilterToAstConverter.getReferencedColumns(EqualTo("int_col", 1)) == Set("int_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      And(EqualTo("int_col", 1), GreaterThan("long_col", 100L))) == Set("int_col", "long_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      Or(EqualTo("int_col", 1), EqualTo("long_col", 100L))) == Set("int_col", "long_col"))
    assert(FilterToAstConverter.getReferencedColumns(Not(EqualTo("int_col", 1))) == Set("int_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      In("int_col", Array(1, 2, 3))) == Set("int_col"))
    assert(FilterToAstConverter.getReferencedColumns(
      StringContains("string_col", "x")) == Set("string_col"))
  }
}
