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
    StructField("dec_col", DecimalType(10, 2))
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

  test("canPruneRowGroups accepts comparisons and IsNull but not IsNotNull") {
    assert(FilterToAstConverter.canPruneRowGroups(EqualTo("int_col", 1)))
    assert(FilterToAstConverter.canPruneRowGroups(GreaterThan("int_col", 1)))
    assert(FilterToAstConverter.canPruneRowGroups(LessThanOrEqual("int_col", 1)))
    assert(FilterToAstConverter.canPruneRowGroups(In("int_col", Array(1, 2))))
    assert(FilterToAstConverter.canPruneRowGroups(IsNull("int_col")))
    assert(!FilterToAstConverter.canPruneRowGroups(IsNotNull("int_col")))
    assert(!FilterToAstConverter.canPruneRowGroups(In("int_col", Array.empty)))
    assert(!FilterToAstConverter.canPruneRowGroups(StringStartsWith("string_col", "a")))
  }

  test("canPruneRowGroups sees through a negation to what it means") {
    assert(FilterToAstConverter.canPruneRowGroups(Not(IsNotNull("int_col"))))
    assert(!FilterToAstConverter.canPruneRowGroups(Not(IsNull("int_col"))))
    assert(FilterToAstConverter.canPruneRowGroups(Not(EqualTo("int_col", 1))))
  }

  test("canPruneRowGroups needs one side of an And and both sides of an Or") {
    assert(FilterToAstConverter.canPruneRowGroups(
      And(IsNotNull("int_col"), GreaterThan("int_col", 1))))
    assert(!FilterToAstConverter.canPruneRowGroups(
      And(IsNotNull("int_col"), IsNotNull("string_col"))))
    assert(FilterToAstConverter.canPruneRowGroups(
      Or(EqualTo("int_col", 1), EqualTo("int_col", 2))))
    assert(!FilterToAstConverter.canPruneRowGroups(
      Or(EqualTo("int_col", 1), IsNotNull("string_col"))))
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
    // Round-tripping a decimal needs the column's precision and scale, so it stays residual.
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

  test("convertFilter negates every filter it can negate at the leaves") {
    // These have to convert rather than go residual, with the negation at the leaf. See
    // convertNegated.
    val c = converter()
    Seq(
      EqualTo("int_col", 1),
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

  test("convertFilter leaves a negation it cannot push at the leaves residual") {
    val c = converter()
    // The AST has no null-safe not-equal.
    assert(c.convertFilter(Not(EqualNullSafe("int_col", 1))).isEmpty)
    // An empty NOT IN accepts every row and there is no always-true literal to say so.
    assert(c.convertFilter(Not(In("int_col", Array.empty[Any]))).isEmpty)
    // One unconvertible side of a negated compound filter takes the whole filter with it.
    val withStringSide = And(EqualTo("int_col", 1), StringContains("string_col", "x"))
    assert(c.convertFilter(Not(withStringSide)).isEmpty)
    val withDecimalSide = Or(EqualTo("int_col", 1),
      EqualTo("dec_col", BigDecimal(1.5).bigDecimal))
    assert(c.convertFilter(Not(withDecimalSide)).isEmpty)
  }

  test("convertFilter rejects a whole Or when either side is unconvertible") {
    // Pushing only the convertible side of a disjunction would reject rows the other side
    // accepts, so the entire Or has to stay residual.
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
    // The whole conjunct is unconvertible, but its convertible half must still be pushed and
    // the rest reported, rather than the entire tree being discarded.
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
    // Spark infers this pair: `int_col = 42` cannot hold for a null, so pushing the null check
    // too only costs an AST node and a statistics column.
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
    // On its own the null check can still rule out a row group whose column is entirely null, and
    // a conjunct on a different column says nothing about this one.
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
    // `startswith` rules out a null but stays residual, so the null check is the only thing left
    // that can prune, and dropping it would leave the read with nothing pushed on the column.
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
