/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Rewrites an expression using rules that are guaranteed preserve the result while attempting
 * to remove cosmetic variations. Deterministic expressions that are `equal` after canonicalization
 * will always return the same answer given the same input (i.e. false positives should not be
 * possible). However, it is possible that two canonical expressions that are not equal will in fact
 * return the same answer given any input (i.e. false negatives are possible).
 *
 * The following rules are applied:
 *  - Names and nullability hints for `org.apache.spark.sql.types.DataTypes` are stripped.
 *  - Names for `GetStructField` are stripped.
 *  - TimeZoneId for `Cast` and `AnsiCast` are stripped if `needsTimeZone` is false.
 *  - Commutative and associative operations (`Add` and `Multiply`) have their children ordered
 *    by `hashCode`.
 *  - `EqualTo` and `EqualNullSafe` are reordered by hashCode.
 *  - Other comparisons (`GreaterThan`, `LessThan`) are reversed by `hashCode`.
 *  - Elements in `In` are reordered by `hashCode`.
 *
 *  This is essentially a copy of the Spark `Canonicalize` class but updated for GPU operators
 */
object GpuCanonicalize {
  def execute(e: Expression): Expression = {
    expressionReorder(ignoreTimeZone(ignoreNamesTypes(e)))
  }

  /** Remove names and nullability from types, and names from `GetStructField`. */
  def ignoreNamesTypes(e: Expression): Expression = e match {
    case a: AttributeReference =>
      AttributeReference("none", TrampolineUtil.asNullable(a.dataType))(exprId = a.exprId)
    case GetStructField(child, ordinal, Some(_)) => GetStructField(child, ordinal, None)
    case _ => e
  }

  /** Remove TimeZoneId for Cast if needsTimeZone return false. */
  def ignoreTimeZone(e: Expression): Expression = e match {
    case c: CastBase if c.timeZoneId.nonEmpty && !c.needsTimeZone =>
      c.withTimeZone(null)
    case c: GpuCast if c.timeZoneId.nonEmpty =>
      // TODO when we start to support time zones check for `&& !c.needsTimeZone`
      c.withTimeZone(null)
    case _ => e
  }

  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = e match {
    case c if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other :: Nil
  }

  /** Orders a set of commutative operations by their hash code. */
  private def orderCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(e, f).sortBy(_.hashCode())

  /** Rearrange expressions that are commutative or associative. */
  private def expressionReorder(e: Expression): Expression = e match {
    case a: GpuAdd => orderCommutative(a, { case GpuAdd(l, r) => Seq(l, r) }).reduce(GpuAdd)
    case m: GpuMultiply =>
      orderCommutative(m, { case GpuMultiply(l, r) => Seq(l, r) }).reduce(GpuMultiply)
    case o: GpuOr =>
      orderCommutative(o, { case GpuOr(l, r) if l.deterministic && r.deterministic => Seq(l, r) })
          .reduce(GpuOr)
    case a: GpuAnd =>
      orderCommutative(a, { case GpuAnd(l, r) if l.deterministic && r.deterministic => Seq(l, r)})
          .reduce(GpuAnd)

    case GpuEqualTo(l, r) if l.hashCode() > r.hashCode() => GpuEqualTo(r, l)
    case GpuEqualNullSafe(l, r) if l.hashCode() > r.hashCode() => GpuEqualNullSafe(r, l)

    case GpuGreaterThan(l, r) if l.hashCode() > r.hashCode() => GpuLessThan(r, l)
    case GpuLessThan(l, r) if l.hashCode() > r.hashCode() => GpuGreaterThan(r, l)

    case GpuGreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GpuLessThanOrEqual(r, l)
    case GpuLessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GpuGreaterThanOrEqual(r, l)

    // Note in the following `NOT` cases, `l.hashCode() <= r.hashCode()` holds. The reason is that
    // canonicalization is conducted bottom-up -- see [[Expression.canonicalized]].
    case GpuNot(GpuGreaterThan(l, r)) => GpuLessThanOrEqual(l, r)
    case GpuNot(GpuLessThan(l, r)) => GpuGreaterThanOrEqual(l, r)
    case GpuNot(GpuGreaterThanOrEqual(l, r)) => GpuLessThan(l, r)
    case GpuNot(GpuLessThanOrEqual(l, r)) => GpuGreaterThan(l, r)

    // order the list in the In operator
    case GpuInSet(value, list) if list.length > 1 => GpuInSet(value, list.sortBy(_.hashCode()))

    case _ => e
  }
}