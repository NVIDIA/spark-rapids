/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import org.apache.spark.sql.DataFrame

/**
 * Shared test utilities.
 */
object TestUtils {

  /** Compare two DataFrames row-by-row, reporting per-column mismatches. */
  def assertDataFrameEquals(
    actual: DataFrame,
    expected: DataFrame
  ): Unit = {
    assert(actual.schema == expected.schema,
      s"Schema mismatch:\n  actual:   ${actual.schema}\n  expected: ${expected.schema}")

    val actualRows  = actual.collect().sortBy(_.toString)
    val expectedRows = expected.collect().sortBy(_.toString)

    assert(actualRows.length == expectedRows.length,
      s"Row count mismatch: actual=${actualRows.length}, expected=${expectedRows.length}")

    val mismatches = scala.collection.mutable.ArrayBuffer.empty[String]
    for (i <- actualRows.indices) {
      val aRow = actualRows(i)
      val eRow = expectedRows(i)
      for (field <- actual.schema.fieldNames) {
        val aVal = Option(aRow.getAs[Any](field))
        val eVal = Option(eRow.getAs[Any](field))
        if (aVal != eVal) {
          mismatches += s"  [row $i] $field: actual=$aVal, expected=$eVal"
        }
      }
    }

    if (mismatches.nonEmpty) {
      throw new AssertionError(
        s"\nFound ${mismatches.length} column-level mismatches:\n${mismatches.mkString("\n")}\n")
    }
  }
}
