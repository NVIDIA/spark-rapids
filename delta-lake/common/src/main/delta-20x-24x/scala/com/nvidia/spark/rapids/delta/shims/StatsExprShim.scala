/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 */

package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Spark 3.2.x shim: no-op unwrap. RuntimeReplaceable replacement not available.
 */
object StatsExprShim {
  def unwrapRuntimeReplaceable(expr: Expression): Expression = expr
}


