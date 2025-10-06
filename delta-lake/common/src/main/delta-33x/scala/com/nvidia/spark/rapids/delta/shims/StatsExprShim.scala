/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 */

package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Spark 3.3.x shim: no-op unwrap. RuntimeReplaceable replacement not needed here.
 */
object StatsExprShim {
  def unwrapRuntimeReplaceable(expr: Expression): Expression = expr
}


