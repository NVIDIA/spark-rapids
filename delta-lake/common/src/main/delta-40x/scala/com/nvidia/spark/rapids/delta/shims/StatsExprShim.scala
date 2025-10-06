/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 */

package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.sql.catalyst.expressions.{Expression, RuntimeReplaceable}

/**
 * Spark 4.0 shim: unwrap RuntimeReplaceable to make expressions evaluable at task time.
 */
object StatsExprShim {
  def unwrapRuntimeReplaceable(expr: Expression): Expression =
    expr.transform { case rr: RuntimeReplaceable => rr.replacement }
}


