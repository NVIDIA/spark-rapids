package ai.rapids.spark

import org.apache.spark.sql.types.{DataTypes, StructType}

abstract class GpuExpressionTestSuite extends SparkQueryCompareTestSuite {

  /**
   * Evaluate the GpuExpression and compare the results to the provided function.
   *
   * @param inputExpr GpuExpression under test
   * @param expectedFun Function that produces expected results
   * @param schema Schema to use for generated data
   * @param rowCount Number of rows of random to generate
   * @param maxFloatDiff Maximum acceptable difference between expected and actual results
   */
  def checkEvaluateGpuUnaryMathExpression(inputExpr: GpuExpression,
    expectedFun: Double => Option[Double],
    schema: StructType,
    rowCount: Int = 4,
    seed: Long = 0,
    maxFloatDiff: Double = 0.00001): Unit = {

    // generate batch
    val batch = FuzzerUtils.createColumnarBatch(schema, rowCount, seed = seed)
    try {
      // evaluate expression
      val result = inputExpr.columnarEval(batch).asInstanceOf[GpuColumnVector]
      try {
        // bring gpu data onto host
        val hostInput = batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()
        try {
          val hostResult = result.copyToHost()
          try {
            // compare results
            assert(result.getRowCount == rowCount)
            for (i <- 0 until result.getRowCount.toInt) {
              val inputValue = if (hostInput.isNullAt(i)) {
                None
              } else {
                Some(hostInput.getDouble(i))
              }
              val actual = if (hostResult.isNullAt(i)) {
                null
              } else {
                hostResult.getDouble(i)
              }
              val expected = inputValue.flatMap(v => expectedFun(v)).getOrElse(null)
              if (!compare(expected, actual, maxFloatDiff)) {
                throw new IllegalStateException(s"Expected: $expected. Actual: $actual. Input value: $inputValue")
              }
            }
          } finally {
            hostResult.close()
          }
        } finally {
          hostInput.close()
        }
      } finally {
        result.close()
      }
    } finally {
      batch.close()
    }
  }

}
