package com.nvidia.spark.rapids.unit

import com.nvidia.spark.rapids._
import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Expression, Literal, PromotePrecision}
import org.apache.spark.sql.rapids.GpuAdd
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType}


class DecimalUnitTest extends FunSuite with Arm {
  test("PromotePrecision elimination test") {
    val rapidsConf = new RapidsConf(Map[String, String]())
    val testWrapper = (input: Expression, expected: Expression) => {
      val output = GpuOverrides.wrapExpr(input, rapidsConf, None).convertToGpu()
      expected.semanticEquals(output) shouldBe true
    }
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      val exp1 = PromotePrecision(Cast(Literal(12.345f), DoubleType))
      testWrapper(exp1, GpuLiteral(12.345f, FloatType))
      val exp2 = Add(exp1, Cast(Literal(123), DoubleType))
      testWrapper(exp2,
        GpuAdd(GpuLiteral(12.345f, FloatType), GpuCast(GpuLiteral(123, IntegerType), DoubleType)))
    }
  }
}
