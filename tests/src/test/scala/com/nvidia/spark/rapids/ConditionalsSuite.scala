package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

class ConditionalsSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf()
    .set("spark.sql.ansi.enabled", "true")
    .set("spark.rapids.sql.expression.RLike", "true")

  testSparkResultsAreEqual("CASE WHEN test all branches", testData, conf) { df =>
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{1,3}$' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{4,6}$' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN first branch always true", testData2, conf) { df =>
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{1,3}$' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{4,6}$' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN second branch always true", testData2, conf) { df =>
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{4,6}$' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{1,3}$' THEN CAST(a AS INT) + 123 " +
        "ELSE -1 END"))
  }

  testSparkResultsAreEqual("CASE WHEN else condition always true", testData2, conf) { df =>
    df.withColumn("test", expr(
      "CASE " +
        "WHEN a RLIKE '^[0-9]{4,6}$' THEN CAST(a AS INT) " +
        "WHEN a RLIKE '^[0-9]{7,9}$' THEN CAST(a AS INT) + 123 " +
        "ELSE CAST(a AS INT) END"))
  }

  private def testData(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      "123",
      "123456",
      "123456789",
      null,
      "99999999999999999999"
    ).toDF("a").repartition(2)
  }

  private def testData2(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      "123",
      "456"
    ).toDF("a").repartition(2)
  }

}
