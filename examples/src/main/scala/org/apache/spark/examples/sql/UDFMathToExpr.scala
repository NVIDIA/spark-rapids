package org.apache.spark.examples.sql

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._;

object UDFMathToExpr {
  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._
  def main(args: Array[String]) : Unit = {
    val acosUDF = udf{x : Double => math.acos(x)}
    val asinUDF = udf{x : Double => math.asin(x)}
    val dataset = List(1.0, 2, 3).toDS()
    val result = dataset.withColumn("new", acosUDF('value) + asinUDF('value))
    result.show; println(result.queryExecution.analyzed)
    val ref = dataset.withColumn("new", acos(col("value")) + (asin(col("value"))))
    ref.show; println(ref.queryExecution.analyzed)
  }
}

