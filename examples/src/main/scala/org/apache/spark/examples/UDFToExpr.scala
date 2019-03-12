package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._;

object UDFToExpr {
  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._
  def main(args: Array[String]) : Unit = {
    val inc = udf{x : Int => x + 1}
    val dec = udf{x : Int => x - 1}
    val dataset = List(1, 2, 3).toDS()
    val result = dataset.withColumn("new", inc('value) + dec('value))
    result.show; println(result.queryExecution.analyzed)
    val ref = dataset.withColumn("new", (col("value") + 1) + (col("value") - 1))
    ref.show; println(ref.queryExecution.analyzed)
  }
}

