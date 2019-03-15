package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._;

object ShortCircuit {
  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._
  def main(args: Array[String]) : Unit = {
    val f: Double => Double = { x =>
      val t =
        if (x > 1.0 && x < 3.7) {
          if (x > 1.1 && x < 2.0) 1.0 else 1.1
        } else {
	  if (x < 0.1) 2.3 else 4.1
        }

      t + 2.2
    }
    val u = udf(f)
    val dataset = List(1.0, 2, 3, 4).toDS()
    val result = dataset.withColumn("new", u('value))
    result.show; println(result.queryExecution)
  }
}
