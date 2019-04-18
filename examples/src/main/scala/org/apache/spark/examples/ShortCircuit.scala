/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
          (if (x > 1.1 && x < 2.0) 1.0 else 1.1) + 24.0
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
