/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf => makeUdf}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

import org.scalatest.Assertions._
import org.apache.spark.sql.functions.{log => logalias}
import java.nio.charset.Charset

import com.nvidia.spark.rapids.RapidsConf



class OpcodeSuite extends FunSuite {

  val conf: SparkConf = new SparkConf()
    .set("spark.sql.extensions", "com.nvidia.spark.udf.Plugin")
    .set(RapidsConf.EXPLAIN.key, "true")

  val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("OpcodeSuite")
      .config(conf)
      .getOrCreate()

  import spark.implicits._

// Utility Function for checking equivalency of Dataset type
  def checkEquiv[T](ds1: Dataset[T], ds2: Dataset[T]) : Unit = {
    ds1.explain(true)
    ds2.explain(true)
    val resultdf = ds1.toDF()
    val refdf = ds2.toDF()
    ds1.show
    ds2.show
    val columns = refdf.schema.fields.map(_.name)
    val selectiveDifferences = columns.map(col => refdf.select(col).except(resultdf.select(col)))
    selectiveDifferences.map(diff => { assert(diff.count==0) } )
    ds1.show
    ds2.show
  }
}

