/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.rapids.TestUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

// class used for testing
case class RapidsFriends(name: String, friend: String, age: Int)

/**
 * Utilities to generate event logs used for qualification testing.
 */
object QualificationInfoUtils extends Logging {

  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  def randomAlpha(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }

  def randomString(length: Int) = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  val randForInt = new scala.util.Random(11)
  def randomInt(): Int = {
    randForInt.nextInt(100)
  }

  def generateFriendsDataset(spark: SparkSession): Dataset[RapidsFriends] = {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(
      Seq.fill(1000){(randomAlpha(10), randomAlpha(5), randomInt)})
      .toDF("name", "friend", "age")
    df.as[RapidsFriends]
  }

  // dataset operations in plan show up as Lambda
  def genDatasetEventLog(spark: SparkSession, size: Int = 1000) = {
    import spark.implicits._
    TestUtils.withTempPath { jsonOutFile =>
      val ds = generateFriendsDataset(spark)
      val dsAge = ds.filter(d => d.age > 25).map(d => (d.friend, d.age))
      dsAge.write.json(jsonOutFile.getCanonicalPath)
    }
  }

  def parseAge = (age: Int) => {
    val majorAge = Seq("21", "25", "55", "18")
    if (majorAge.contains(age)) {
      "MILESTONE"
    } else {
      "other"
    }
  }

  // UDF with dataset, shows up with Lambda
  def genUDFDSEventLog(spark: SparkSession, size: Int = 1000) = {
    import spark.implicits._
    TestUtils.withTempPath { jsonOutFile =>
      val ageFunc = udf(parseAge)
      val ds = generateFriendsDataset(spark)
      ds.withColumn("ageCategory",ageFunc(col("age")))
      val dsAge = ds.filter(d => d.age > 25).map(d => (d.friend, d.age))
      dsAge.write.json(jsonOutFile.getCanonicalPath)
    }
  }

  def cleanCountry = (country: String) => {
    val allUSA = Seq("US", "USa", "USA", "United states", "United states of America")
    if (allUSA.contains(country)) {
      "USA"
    } else {
      "unknown"
    }
  }

  // if register UDF with udf function it shows up in plan with UDF
  // Registering udf like:
  //   val normaliseCountry = spark.udf.register("normalisedCountry",cleanCountry)
  // doesn't seem to put anything unique in the plan
  def genUDFFuncEventLog(spark: SparkSession, size: Int = 1000) = {
    import spark.implicits._
    TestUtils.withTempPath { jsonInputFile =>
      TestUtils.withTempPath { jsonOutFile =>
        val userData = spark.createDataFrame(Seq(
          (1, "Chandler", "Pasadena", "US"),
          (2, "Monica", "New york", "USa"),
          (3, "Phoebe", "Suny", "USA"),
          (4, "Rachael", "St louis", "United states of America"),
          (5, "Joey", "LA", "Ussaa"),
          (6, "Ross", "Detroit", "United states")
        )).toDF("id", "name", "city", "country")
        userData.write.json(jsonInputFile.getCanonicalPath)
        val userDataRead = spark.read.json(jsonInputFile.getCanonicalPath)
        val allUSA = Seq("US", "USa", "USA", "United states", "United states of America") 
        userDataRead.createOrReplaceTempView("user_data")
        val cleanCountryUdf = udf(cleanCountry)
        val resDf = userDataRead.withColumn("normalisedCountry", cleanCountryUdf(col("country")))
        resDf.write.json(jsonOutFile.getCanonicalPath)
      }
    }
  }

  /*
   * Example command:
   * $SPARK_HOME/bin/spark-submit --master local[1] --driver-memory 30g \
   * --jars ./rapids-4-spark-tools/target/rapids-4-spark-tools-21.06.0-SNAPSHOT-tests.jar,\
   *  ./rapids-4-spark-tools/target/rapids-4-spark-tools-21.06.0-SNAPSHOT.jar \
   * --class com.nvidia.spark.rapids.tool.profiling.QualificationInfoUtils \
   * ./rapids-4-spark-tools/target/rapids-4-spark-tools-21.06.0-SNAPSHOT-tests.jar udffunc \
   * /tmp/testeventlogDir 100001
   */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println(s"ERROR: must specify a logType dataset, udfds, or udffunc")
      System.exit(1)
    }
    val logType = args(0)
    if (logType != "dataset" && logType != "udfds" && logType != "udffunc") {
      println(s"ERROR: logType must be one of: dataset, udfds, or udffunc")
      System.exit(1)
    }
    val eventDir = if (args.length > 1) args(1) else "/tmp/spark-eventLogTest"
    val size = if (args.length > 2) args(2).toInt else 1000
    val spark = {
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", eventDir)
        .getOrCreate()
    }
    import spark.implicits._
    if (logType.toLowerCase.equals("dataset")) {
      genDatasetEventLog(spark, size)
    } else if (logType.toLowerCase.equals("udfds")) {
      genUDFDSEventLog(spark, size)
    } else if (logType.toLowerCase.equals("udffunc")) {
      genUDFFuncEventLog(spark, size)
    } else {
      println(s"ERROR: Invalid log type specified: $logType")
      System.exit(1)
    }
    spark.stop()
  }
}
