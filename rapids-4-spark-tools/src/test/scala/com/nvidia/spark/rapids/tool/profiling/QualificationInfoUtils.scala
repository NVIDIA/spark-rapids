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
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.rapids.tool.profiling._

case class RapidsFriends(name: String, friend: String, age: Int)

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

  val r = new scala.util.Random(11)
  def randomInt(): Int = r.nextInt(100)


  def generateFriendsDataset(spark: SparkSession): Dataset[RapidsFriends] = {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq.fill(1000){(randomAlpha(10), randomAlpha(5), randomInt)}).toDF("name", "friend", "age")
    df.as[RapidsFriends]
  }

  def genDatasetEventLog(spark: SparkSession, size: Int = 1000) = {
    import spark.implicits._
    val tempFile = File.createTempFile("dataSetEventLog", null)
    tempFile.deleteOnExit()
    val ds = generateFriendsDataset(spark)
    val dsAge = ds.filter(d => d.age > 25).map(d => (d.friend, d.age))
    dsAge.write.json(tempFile.getName())
  }

/*
SPARK_HOME/bin/spark-submit --master local[1] --driver-memory 30g --jars /home/tgraves/workspace/spark-rapids-another/rapids-4-spark-tools/target/rapids-4-spark-tools-21.06.0-SNAPSHOT-tests.jar,/home/tgraves/workspace/spark-rapids-another/rapids-4-spark-tools/target/rapids-4-spark-tools-21.06.0-SNAPSHOT.jar,/home/tgraves/.m2/repository/ai/rapids/cudf/21.06-SNAPSHOT/cudf-21.06-SNAPSHOT-cuda11.jar,/home/tgraves/workspace/spark-rapids-another/dist/target/rapids-4-spark_2.12-21.06.0-SNAPSHOT.jar --conf spark.driver.extraJavaOptions=-Duser.timezone=GMT --conf spark.sql.session.timeZone=UTC --conf spark.executor.extraJavaOptions=-Duser.timezone=GMT   --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.incompatibleOps.enabled=true  --conf spark.rapids.sql.explain="NOT_ON_GPU"   --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/home/tgraves/spark-eventlogs --class com.nvidia.spark.rapids.tool.profiling.QualificationInfoSuite /home/tgraves/workspace/spark-rapids-another/rapids-4-spark-tools/target/rapids-4-spark-tools-21.06.0-SNAPSHOT-tests.jar /home/tgraves/testeventlogDir 1001
*/
  def main(args: Array[String]): Unit = {
    val eventDir = if (args.length > 0) args(0) else "/tmp/spark-eventLogTest"
    val size = if (args.length > 1) args(1).toInt else 1000
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
    genDatasetEventLog(spark, size)
    spark.stop()
  }

}
