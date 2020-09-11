/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.File

import com.nvidia.spark.rapids.AdaptiveQueryExecSuite.TEST_FILES_ROOT
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.{PartialReducerPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuCustomShuffleReaderExec, GpuShuffledHashJoinBase}

object AdaptiveQueryExecSuite {
  val TEST_FILES_ROOT: File = TestUtils.getTempDir(this.getClass.getSimpleName)
}

class AdaptiveQueryExecSuite
    extends SparkQueryCompareTestSuite
    with AdaptiveSparkPlanHelper
    with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    TEST_FILES_ROOT.mkdirs()
  }

  override def afterEach(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(TEST_FILES_ROOT)
  }

  private def runAdaptiveAndVerifyResult(
      spark: SparkSession, query: String): (SparkPlan, SparkPlan) = {

    val dfAdaptive = spark.sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    // isFinalPlan is a private field so we have to use toString to access it
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))

    dfAdaptive.collect()
    val planAfter = dfAdaptive.queryExecution.executedPlan
    // isFinalPlan is a private field so we have to use toString to access it
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    // With AQE, the query is broken down into query stages based on exchange boundaries, so the
    // final query that is executed depends on the results from its child query stages. There
    // cannot be any exchange nodes left when the final query is executed because they will
    // have already been replaced with QueryStageExecs.
    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[GpuShuffledHashJoinBase] = {
    collect(plan) {
      case j: GpuShuffledHashJoinBase => j
    }
  }

  test("skewed inner join optimization") {
    skewJoinTest { spark =>
      val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 join skewData2 ON key1 = key2")
      val innerSmj = findTopLevelSortMergeJoin(innerAdaptivePlan)
      checkSkewJoin(innerSmj, 2, 1)
    }
  }

  test("skewed left outer join optimization") {
    skewJoinTest { spark =>
      val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 left outer join skewData2 ON key1 = key2")
      val leftSmj = findTopLevelSortMergeJoin(leftAdaptivePlan)
      checkSkewJoin(leftSmj, 2, 0)
    }
  }

  test("skewed right outer join optimization") {
    skewJoinTest { spark =>
      val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 right outer join skewData2 ON key1 = key2")
      val rightSmj = findTopLevelSortMergeJoin(rightAdaptivePlan)
      checkSkewJoin(rightSmj, 0, 1)
    }
  }

  test("Join partitioned tables") {
    assumeSpark301orLater

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1") // force shuffle exchange

    withGpuSparkSession(spark => {
      import spark.implicits._

      val path = new File(TEST_FILES_ROOT, "test.parquet").getAbsolutePath
      (0 until 100)
          .map(i => (i,i*5))
          .toDF("a", "b")
          .write
          .mode(SaveMode.Overwrite)
          .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("testData")

      spark.sql("DROP TABLE IF EXISTS t1").collect()
      spark.sql("DROP TABLE IF EXISTS t2").collect()

      spark.sql("CREATE TABLE t1 (a INT, b INT) USING parquet").collect()
      spark.sql("CREATE TABLE t2 (a INT, b INT) USING parquet PARTITIONED BY (a)").collect()

      spark.sql("INSERT INTO TABLE t1 SELECT a, b FROM testData").collect()
      spark.sql("INSERT INTO TABLE t2 SELECT a, b FROM testData").collect()

      val df = spark.sql(
        "SELECT t1.a, t2.b " +
            "FROM t1 " +
            "JOIN t2 " +
            "ON t1.a = t2.a " +
            "WHERE t2.a = 5" // filter on partition key to force dynamic partition pruning
      )
      df.collect()

      // assert that DPP did cause this to run as a non-AQE plan
      assert(!df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])

      // assert that both inputs to the SHJ are coalesced
      val shj = TestUtils.findOperator(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuShuffledHashJoinBase]).get
      assert(shj.children.length == 2)
      assert(shj.children.forall(_.isInstanceOf[GpuCoalesceBatches]))

    }, conf)
  }

  test("Plugin should translate child plan of GPU DataWritingCommandExec to GPU") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")

    withGpuSparkSession(spark => {
      import spark.implicits._

      // read from a parquet file so we can test reading on GPU
      val path = new File(TEST_FILES_ROOT, "DataWritingCommandExecGPU.parquet").getAbsolutePath
      (0 until 100).toDF("a")
          .write
          .mode(SaveMode.Overwrite)
          .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("testData")

      spark.sql("CREATE TABLE IF NOT EXISTS DataWritingCommandExecGPU (a INT) USING parquet")
          .collect()

      val df = spark.sql("INSERT INTO TABLE DataWritingCommandExecGPU SELECT * FROM testData")
      df.collect()

      // write should be on GPU
      val writeCommand = TestUtils.findOperator(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuDataWritingCommandExec])
      assert(writeCommand.isDefined)

      // the read should be an adaptive plan
      val adaptiveSparkPlanExec = TestUtils.findOperator(writeCommand.get,
        _.isInstanceOf[AdaptiveSparkPlanExec])
        .get.asInstanceOf[AdaptiveSparkPlanExec]

      // assert that at least part of the adaptive plan ran on GPU
      assert(TestUtils.findOperator(adaptiveSparkPlanExec, _.isInstanceOf[GpuExec]).isDefined)
    }, conf)
  }

  test("Plugin should translate child plan of CPU DataWritingCommandExec to GPU") {

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
      // force DataWritingCommandExec onto CPU for this test because we want to verify that
      // the read will still happen on GPU with a CPU write
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "DataWritingCommandExec")
      .set("spark.rapids.sql.exec.DataWritingCommandExec", "false")

      withGpuSparkSession(spark => {
        import spark.implicits._

        // read from a parquet file so we can test reading on GPU
        val path = new File(TEST_FILES_ROOT, "DataWritingCommandExecCPU.parquet").getAbsolutePath
        (0 until 100).toDF("a")
            .write
            .mode(SaveMode.Overwrite)
            .parquet(path)

        spark.read.parquet(path).createOrReplaceTempView("testData")

        spark.sql("CREATE TABLE IF NOT EXISTS DataWritingCommandExecCPU (a INT) USING parquet")
            .collect()

        val df = spark.sql("INSERT INTO TABLE DataWritingCommandExecCPU SELECT * FROM testData")
        df.collect()

        // write should be on CPU
        val writeCommand = TestUtils.findOperator(df.queryExecution.executedPlan,
          _.isInstanceOf[DataWritingCommandExec])
        assert(writeCommand.isDefined)

        // the read should be an adaptive plan
        val adaptiveSparkPlanExec = TestUtils.findOperator(writeCommand.get,
          _.isInstanceOf[AdaptiveSparkPlanExec])
            .get.asInstanceOf[AdaptiveSparkPlanExec]

        // even though the write couldn't run on GPU, the read should have done
        assert(TestUtils.findOperator(adaptiveSparkPlanExec.executedPlan,
          _.isInstanceOf[GpuExec]).isDefined)

    }, conf)
  }

  def skewJoinTest(fun: SparkSession => Unit) {
    assumeSpark301orLater

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "100")
      .set(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key, "800")
      .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "800")

    withGpuSparkSession(spark => {
      import spark.implicits._

      spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 250, 249)
                .when('id >= 750, 1000)
                .otherwise('id).as("key1"),
            'id as "value1")
          .createOrReplaceTempView("skewData1")

      // note that the skew amount here has been modified compared to the original Spark test to
      // compensate for the effects of compression when running on GPU which can change the
      // partition sizes substantially
      spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 500, 249)
                .otherwise('id).as("key2"),
            'id as "value2")
          .createOrReplaceTempView("skewData2")

      // invoke the test function
      fun(spark)

    }, conf)
  }

  /** most of the AQE tests requires Spark 3.0.1 or later */
  private def assumeSpark301orLater = {
    val sparkShimVersion = ShimLoader.getSparkShims.getSparkShimVersion
    val isValidTestForSparkVersion = sparkShimVersion match {
      case SparkShimVersion(3, 0, 0) => false
      case DatabricksShimVersion(3, 0, 0) => false
      case _ => true
    }
    assume(isValidTestForSparkVersion)
  }

  def checkSkewJoin(
      joins: Seq[GpuShuffledHashJoinBase],
      leftSkewNum: Int,
      rightSkewNum: Int): Unit = {
    assert(joins.size == 1 && joins.head.isSkewJoin)

    val leftSkew = joins.head.left.collect {
      case r: GpuCustomShuffleReaderExec => r
    }.head.partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct
    assert(leftSkew.length == leftSkewNum)

    val rightSkew = joins.head.right.collect {
      case r: GpuCustomShuffleReaderExec => r
    }.head.partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct
    assert(rightSkew.length == rightSkewNum)
  }

}
