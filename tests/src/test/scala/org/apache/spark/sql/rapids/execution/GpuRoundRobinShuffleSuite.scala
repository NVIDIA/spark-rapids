/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM
import com.nvidia.spark.rapids.shims.GpuHashPartitioning

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.rdd.{DeterministicLevel, MapPartitionsRDD}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRoundRobinShuffleSuite extends SparkQueryCompareTestSuite {
  private def conf(sortBefore: Boolean): SparkConf = new SparkConf()
    .setMaster("local[1]")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.execution.sortBeforeRepartition", sortBefore.toString)

  private def dependency(
      spark: SparkSession,
      batchSizes: Seq[Int],
      partitioning: GpuPartitioning = GpuRoundRobinPartitioning(4),
      inputLevel: DeterministicLevel.Value = DeterministicLevel.DETERMINATE,
      trackOpTime: Boolean = false): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val input = new MapPartitionsRDD[ColumnarBatch, Int](
      spark.sparkContext.parallelize(0 until 8, 8), (_, index, _) => {
        if (index != 7) {
          Iterator.empty
        } else {
          var start = 0
          batchSizes.iterator.map { size =>
            val ids = (start until start + size).map(Int.box)
            start += size
            withResource(new Table.TestBuilder().column(ids: _*).build()) { table =>
              GpuColumnVector.from(table, Array(IntegerType))
            }
          }
        }
      }) {
      override protected def getOutputDeterministicLevel: DeterministicLevel.Value = inputLevel
    }
    val metrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
    GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(input,
      Seq(AttributeReference("id", IntegerType, nullable = false)()), partitioning,
      Array(IntegerType),
      new GpuColumnarBatchSerializer(metrics, Array(IntegerType), ShuffleKudoMode.CPU,
        useKudo = false, kudoMeasureBufferCopy = false),
      useGPUShuffle = false, useMultiThreadedShuffle = false,
      rangeInputBatchingEnabled = false, metrics, Map.empty, Map.empty,
      opTimeNewShuffleWrite = if (trackOpTime) Some(NoopMetric) else None)
  }

  private def assignments(
      dep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]): Array[(Int, Int)] = {
    dep.rdd.sparkContext.runJob(dep.rdd,
      (iter: Iterator[Product2[Int, ColumnarBatch]]) => iter.flatMap { pair =>
        val batch = pair._2
        // The exchange iterator owns these host slices and closes them when it advances.
        (0 until batch.numRows()).map(i => (batch.column(0).getInt(i), pair._1))
      }.toArray, Seq(7)).flatten
  }

  Seq(false, true).foreach { sortBefore =>
    test(s"round-robin batch cuts require indeterminate shuffle output, sort=$sortBefore") {
      withGpuSparkSession({ spark =>
        GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
        val first = dependency(spark, Seq(10))
        val retry = dependency(spark, Seq(5, 5))
        val firstRows = assignments(first)
        val retryRows = assignments(retry)
        val expected = (0 until 10).toArray
        assert(firstRows.map(_._1).sorted.toSeq === expected.toSeq)
        assert(retryRows.map(_._1).sorted.toSeq === expected.toSeq)

        // Keeping one completed reducer while rerunning the others can lose or duplicate rows.
        val mixed = firstRows.filter(_._2 == 0) ++ retryRows.filter(_._2 != 0)
        assert(mixed.map(_._1).sorted.toSeq != expected.toSeq)
        assert(first.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
        assert(retry.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
      }, conf(sortBefore))
    }

    test(s"round-robin preserves rows on CPU and GPU, sort=$sortBefore") {
      def rows(spark: SparkSession): Seq[Long] = {
        spark.range(0, 1000, 1, 8)
          .repartition(4, col("id") % 4).repartition(4)
          .collect().map(_.longValue()).sorted.toSeq
      }
      val cpu = withCpuSparkSession(rows, conf(sortBefore))
      val gpu = withGpuSparkSession({ spark =>
        val df = spark.range(0, 1000, 1, 8)
          .repartition(4, col("id") % 4).repartition(4)
        assert(df.queryExecution.executedPlan.exists {
          case exchange: GpuShuffleExchangeExecBase =>
            exchange.outputPartitioning.isInstanceOf[RoundRobinPartitioning]
          case _ => false
        })
        df.collect().map(_.longValue()).sorted.toSeq
      }, conf(sortBefore))
      assert(cpu === (0L until 1000L))
      assert(gpu === cpu)
    }

    test(s"round-robin fetch failure cannot reuse a completed reducer, sort=$sortBefore") {
      withGpuSparkSession({ spark =>
        val df = spark.range(0, 1000, 1, 8)
          .repartition(4, col("id") % 4).repartition(4)
        val exchange = df.queryExecution.executedPlan.collectFirst {
          case e: GpuShuffleExchangeExecBase
              if e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => e
        }.getOrElse(fail("GPU round-robin exchange not found"))
        val shuffleId = exchange.shuffleDependencyColumnar.shuffleId
        val result = df.queryExecution.toRdd.mapPartitionsWithIndex { (index, rows) =>
          // local[1] lets reducer 0 finish before this fetch failure triggers a map-stage retry.
          if (index == 1 && TaskContext.get().stageAttemptNumber() == 0) {
            throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId,
              shuffleId, 0L, 0, index, "injected round-robin fetch failure", null)
          }
          rows.map(_.getLong(0))
        }
        val error = intercept[SparkException](result.collect())
        assert(error.getMessage.contains("indeterminate"))
      }, conf(sortBefore))
    }
  }

  test("round-robin determinism survives operation-time tracking and all input levels") {
    withGpuSparkSession({ spark =>
      Seq(DeterministicLevel.DETERMINATE, DeterministicLevel.UNORDERED,
        DeterministicLevel.INDETERMINATE).foreach { level =>
        Seq(false, true).foreach { trackOpTime =>
          val dep = dependency(spark, Seq(10), inputLevel = level, trackOpTime = trackOpTime)
          assert(dep.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
        }
      }
    }, conf(sortBefore = true))
  }

  test("split-and-retry changes round-robin assignments for determinate input") {
    withGpuSparkSession({ spark =>
      GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
      val injections = spark.sparkContext.longAccumulator("round-robin split injection")
      val partitioning = new GpuRoundRobinPartitioning(4) {
        private var injected = false

        override def columnarEvalAny(batch: ColumnarBatch): Any = {
          if (!injected) {
            injected = true
            injections.add(1)
            withResource(batch) { _ =>
              throw new GpuSplitAndRetryOOM("injected round-robin split")
            }
          } else {
            super.columnarEvalAny(batch)
          }
        }
      }
      val first = assignments(dependency(spark, Seq(10)))
      val retry = dependency(spark, Seq(10), partitioning)
      val retried = assignments(retry)
      assert(injections.value === 1L)
      assert(retried.map(_._1).sorted.toSeq === (0 until 10))
      assert(retried.toMap != first.toMap)
      assert(retry.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
    }, conf(sortBefore = false))
  }

  test("single-partition and hash exchanges preserve input determinism") {
    withGpuSparkSession({ spark =>
      Seq(DeterministicLevel.DETERMINATE, DeterministicLevel.UNORDERED,
        DeterministicLevel.INDETERMINATE).foreach { level =>
        val partitionings = Seq(GpuRoundRobinPartitioning(1), GpuSinglePartitioning,
          GpuHashPartitioning(Seq(GpuLiteral(1, IntegerType)), 4))
        partitionings.foreach { partitioning =>
          val dep = dependency(spark, Seq(10), partitioning, level)
          assert(dep.rdd.outputDeterministicLevel === level)
        }
      }
    }, conf(sortBefore = false))
  }
}
