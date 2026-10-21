/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.SplittableJoinIterator.PreparedJoinBatch
import com.nvidia.spark.rapids.shims.OperatorsUtilShims

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuGreaterThan
import org.apache.spark.sql.rapids.execution.{ConditionalNestedLoopJoinIterator,
  GpuBroadcastNestedLoopJoinExec}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class BroadcastNestedLoopJoinSuite extends SparkQueryCompareTestSuite {

  test("BroadcastNestedLoopJoinExec AQE off") {
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark).repartition(2)
      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df3 = df1.crossJoin(broadcast(df2))
      df3.collect()
      val plan = df3.queryExecution.executedPlan

      val nljCount =
        OperatorsUtilShims.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])
      assert(nljCount.size === 1)
    }, conf)
  }

  test("BroadcastNestedLoopJoinExec AQE on") {
    val conf = new SparkConf()
        .set("spark.rapids.sql.exec.BroadcastNestedLoopJoinExec", "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        // In some cases AQE can make the children not look like they are on the GPU
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark).repartition(2)
      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df3 = df1.crossJoin(broadcast(df2))
      df3.collect()
      val plan = df3.queryExecution.executedPlan

      val nljCount =
        OperatorsUtilShims.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])

      assert(nljCount.size === 1)
    }, conf)
  }

  private val streamAttr = AttributeReference("a", IntegerType, nullable = true)()
  private val condition = GpuGreaterThan(
    GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "a"),
    GpuLiteral(5, IntegerType))
  private val inputs = Seq(
    Seq[Integer](null, 0, 5, 6, 9),
    Seq[Integer](6, 9),
    Seq[Integer](null, 0, 5),
    Seq.empty[Integer])

  private def withJoin(
      buildRows: Int,
      values: Seq[Integer])(
      f: (ConditionalNestedLoopJoinIterator, LazySpillableColumnarBatch) => Unit): Unit = {
    withGpuSparkSession { _ =>
      withResource(new ColumnarBatch(Array.empty, buildRows)) { build =>
        withResource(LazySpillableColumnarBatch(build, "build")) { builtBatch =>
          withResource(new ColumnarBatch(Array(GpuColumnVector.from(
              ColumnVector.fromBoxedInts(values: _*), IntegerType)), values.length)) { stream =>
            withResource(LazySpillableColumnarBatch(stream, "stream")) { streamBatch =>
              val iter = closeOnExcept(condition.convertToAst(1).compile()) { compiled =>
                new ConditionalNestedLoopJoinIterator(LeftAnti, GpuBuildRight,
                  LazySpillableColumnarBatch.spillOnly(builtBatch), Iterator.empty,
                  Seq(streamAttr), 1024 * 1024, 0.75, compiled, NoopMetric, NoopMetric)
              }
              withResource(iter)(f(_, streamBatch))
            }
          }
        }
      }
    }
  }

  private def expectedRows(buildRows: Int, values: Seq[Integer]): Seq[Integer] = {
    if (buildRows == 0) values else values.filter(v => v == null || v <= 5)
  }

  test("left anti row count accepts a rows-only build") {
    for (buildRows <- Seq(0, 1, 3); values <- inputs) {
      withClue(s"build rows $buildRows, stream $values: ") {
        withJoin(buildRows, values) { (iter, stream) =>
          withResource(iter.prepareJoinBatch(stream)) { prepared =>
            assertResult(expectedRows(buildRows, values).length.toLong)(prepared.numJoinRows)
          }
        }
      }
    }
  }

  test("left anti gather maps accept a rows-only build with and without a row count") {
    for (buildRows <- Seq(0, 1, 3); values <- inputs; prepared <- Seq(false, true)) {
      withClue(s"build rows $buildRows, stream $values, prepared $prepared: ") {
        withJoin(buildRows, values) { (iter, stream) =>
          val expected = expectedRows(buildRows, values)
          val state = if (prepared) Some(PreparedJoinBatch(expected.length)) else None
          val gatherer = iter.createGatherer(stream, state)
          withResource(gatherer) { _ =>
            if (expected.isEmpty) {
              assert(gatherer.isEmpty)
            } else {
              assert(gatherer.isDefined)
              withResource(gatherer.get.gatherNext(expected.length)) { result =>
                assertResult(1)(result.numCols())
                withResource(result.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
                  val actual = (0 until result.numRows()).map { i =>
                    if (host.isNullAt(i)) null else Integer.valueOf(host.getInt(i))
                  }
                  assertResult(expected)(actual)
                }
              }
              assert(gatherer.get.isDone)
            }
          }
        }
      }
    }
  }
}
