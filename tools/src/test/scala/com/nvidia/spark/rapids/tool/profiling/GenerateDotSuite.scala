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
import java.security.SecureRandom

import scala.collection.mutable
import scala.io.Source

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}

class GenerateDotSuite extends FunSuite with BeforeAndAfterAll with Logging {

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  test("Generate DOT") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val eventLog = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
        import spark.implicits._
        val t1 = Seq((1, 2), (3, 4)).toDF("a", "b")
        t1.createOrReplaceTempView("t1")
        spark.sql("SELECT a, MAX(b) FROM t1 GROUP BY a ORDER BY a")
      }

      // create new session for tool to use
      val spark2 = SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()

      TrampolineUtil.withTempDir { dotFileDir =>
        val appArgs = new ProfileArgs(Array(
          "--output-directory",
          dotFileDir.getAbsolutePath,
          "--generate-dot",
          eventLog))
        ProfileMain.mainInternal(spark2, appArgs)

        val tempSubDir = new File(dotFileDir, ProfileMain.SUBDIR)

        // assert that a file was generated
        val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, _.startsWith("local"))
        assert(dotDirs.length === 2)

        // assert that the generated files looks something like what we expect
        var hashAggCount = 0
        var stageCount = 0
        for (file <- dotDirs) {
          assert(file.getAbsolutePath.endsWith(".dot"))
          val source = Source.fromFile(file)
          val dotFileStr = source.mkString
          source.close()
          assert(dotFileStr.startsWith("digraph G {"))
          assert(dotFileStr.endsWith("}"))
          val hashAggr = "HashAggregate"
          val stageWord = "STAGE"
          hashAggCount += dotFileStr.sliding(hashAggr.length).count(_ == hashAggr)
          stageCount += dotFileStr.sliding(stageWord.length).count(_ == stageWord)
        }

        assert(hashAggCount === 8, "Expected: 4 in node labels + 4 in graph label")
        assert(stageCount === 4, "Expected: UNKNOWN Stage, Initial Aggregation, " +
          "Final Aggregation, Sorting final output")
      }
    }
  }

  test("Empty physical plan") {
    val planLabel = SparkPlanGraph.makeDotLabel(
      appId = "local-12345-1",
      sqlId = "120",
      physicalPlan = "")

    planLabelChecks(planLabel)
  }

  test("Long physical plan") {
    val random = new SecureRandom()
    val seed = System.currentTimeMillis();
    random.setSeed(seed);
    info("Seeding test with: " + seed)
    val numTests = 100

    val lineLengthRange = 50 until 200


    val planLengthSeq = mutable.ArrayBuffer.empty[Int]
    val labelLengthSeq = mutable.ArrayBuffer.empty[Int]

    // some imperfect randomness for edge cases
    for (_ <- 1 to numTests) {
      val lineLength = lineLengthRange.start +
        random.nextInt(lineLengthRange.length) -
        SparkPlanGraph.htmlLineBreak.length()

      val sign = if (random.nextBoolean()) 1 else -1
      val planLength = 16 * 1024 + sign * lineLength * (1 + random.nextInt(5));
      val planStr = (0 to planLength / lineLength).map(_ => "a" * lineLength).mkString("\n")

      planLengthSeq += planStr.length()

      val planLabel = SparkPlanGraph.makeDotLabel(
        appId = "local-12345-1",
        sqlId = "120",
        physicalPlan = planStr)

      labelLengthSeq += planLabel.length()

      planLabelChecks(planLabel)
      assert(planLabel.length() <= 16 * 1024)
      assert(planLabel.contains("a" * lineLength))
      assert(planLabel.contains(SparkPlanGraph.htmlLineBreak))
    }

    info(s"Plan length summary: min=${labelLengthSeq.min} max=${labelLengthSeq.max}")
    info(s"Plan label summary: min=${planLengthSeq.min} max=${planLengthSeq.max}")
  }

  private def planLabelChecks(planLabel: String) {
    assert(planLabel.startsWith("<<table "))
    assert(planLabel.endsWith("</table>>"))
    assert(planLabel.contains("local-12345-1"))
    assert(planLabel.contains("Plan for SQL ID : 120"))
  }
}
