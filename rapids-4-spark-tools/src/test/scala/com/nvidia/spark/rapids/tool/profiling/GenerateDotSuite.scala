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

        // assert that a file was generated
        val dotDirs = ToolTestUtils.listFilesMatching(dotFileDir, _.startsWith("local"))
        assert(dotDirs.length === 2)

        // assert that the generated files looks something like what we expect
        var hashAggCount = 0
        for (dir <- dotDirs) {
          val dotFiles = ToolTestUtils.listFilesMatching(dir, _.endsWith(".dot"))
          assert(dotFiles.length === 1)
          val source = Source.fromFile(dotFiles.head)
          try {
            val lines = source.getLines().toArray
            assert(lines.head === "digraph G {")
            assert(lines.last === "}")
            hashAggCount += lines.count(_.contains("HashAggregate"))
          } finally {
            source.close()
          }
        }
        assert(hashAggCount === 2)
      }
    }
  }
}
