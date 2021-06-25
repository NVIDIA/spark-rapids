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

import scala.io.Source

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}

class GenerateTimelineSuite extends FunSuite with BeforeAndAfterAll with Logging {

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  test("Generate Timeline") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val eventLog = ToolTestUtils.generateEventLog(eventLogDir, "timeline") { spark =>
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
          "--generate-timeline",
          eventLog))
        ProfileMain.mainInternal(spark2, appArgs)

        val tempSubDir = new File(dotFileDir, ProfileMain.SUBDIR)

        // assert that a file was generated
        val outputDirs = ToolTestUtils.listFilesMatching(tempSubDir, _.startsWith("local"))
        assert(outputDirs.length === 1)

        // assert that the generated files looks something like what we expect
        var stageZeroCount = 0
        var stageRangeZeroCount = 0
        var jobZeroCount = 0
        // We cannot really simply test the SQL count because that counter does not get reset
        // with each test
        for (file <- outputDirs) {
          assert(file.getAbsolutePath.endsWith(".svg"))
          val source = Source.fromFile(file)
          try {
            val lines = source.getLines().toArray
            stageZeroCount += lines.count(_.contains("STAGE 0"))
            stageRangeZeroCount += lines.count(_.contains("STAGE RANGE 0"))
            jobZeroCount += lines.count(_.contains("JOB 0"))
          } finally {
            source.close()
          }
        }
        //
        assert(stageZeroCount === 1)
        assert(stageRangeZeroCount === 1)
        assert(jobZeroCount === 1)
      }
    }
  }
}
