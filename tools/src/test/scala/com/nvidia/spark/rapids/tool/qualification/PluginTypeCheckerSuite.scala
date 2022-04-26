/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.qualification

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.FunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.TrampolineUtil

class PluginTypeCheckerSuite extends FunSuite with Logging {

  test("read not supported datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:boolean,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,BOOLEAN\n"
      val supText = (header + "parquet,read,NS\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 0.0)
      assert(nsTypes.contains("boolean"))
    }
  }

  test("invalid file") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:boolean,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,BOOLEAN\n"
      // text longer then header should throw
      val supText = (header + "parquet,read,NS,NS\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      assertThrows[IllegalStateException] {
        checker.setPluginDataSourceFile(csvSupportedFile.toString)
      }
    }
  }

  test("read not CO datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,int\n"
      val supText = (header + "parquet,read,CO\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 0.0)
      assert(nsTypes.contains("int"))
    }
  }

  test("unknown file format") {
    val checker = new PluginTypeChecker
    val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
    val (score, nsTypes) = checker.scoreReadDataTypes("invalidFormat", testSchema)
    assert(score == 0.0)
    assert(nsTypes.contains("*"))
  }

  test("unknown datatype ok") {
    val checker = new PluginTypeChecker
    // right now we only look for unsupported types so an unknown one
    // comes back 1.0
    val testSchema = "loan_id:invalidDT"
    val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
    assert(score == 1.0)
    assert(nsTypes.isEmpty)
  }

  test("supported type") {
    // expect string and bigint parquet to be always supported
    val checker = new PluginTypeChecker
    val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
    val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
    assert(score == 1.0)
    assert(nsTypes.isEmpty)
  }

  test("supported operator score") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val header = "CPUOperator,Score\n"
      val supText = (header + "FilterExec,3\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testScore.txt")
      Files.write(csvSupportedFile, supText)
      checker.setOperatorScore(csvSupportedFile.toString)
      val operScore = checker.getOperatorScore
      assert(operScore.contains("FilterExec"))
      assert(!operScore.contains("ProjectExec"))
      assert(operScore("FilterExec") == 3)
    }
  }

  test("supported operator score from default file") {
    val checker = new PluginTypeChecker
    val result = checker.getOperatorScore
    assert(result("FilterExec") == 2)
    assert(result("Ceil") == 3)
  }

  test("supported Execs") {
    val checker = new PluginTypeChecker
    val result = checker.getSupportedExecs
    assert(result.contains("ShuffledHashJoinExec"))
    assert(result("ShuffledHashJoinExec") == "S")
    assert(result("CollectLimitExec") == "NS")
  }

  test("supported Expressions") {
    val checker = new PluginTypeChecker
    val result = checker.getSupportedExprs
    assert(result.contains("Add"))
    assert(result("Add") == "S")
    assert(result.contains("IsNull"))
  }
}
