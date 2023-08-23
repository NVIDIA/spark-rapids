/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.scaletest

import java.io.{BufferedWriter, File, FileWriter}

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

/**
 * A Class for the report of Scale Test.
 * Only execution time are included at the beginning, will add more metadata for the test.
 * TODO: task failures, memory peak info, gpu usage etc.
 */
class TestReport(config: Config, executionElapseMap: Map[String, Seq[Long]]) {
  def save(): Unit = {
    if (config.overwrite != true) {
      val file = new File(config.reportPath)
      if (file.exists()) {
        throw new IllegalStateException(s"File $config.reportPath already exists. Please use " +
          s"--overwrite argument to force overwrite.")
      }
    }
    val writer = new BufferedWriter(new FileWriter(config.reportPath))
    writer.write("query,iteration_elapses/millis,average_elapse/millis")
    writer.newLine()
    executionElapseMap.foreach { case (key, value) =>
      writer.write(s"$key,[${value.mkString(",")}], ${value.sum/(value.length)}")
      writer.newLine()
    }
    writer.close()
    println(s"CSV report file saved at: ${config.reportPath}")
  }
}
