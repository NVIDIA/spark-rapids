package com.nvidia.spark.rapids.tests.scaletest

import java.io.{BufferedWriter, File, FileWriter}

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

/**
 * A Class for the report of Scale Test.
 * Only execution time are included at the beginning, will add more metadata for the test.
 * TODO: task failures, memory peak info, gpu usage etc.
 */
class TestReport(config: Config, executionElapseMap: Map[String, Long]) {
  def save(): Unit = {
    if (config.overwrite != true) {
      val file = new File(config.reportPath)
      if (file.exists()) {
        throw new IllegalStateException(s"File $config.reportPath already exists. Please use " +
          s"--overwrite argument to force overwrite.")
      }
    }
    val writer = new BufferedWriter(new FileWriter(config.reportPath))
    writer.write("query,elapse")
    writer.newLine()
    executionElapseMap.foreach { case (key, value) =>
      writer.write(s"$key,$value")
      writer.newLine()
    }
    writer.close()
    println(s"CSV report file saved at: ${config.reportPath}")
  }
}
