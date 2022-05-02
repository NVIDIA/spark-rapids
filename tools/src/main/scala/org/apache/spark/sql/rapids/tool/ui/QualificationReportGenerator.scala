/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.ui

import java.io.{File, InputStream, OutputStream}
import java.text.SimpleDateFormat
import java.util.Date

import com.nvidia.spark.rapids.tool.qualification.Qualification
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class QualificationReportGenerator(
    conf: SparkConf,
    provider: Qualification) extends Logging {

  import QualificationReportGenerator._

  val outputWorkPath = new Path(provider.getReportOutputPath, "ui-report")

  private val fs = FileSystem.get(outputWorkPath.toUri, new Configuration())

  val outputWorkDir = Some(fs.mkdirs(outputWorkPath))
  val destinationFolder = {
    val timestamp = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
    new File(s"${outputWorkDir}-${timestamp}")
  }

  def copyAssetFiles() : Unit = {
    if (fs.mkdirs(outputWorkPath)) {
      for ((folder, assets) <- ASSETS_FOLDER_MAP) {
        val destinationAssetPath = new Path(outputWorkPath, folder)
        if(fs.mkdirs(destinationAssetPath)) {
          val relativePath = s"${RAPIDS_UI_ASSETS_DIR}/${folder}"
          assets.foreach { srcFile =>
            var inputStream: InputStream = null;
            var outputStream: OutputStream = null;
            try {
              outputStream = fs.create(new Path(destinationAssetPath, srcFile))
              inputStream = getClass().getResourceAsStream(s"${relativePath}/${srcFile}")
              val buffer = new Array[Byte](130 * 1024)
              Iterator.continually(inputStream.read(buffer)).takeWhile(_ != -1).foreach { bCount =>
                outputStream.write(buffer, 0, bCount)
                outputStream.flush()
              }
            } finally {
              if (inputStream != null) {
                inputStream.close()
              }
              if (outputStream != null) {
                outputStream.close()
              }
            }
          }
        }
      }
    }
  }

  def launch(): Unit = {
    copyAssetFiles
    generateJSFiles
  }

  def generateJSFiles(): Unit = {
    implicit val formats = DefaultFormats
    val appInfoRecs = Serialization.write(provider.getListing())
    val infoSummary = Serialization.write(provider.getAllApplicationsInfo())
    val dataSourceInfo = Serialization.write(provider.getDataSourceInfo())
    val qualInfoSummaryContent =
      s"""
         |let appInfoRecords =
         |\t${appInfoRecs};
         |let qualificationRecords =
         |\t${infoSummary};
         |let dataSourceInfoRecords =
         |\t${dataSourceInfo};
       """.stripMargin
    val outputFile = Some(fs.create(new Path(outputWorkPath, s"js/mock-data.js")))
    try {
      outputFile.foreach { dataFile =>
        dataFile.writeBytes(qualInfoSummaryContent)
      }
    } finally {
      outputFile.foreach { dataFile =>
        dataFile.flush()
        dataFile.close()
      }
    }
  }
}

object QualificationReportGenerator extends Logging {
  val UI_HOME = getClass.getResource("/ui")
  val RAPIDS_UI_ASSETS_DIR = "/ui/assets"
  val ASSETS_FOLDER_MAP = Map(
    "html" -> Seq("index.html", "application.html", "raw.html"),
    "css" -> Seq("spur.css"),
    "js" -> Seq("app-report.js", "qual-report.js", "raw-report.js", "spur.js", "ui-data.js",
      "uiutils.js"))

  private val conf = new SparkConf

  def createQualReportGenerator(
      provider: Qualification): Unit = {
    val generator = new QualificationReportGenerator(conf, provider)
    generator.launch()
  }
}
