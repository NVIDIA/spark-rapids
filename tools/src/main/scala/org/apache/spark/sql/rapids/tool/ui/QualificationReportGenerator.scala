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

import java.io.{InputStream, OutputStream}

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

  val fs = FileSystem.get(outputWorkPath.toUri, new Configuration())

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
  val RAPIDS_UI_ASSETS_DIR = "/ui"
  val ASSETS_FOLDER_MAP = Map(
    "html" -> Seq("index.html", "application.html", "raw.html"),
    "css" -> Seq("rapids-dashboard.css"),
    "js" -> Seq(
      "app-report.js", "qual-report.js", "raw-report.js", "ui-data.js", "uiutils.js"),
    "assets" -> Seq(
      "searchPanes.bootstrap4.min.js",
      "buttons.bootstrap4.min.css",
      "buttons.bootstrap4.min.js",
      "buttons.html5.min.js",
      "dataTables.bootstrap4.min.css",
      "dataTables.bootstrap4.min.js",
      "dataTables.buttons.min.js",
      "dataTables.responsive.min.js",
      "dataTables.searchPanes.min.js",
      "dataTables.select.min.js",
      "jquery-3.6.0.min.js",
      "jquery.dataTables.min.js",
      "mustache-2.3.0.min.js",
      "responsive.bootstrap4.min.css",
      "responsive.bootstrap4.min.js",
      "searchPanes.bootstrap4.min.css",
      "select.bootstrap4.min.css"),
    "assets/fontawesome-free-5.15.4-web/css" -> Seq(
      "solid.min.css",
      "fontawesome.min.css",
      "brands.min.css"),
    "assets/fontawesome-free-5.15.4-web/webfonts" -> Seq(
      "fa-solid-900.woff2",
      "fa-solid-900.woff",
      "fa-solid-900.ttf",
      "fa-solid-900.svg",
      "fa-solid-900.eot",
      "fa-brands-400.woff2",
      "fa-brands-400.woff",
      "fa-brands-400.ttf",
      "fa-brands-400.svg",
      "fa-brands-400.eot"),
    "assets/bootstrap-4.6.1-dist/css" -> Seq("bootstrap.min.css"),
    "assets/bootstrap-4.6.1-dist/js" -> Seq("bootstrap.bundle.min.js"),
    "assets/spur/dist/css" -> Seq("spur.min.css"),
  )

  private val conf = new SparkConf

  def createQualReportGenerator(
      provider: Qualification): Unit = {
    val generator = new QualificationReportGenerator(conf, provider)
    generator.launch()
  }
}
