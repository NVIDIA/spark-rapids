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

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.nvidia.spark.rapids.tool.qualification.Qualification
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils.getPropertiesFromFile

class QualificationReportGenerator(
    conf: SparkConf,
    provider: Qualification) {

  import QualificationReportGenerator._

  // TODO: use HDFS API to create the logoutput if necessary
  val assetsFolder = new File(getAssetsPath)
  val outputWorkDir = new File(provider.getReportOutputPath, "ui-report")
  val destinationFolder = {
    val timestamp = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
    new File(s"${outputWorkDir}-${timestamp}")
  }

  def getAssetsPath(): String = s"${UI_HOME}${File.separator}${RAPIDS_UI_ASSETS_DIR}"

  def copyAssetFolder(src: File, dest: File): Unit = {
    if (src.isDirectory) {
      if (!dest.exists()) {
        dest.mkdirs()
      }
      src.listFiles().foreach { srcDir =>
        copyAssetFolder(srcDir, new File(dest, srcDir.getName))
      }
    } else {
      new FileOutputStream(dest) getChannel() transferFrom(
        new FileInputStream(src) getChannel(), 0, Long.MaxValue)
    }
  }

  def launch(): Unit = {
    copyAssetFolder(assetsFolder, destinationFolder)
    generateJSFiles
  }

  def generateJSFiles(): Unit = {
    implicit val formats = DefaultFormats
    val appInfoRecs = Serialization.write(provider.getListing())
    val infoSummary = Serialization.write(provider.getAllApplicationsInfo())
    val dataSourceInfo = Serialization.write(provider.getDataSourceInfo())
    val qualInfoSummaryContent =
      s"""
         |
         |let appInfoRecords =
         |\t${appInfoRecs};
         |let qualificationRecords =
         |\t${infoSummary};
         |let dataSourceInfoRecords =
         |\t${dataSourceInfo};
       """.stripMargin
    val outputFile = new File(destinationFolder, s"js/mock-data.js")
    val pw = new PrintWriter(outputFile)
    pw.write(qualInfoSummaryContent)
    pw.close
  }
}

object QualificationReportGenerator extends Logging {
  val RAPIDS_UI_PREFIX = "spark.rapids.tool.qualification.ui."
  val UI_HOME = getClass.getResource("/ui").getPath
  val RAPIDS_UI_CONF_DIR = "conf"
  val RAPIDS_UI_ASSETS_DIR = "assets"
  val RAPIDS_UI_CONF_FILE = "qual-ui-report-defaults.conf"

  private val conf = new SparkConf

  def createQualReportGenerator(
      provider: Qualification,
      propFilePath: String = null): Unit = {
    loadDefaultSparkProperties(conf, propFilePath)
    val generator = new QualificationReportGenerator(conf, provider)
    generator.launch()
  }

  def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {
    val propPath = Option(filePath).getOrElse(getDefaultUIProperties())
    getPropertiesFromFile(propPath).filter { case (k, v) =>
      k.startsWith(RAPIDS_UI_PREFIX)
    }.foreach { case (k, v) =>
      conf.setIfMissing(k, v)
      sys.props.getOrElseUpdate(k, v)
    }
    propPath
  }

  def getDefaultUIProperties(): String = {
    s"${UI_HOME}${File.separator}${RAPIDS_UI_CONF_DIR}${File.separator}${RAPIDS_UI_CONF_FILE}"
  }
}
