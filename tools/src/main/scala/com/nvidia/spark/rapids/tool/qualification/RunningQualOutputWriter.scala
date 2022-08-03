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

package com.nvidia.spark.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.TEXT_DELIMITER

import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo

/**
 * This class handles writing output to files for a running qualification app.
 *
 * @param appId The id of the application
 * @param appName The name of the application
 * @param outputDir The directory to output the files to
 */
class RunningQualOutputWriter(
    appId: String,
    appName: String,
    outputDir: String)
  extends QualOutputWriter(outputDir, reportReadSchema=false, printStdout=false,
    prettyPrintOrder = "desc") {
  // TODO - do we really need to extend the outputwriter

  // Since this is running app keeps these open until finished with application.
  private lazy val csvPerSQLFileWriter = new ToolTextFileWriter(outputDir,
    s"${QualOutputWriter.LOGFILE_NAME}_persql.csv", "Per SQL CSV Report")
  private lazy val textPerSQLFileWriter = new ToolTextFileWriter(outputDir,
    s"${QualOutputWriter.LOGFILE_NAME}_persql.log", "Per SQL Summary Report")

  // we don't know max length since process per query, hardcode for 100 for now
  private val SQL_DESC_LENGTH = 100
  private val appNameSize = if (appName.nonEmpty) appName.size else 100
  // we don't know the max sql query name size so lets cap it at 100
  val headersAndSizes = QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize, appId.size, SQL_DESC_LENGTH)
  val entireTextHeader = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
    TEXT_DELIMITER, true)
  private val sep = "=" * (entireTextHeader.size - 1)

  def init(): Unit = {
    csvPerSQLFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes,
      QualOutputWriter.CSV_DELIMITER, false))
    textPerSQLFileWriter.write(s"$sep\n")
    textPerSQLFileWriter.write(entireTextHeader)
    textPerSQLFileWriter.write(s"$sep\n")
    csvPerSQLFileWriter.flush()
    textPerSQLFileWriter.flush()
  }

  def close(): Unit = {
    csvPerSQLFileWriter.close()
    textPerSQLFileWriter.write(s"$sep\n")
    textPerSQLFileWriter.close()
  }

  def writePerSqlCSVReport(sqlInfo: String): Unit = {
    csvPerSQLFileWriter.write(sqlInfo)
    csvPerSQLFileWriter.flush()
  }

  def writePerSqlTextReport(sqlInfo: String): Unit = {
    textPerSQLFileWriter.write(sqlInfo)
    textPerSQLFileWriter.flush()
  }

  def writeApplicationReport(appSummary: QualificationSummaryInfo): Unit = {
    // only writing a single application so num output rows should be 1
    writeTextReport(textPerSQLFileWriter, Seq(appSummary), Seq.empty, numOutputRows=1)
    writeDetailedCSVReport(csvPerSQLFileWriter, Seq(appSummary))
  }
}