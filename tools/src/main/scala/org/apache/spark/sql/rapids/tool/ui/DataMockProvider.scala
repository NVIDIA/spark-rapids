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

import java.io.File

import scala.annotation.tailrec

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.{QualApplicationInfo, QualificationSummaryInfo}

/**
 * A class that implements the Provider interface by loading static json files.
 * This class is a transient phase until the features are supported in the qualification tool.
 */
class DataMockProvider extends QualificationReportProvider {
  import DataMockProvider._
  // this is the main output of the qualification report
  var qualApplicationInfo: Seq[QualApplicationInfo] =
    DataMockProvider.loadDataFromJson(classOf[QualApplicationInfo])
  var qualSummaryInfo: Seq[QualificationSummaryInfo] =
    DataMockProvider.loadDataFromJson(classOf[QualificationSummaryInfo]);

  /**
   * Returns a list of applications available for the report to show.
   * This is basically the summary of
   *
   * @return List of all known applications.
   */
  override def getListing(): Iterator[QualApplicationInfo] = qualApplicationInfo.iterator;

  /**
   * @return the [[QualificationSummaryInfo]] for the appId if it exists.
   */
  override def getApplicationInfo(appID: String): Option[QualificationSummaryInfo] = {
    qualSummaryInfo.find(_.appId == appID)
  }

  /**
   * The outputPath of the current instance of the provider
   */
  override def getReportOutputPath: String = {
    return outputDir
  }

  /**
   * @return all the [[QualificationSummaryInfo]] available
   */
  override def getAllApplicationsInfo(): Seq[QualificationSummaryInfo] = qualSummaryInfo
}

object DataMockProvider extends Logging {
  private var inputDir: String = null
  private var outputDir: String = null

  def convertFileContentToObjs[T](rawJson: String)(
      implicit fmt: Formats = DefaultFormats, mf: Manifest[T]): Option[List[T]] = {
    val loadedJson = JsonMethods.parse(rawJson)
    loadedJson.extractOpt[List[T]]
  }

  def loadDataFromJson[T](klass: Class[T])(implicit m: Manifest[T]) : Seq[T] = {
    val inputFile = new File(s"${inputDir}${File.separator}get${klass.getSimpleName}.json")
    val jsonContent = scala.io.Source.fromFile(inputFile).mkString
    val resultOpt = convertFileContentToObjs[T](jsonContent);
    logInfo(s"result of loading ${klass}: ${resultOpt.toString()}")
    resultOpt.getOrElse(Seq())
  }

  @tailrec
  private def parse(args: List[String]): Unit = {
    args match {
      case ("--inputDir" | "-i") :: value :: tail =>
        inputDir = value
        parse(tail)
      case ("--outputDir" | "-o") :: value :: tail =>
        outputDir = value
        parse(tail)
      case Nil =>
      case _ =>
        printUsageAndExit(1)
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off println
    System.err.println(
      """
        |Usage: DataMockProvider [options]
        |
        |Options:
        |  --inputDir DIR               Path to a directory that has JSON mock data
        |                               Default is conf/spark-defaults.conf.
        |
        |""".stripMargin)
    // scalastyle:on println
    System.exit(exitCode)
  }
  def main(args: Array[String]) = {
    parse(args.toList)
    var testQualAppInfo: Seq[QualApplicationInfo] =
      DataMockProvider.loadDataFromJson(classOf[QualApplicationInfo])
    val provider = new DataMockProvider
    QualificationReportGenerator.createQualReportGenerator(provider)
  }
}
