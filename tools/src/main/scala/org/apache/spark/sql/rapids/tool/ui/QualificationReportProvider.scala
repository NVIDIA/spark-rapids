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

import org.apache.spark.sql.rapids.tool.qualification.{QualApplicationInfo, QualificationSummaryInfo}

/**
 * An interface for entity that provides the qualification data consumed by the report UI generator.
 * This allows separation between the view and the data.
 */
trait QualificationReportProvider {
  /**
   * Returns configuration data of the provider.
   * The report generator can then dump those configurations to into the report
   *
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  def getConfig(): Map[String, String] = Map()

  /**
   * Returns the time the qualification last updated the application information.
   * For a static report, this is the end-time of the analysis of all the apps.
   *
   * @return 0 if this is undefined or unsupported, otherwise the last updated time in millis
   */
  def getLastUpdatedTime(): Long = {
    0
  }

  /**
   * Returns the time the history provider last updated the application history information
   *
   * @return 0 if this is undefined or unsupported, otherwise the last updated time in millis
   */
  def getStartTime(): Long = {
    0
  }

  /**
   * Returns a list of applications available for the report to show.
   * This is basically the summary of
   *
   * @return List of all known applications.
   */
  def getListing(): Iterator[QualApplicationInfo]

  /**
   * @return the [[QualificationSummaryInfo]] for the appId if it exists.
   */
  def getApplicationInfo(appId: String): Option[QualificationSummaryInfo]

  /**
   * @return all the [[QualificationSummaryInfo]] available
   */
  def getAllApplicationsInfo(): Seq[QualificationSummaryInfo]

  /**
   * Update the report when an app is selectively removed from the app listings.
   */
  def onAppDetached(): Unit = {}

  /**
   * Update the report when an app is selectively removed from the app listings.
   */
  def onAppAttached(): Unit = {}

  /**
   * The outputPath of the current instance of the provider
   */
  def getReportOutputPath: String
}
