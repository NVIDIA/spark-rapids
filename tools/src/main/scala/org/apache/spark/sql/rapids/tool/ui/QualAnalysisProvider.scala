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

class QualAnalysisProvider(outputDir: String) extends QualificationReportProvider {
  /**
   * Returns a list of applications available for the report to show.
   * This is basically the summary of
   *
   * @return List of all known applications.
   */
  override def getListing(): Iterator[QualApplicationInfo] = ???

  /**
   * @return the [[QualificationSummaryInfo]] for the appId if it exists.
   */
  override def getApplicationInfo(appId: String): Option[QualificationSummaryInfo] = ???

  /**
   * @return all the [[QualificationSummaryInfo]] available
   */
  override def getAllApplicationsInfo(): Seq[QualificationSummaryInfo] = ???

  /**
   * The outputPath of the current instance of the provider
   */
  override def getReportOutputPath: String = ???
}
