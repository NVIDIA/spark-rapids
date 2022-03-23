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

package org.apache.spark.rapids.tool.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.SparkConf
import org.apache.spark.rapids.tool.status.RapidsAppStatusStore
import org.apache.spark.ui.{UIUtils, WebUIPage}

class RapidsEnvPage (parent: RapidsEnvTab,
                      conf: SparkConf,
                     rapidsStore: RapidsAppStatusStore) extends WebUIPage("") {
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyHeader = Seq("Name", "Value")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def classPathHeader = Seq("Resource", "Source")
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>

  override def render(request: HttpServletRequest): Seq[Node] = {
    val appEnv = rapidsStore.appSStore.environmentInfo()
    val jvmInformation = Map(
      "Java Version" -> appEnv.runtime.javaVersion,
      "Java Home" -> appEnv.runtime.javaHome,
      "Scala Version" -> appEnv.runtime.scalaVersion)
    val runtimeInformationTable = UIUtils.listingTable(
      propertyHeader, jvmRow, jvmInformation.toSeq.sorted, fixedWidth = true,
      headerClasses = headerClasses)

    val classpathEntriesTable = UIUtils.listingTable(
      classPathHeader, classPathRow, appEnv.classpathEntries.sorted, fixedWidth = true,
      headerClasses = headerClasses)

    val rapidsClasspathEntriesTable = UIUtils.listingTable(
      classPathHeader, classPathRow, rapidsStore.getRapidJARInfo.sorted, fixedWidth = true,
      headerClasses = headerClasses)

    val content =
      <span>
        <span class="collapse-aggregated-runtimeInformation collapse-table"
              onClick="collapseTable('collapse-aggregated-runtimeInformation',
            'aggregated-runtimeInformation')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Runtime Information</a>
          </h4>
        </span>
        <div class="aggregated-runtimeInformation collapsible-table">
          {runtimeInformationTable}
        </div>

        <span class="collapse-aggregated-classpathEntries collapse-table"
              onClick="collapseTable('collapse-aggregated-classpathEntries',
            'aggregated-classpathEntries')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Rapids ClassPath Entries</a>
          </h4>
        </span>
        <div class="aggregated-classpathEntries collapsible-table collapsed">
          {rapidsClasspathEntriesTable}
        </div>

        <span class="collapse-aggregated-classpathEntries collapse-table"
              onClick="collapseTable('collapse-aggregated-classpathEntries',
            'aggregated-classpathEntries')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Classpath Entries</a>
          </h4>
        </span>
        <div class="aggregated-classpathEntries collapsible-table collapsed">
          {classpathEntriesTable}
        </div>
      </span>

    UIUtils.headerSparkPage(request, "RapidsEnv", content, parent)
  }
}
