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

class RapidsComparePage(parent: RapidsTab, conf: SparkConf, store: RapidsAppStatusStore)
  extends WebUIPage("compare") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")
    val otherAppId = parameterId.toInt
    val content =
        <div id="no-info">
          <p>Comparison Results go here......</p>
        </div>
    UIUtils.headerSparkPage(
      request, s"Details for Job $otherAppId", content, parent, showVisualization = true)
  }
}
