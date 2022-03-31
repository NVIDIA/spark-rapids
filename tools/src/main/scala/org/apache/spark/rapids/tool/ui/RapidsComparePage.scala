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

import java.io.{IOException, InputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
//import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

//import org.json4s.DefaultFormats
//import org.json4s.jackson.JsonMethods

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.rapids.tool.SHSUtils
import org.apache.spark.rapids.tool.status.RapidsAppStatusStore
import org.apache.spark.ui.{UIUtils, WebUIPage}

class RapidsComparePage(parent: RapidsTab, conf: SparkConf, store: RapidsAppStatusStore)
  extends WebUIPage("compare") {

  def getAPIPath(request: HttpServletRequest) : Option[String] = {
    val path = UIUtils.uiRoot(request)
    if (path.isEmpty) {
      val alternatePath = request.getRequestURL
      var ind = alternatePath.indexOf("history")
      if (ind > 0) {
        return Some(request.getRequestURL.substring(0, ind - 1))
      }
    }
    None
  }

  def getURLForAppInfo(request: HttpServletRequest, otherAppID: String) : URL = {
    val baseURL = getAPIPath(request).getOrElse("http://localhost:18080")
    new URL(s"$baseURL/api/v1/applications/$otherAppID")
  }

  def getContentAndCode(url: URL): (Int, Option[String], Option[String]) = {
    val (code, in, errString) = connectAndGetInputStream(url)
    val inString = in.map(IOUtils.toString(_, StandardCharsets.UTF_8))
    (code, inString, errString)
  }

  def connectAndGetInputStream(url: URL): (Int, Option[InputStream], Option[String]) = {

    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()
    val code = connection.getResponseCode()
    val inStream = try {
      Option(connection.getInputStream())
    } catch {
      case io: IOException => None
    }
    val errString = try {
      val err = Option(connection.getErrorStream())
      err.map(IOUtils.toString(_, StandardCharsets.UTF_8))
    } catch {
      case io: IOException => None
    }
    (code, inStream, errString)
  }

  def getOtherAppName(otherAppID: String): String = {
    val otherAppInfo = SHSUtils.getApplicationInfo(store.appSStore.store, otherAppID, conf)
//    if (otherAppInfo.isDefined) {
//      return otherAppInfo.get.name
//    }
//    "NOT_DEFINED"
    otherAppInfo.name
  }

  def tryToRunREST(appIDURL: String): String = {
    val url = new URL("http://localhost:18080/api/v1/applications/local-1647443474448")
    val (code, resultOpt, error) = getContentAndCode(url)
    resultOpt.get
//    val jsonResult = resultOpt.get
//    JsonMethods.parse(jsonResult).extractOpt[ApplicationInfo]
  }

  def getAppInfoByRest(request: HttpServletRequest, appID: String): String = {
    val (code, resultOpt, error) = getContentAndCode(getURLForAppInfo(request, appID))
    resultOpt.get
    //JsonMethods.parse(jsonParsed).extract[ApplicationInfo]
  }

  def generateURL(request: HttpServletRequest, otherAppID: String): String = {

    UIUtils.prependBaseUri(request,
      s"/api/v1/applications/$otherAppID/")
  }

  def detApInfoURL(request: HttpServletRequest, otherAppID: String): String = {
    UIUtils.prependBaseUri(request,
      s"/api/v1/applications/$otherAppID/")
  }

//  def createURLForResource(): Unit = {
//    APIResource. ApiRootResource
//  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val otherAppId = request.getParameter("id")
    require(otherAppId != null && otherAppId.nonEmpty, "Missing id parameter")

    val currAppID = store.appSStore.applicationInfo().id
    //val otherAppName = getOtherAppName(currAppID)

    val dataInfoJson = getAppInfoByRest(request, otherAppId)

    val content =
        <div id="no-info">
          <p>Comparison Results between {currAppID} and {otherAppId} will be shown here......</p>
          <p>Other APPNAME is {detApInfoURL(request, otherAppId)}......</p>
          <p>root fo the resquest is  {UIUtils.uiRoot(request)}</p>
          <p>RequestURL is {request.getRequestURL}</p>
          <p>getContextPath is {request.getContextPath}</p>
          <p>servlet path = {request.getServletPath}</p>
          <p>servlet pathInfo = {request.getPathInfo}</p>
          <p>dataInfoJSON application name is = {dataInfoJson}</p>
          <a href="{generateURL(request, otherAppId)}" class="name-link">Request</a>
        </div>
    UIUtils.headerSparkPage(
      request, s"Rapids Comparison $otherAppId", content, parent)
  }
}
