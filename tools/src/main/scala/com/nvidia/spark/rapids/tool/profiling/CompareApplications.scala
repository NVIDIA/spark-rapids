/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.rapids.tool.profiling.{ApplicationInfo, SparkPlanInfoWithStage}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * CompareApplications compares multiple ApplicationInfo objects
 */
class CompareApplications(apps: Seq[ApplicationInfo],
    fileWriter: Option[ToolTextFileWriter]) extends Logging {

  require(apps.size>1)

  def findMatchingStages(): Unit = {
    val normalizedByAppId = apps.map { app =>
      val normalized = app.sqlPlan.mapValues { plan =>
        SparkPlanInfoWithStage(plan, app.accumIdToStageId).normalizeForStageComparison
      }
      (app.appId, normalized)
    }.toMap

    val appIdToSortedSqlIds = mutable.Map[String, mutable.Buffer[Long]]()
    appIdToSortedSqlIds ++= normalizedByAppId.mapValues { sqlIdToPlan =>
      sqlIdToPlan.keys.toList.sorted.toBuffer
    }

    // Each line holds a map with app id/sql id that match each other
    val matchingSqlIds = new ArrayBuffer[mutable.HashMap[String, Long]]()
    val matchingStageIds = new ArrayBuffer[mutable.HashMap[String, Int]]()

    while (appIdToSortedSqlIds.nonEmpty) {
      val appIds = appIdToSortedSqlIds.keys.toSeq.sorted
      val sourceAppId = appIds.head
      val sourceSqlId = appIdToSortedSqlIds(sourceAppId).head
      val sourcePlan = normalizedByAppId(sourceAppId)(sourceSqlId)

      val sqlMatches = mutable.HashMap[String, Long]()
      sqlMatches(sourceAppId) = sourceSqlId
      // The key is the stage for the source app id. The values are pairs of appid/stage
      // for the matching stages in other apps
      val stageMatches = new mutable.HashMap[Int, mutable.Buffer[(String, Int)]]()
      sourcePlan.depthFirstStages.distinct.flatten.foreach { stage =>
        stageMatches(stage) = new mutable.ArrayBuffer[(String, Int)]()
      }

      // Now we want to find the first plan in each app that matches. The sorting is
      // because we assume that the SQL commands are run in the same order, so it should
      // make it simpler to find them.
      appIds.slice(1, appIds.length).foreach { probeAppId =>
        var matchForProbedApp: Option[Long] = None
        appIdToSortedSqlIds(probeAppId).foreach { probeSqlId =>
          if (matchForProbedApp.isEmpty) {
            val probePlan = normalizedByAppId(probeAppId)(probeSqlId)
            if (probePlan.equals(sourcePlan)) {
              sourcePlan.depthFirstStages.zip(probePlan.depthFirstStages).filter {
                case (a, b) => a.isDefined && b.isDefined
              }.distinct.foreach {
                case (sourceStageId, probeStageId) =>
                  stageMatches(sourceStageId.get).append((probeAppId, probeStageId.get))
              }
              matchForProbedApp = Some(probeSqlId)
            }
          }
        }

        matchForProbedApp.foreach { foundId =>
          sqlMatches(probeAppId) = foundId
        }
      }

      stageMatches.toSeq.sortWith {
        case (a, b) => a._1 < b._1
      }.foreach {
        case (sourceStage, others) =>
          val ret = mutable.HashMap[String, Int]()
          ret(sourceAppId) = sourceStage
          others.foreach {
            case (appId, stageId) => ret(appId) = stageId
          }
          matchingStageIds.append(ret)
      }

      // Remove the matches from the data structures
      sqlMatches.foreach {
        case (appId, sqlId) =>
          appIdToSortedSqlIds(appId) -= sqlId
          if (appIdToSortedSqlIds(appId).isEmpty) {
            appIdToSortedSqlIds.remove(appId)
          }
      }

      matchingSqlIds += sqlMatches
    }

    val outputAppIds = normalizedByAppId.keys.toSeq.sorted

    val matchingSqlData = matchingSqlIds.map { info =>
      Row(outputAppIds.map { appId =>
        info.get(appId).map(_.toString).getOrElse("")
      }: _*)
    }.toList.asJava

    val matchingType = StructType(outputAppIds.map(id => StructField(id, StringType)))

    apps.head.writeAsDF(matchingSqlData,
      matchingType,
      "\n\nMatching SQL IDs Across Applications:\n",
      fileWriter)

    val matchingStageData = matchingStageIds.map { info =>
      Row(outputAppIds.map { appId =>
        info.get(appId).map(_.toString).getOrElse("")
      }: _*)
    }.toList.asJava

    apps.head.writeAsDF(matchingStageData,
      matchingType,
      "\n\nMatching Stage IDs Across Applications:\n",
      fileWriter)
  }

  // Compare the App Information.
  def compareAppInfo(): Unit = {
    val messageHeader = "\n\nCompare Application Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"appDF_${app.index}")) {
        query += app.generateAppInfo
        if (i < apps.size) {
          query += "\n union \n"
        } else {
          query += " order by appIndex"
        }
      } else {
        fileWriter.foreach(_.write("No Application Information Found!\n"))
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }

  // Compare Job information
  def compareJobInfo(): Unit = {
    val messageHeader = "\n\nCompare Job Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"jobDF_${app.index}")) {
        query += app.jobtoStagesSQL
        if (i < apps.size) {
          query += "\n union \n"
        } else {
          query += " order by appIndex"
        }
      } else {
        fileWriter.foreach(_.write("No Job Information Found!\n"))
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }

  // Compare Executors information
  def compareExecutorInfo(): Unit = {
    val messageHeader = "\n\nCompare Executor Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"executorsDF_${app.index}")) {
        query += app.generateExecutorInfo
        if (i < apps.size) {
          query += "\n union \n"
        } else {
          query += " order by appIndex"
        }
      } else {
        fileWriter.foreach(_.write("No Executor Information Found!\n"))
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }

  // Compare Rapids Properties which are set explicitly
  def compareRapidsProperties(): Unit = {
    val messageHeader = "\n\nCompare Rapids Properties which are set explicitly:\n"
    var withClauseAllKeys = "with allKeys as \n ("
    val selectKeyPart = "select allKeys.propertyName"
    var selectValuePart = ""
    var query = " allKeys LEFT OUTER JOIN \n"
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"propertiesDF_${app.index}")) {
        if (i < apps.size) {
          withClauseAllKeys += "select distinct propertyName from (" +
              app.generateRapidsProperties + ") union "
          query += "(" + app.generateRapidsProperties + s") tmp_$i"
          query += s" on allKeys.propertyName=tmp_$i.propertyName"
          query += "\n LEFT OUTER JOIN \n"
        } else { // For the last app
          withClauseAllKeys += "select distinct propertyName from (" +
              app.generateRapidsProperties + "))\n"
          query += "(" + app.generateRapidsProperties + s") tmp_$i"
          query += s" on allKeys.propertyName=tmp_$i.propertyName"
        }
        selectValuePart += s",appIndex_${app.index}"
      } else {
        fileWriter.foreach(_.write("No Spark Rapids parameters Found!\n"))
      }
      i += 1
    }

    query = withClauseAllKeys + selectKeyPart + selectValuePart +
        " from (\n" + query + "\n) order by propertyName"
    logDebug("Running query " + query)
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }
}
