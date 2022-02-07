/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.{ApplicationInfo, SparkPlanInfoWithStage}

/**
 * CompareApplications compares multiple ApplicationInfo objects
 */
class CompareApplications(apps: Seq[ApplicationInfo]) extends Logging {

  require(apps.size > 1)

  def findMatchingStages(): (Seq[CompareProfileResults], Seq[CompareProfileResults]) = {
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
      val sourceSqlIdArr = appIdToSortedSqlIds(sourceAppId)
      if (sourceSqlIdArr.isEmpty) {
        appIdToSortedSqlIds.remove(sourceAppId)
      } else {
        val sourceSqlId = sourceSqlIdArr.head

        val sourcePlan = normalizedByAppId(sourceAppId)(sourceSqlId)

        val sqlMatches = mutable.HashMap[String, Long]()
        sqlMatches(sourceAppId) = sourceSqlId
        // The key is the stage for the source app id. The values are
        // pairs of
        // appid/stage for the matching stages in other apps
        val stageMatches =
        new mutable.HashMap[Int, mutable.Buffer[(String, Int)]]()
        sourcePlan.depthFirstStages.distinct.flatten.foreach { stage =>
          stageMatches(stage) = new mutable.ArrayBuffer[(String, Int)]()
        }

        // Now we want to find the first plan in each app that matches.
        // The sorting is
        // because we assume that the SQL commands are run in the same
        // order, so it should
        // make it simpler to find them.
        appIds.slice(1, appIds.length).foreach { probeAppId =>
          var matchForProbedApp: Option[Long] = None
          appIdToSortedSqlIds(probeAppId).foreach { probeSqlId =>
            if (matchForProbedApp.isEmpty) {
              val probePlan = normalizedByAppId(probeAppId)(probeSqlId)
              if (probePlan.equals(sourcePlan)) {
                sourcePlan.depthFirstStages.zip(probePlan.depthFirstStages)
                  .filter {
                    case (a, b) => a.isDefined && b.isDefined
                  }.distinct.foreach {
                  case (sourceStageId, probeStageId) =>
                    stageMatches(sourceStageId.get).append((probeAppId,
                      probeStageId.get))
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
    }

    val outputAppIds = normalizedByAppId.keys.toSeq.sorted

    val matchingSqlData = matchingSqlIds.map { info =>
      outputAppIds.map { appId =>
        info.get(appId).map(_.toString).getOrElse("")
      }
    }

    val matchingSqlIdsRet = if (matchingSqlData.size > 0) {
      matchingSqlData.map(CompareProfileResults(outputAppIds, _))
    } else {
      Seq.empty
    }

    val matchingStageData = matchingStageIds.map { info =>
      outputAppIds.map { appId =>
        info.get(appId).map(_.toString).getOrElse("")
      }
    }

    val matchingStageIdsRet = if (matchingStageData.size > 0) {
      matchingStageData.map(CompareProfileResults(outputAppIds, _))
    } else {
      Seq.empty
    }
    (matchingSqlIdsRet, matchingStageIdsRet)
  }
}
