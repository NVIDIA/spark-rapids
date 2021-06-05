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

package org.apache.spark.sql.rapids.tool.profiling

import com.nvidia.spark.rapids.tool.profiling.ProfileUtils
import org.apache.hadoop.fs.Path
import org.rogach.scallop.ScallopOption
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Map}

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.sql.DataFrame

object ToolUtils extends Logging {

  def isGPUMode(properties: collection.mutable.Map[String, String]): Boolean = {
    (properties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin")
        && properties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean)
  }

  def showString(df: DataFrame, numRows: Int) = {
    df.showString(numRows, 0)
  }

  /**
   * Function to evaluate the event logs to be processed.
   *
   * @param filterNLogs    number of event logs to be selected
   * @param matchlogs      keyword to match file names in the directory
   * @param eventLogsPaths Array of event log paths
   * @return event logs to be processed
   */
  def processAllPaths(
      filterNLogs: ScallopOption[String],
      matchlogs: ScallopOption[String],
      eventLogsPaths: List[String]): ArrayBuffer[Path] = {

    var allPathsWithTimestamp: Map[Path, Long] = Map.empty[Path, Long]

    for (pathString <- eventLogsPaths) {
      val paths = ProfileUtils.stringToPath(pathString)
      if (paths.nonEmpty) {
        allPathsWithTimestamp ++= paths
      }
    }
    // Filter the event logs to be processed based on the criteria. If it is not provided in the
    // command line, then return all the event logs processed above.
    if (matchlogs.isDefined || filterNLogs.isDefined) {
      if (matchlogs.isDefined) {
        allPathsWithTimestamp = allPathsWithTimestamp.filter { case (path, _) =>
          path.getName.contains(matchlogs.toOption.get)
        }
      }
      if (filterNLogs.isDefined) {
        val numberofEventLogs = filterNLogs.toOption.get.split("-")(0).toInt
        val criteria = filterNLogs.toOption.get.split("-")(1)
        if (criteria.equals("newest")) {
          allPathsWithTimestamp = LinkedHashMap(
            allPathsWithTimestamp.toSeq.sortWith(_._2 > _._2): _*)
        } else if (criteria.equals("oldest")) {
          allPathsWithTimestamp = LinkedHashMap(
            allPathsWithTimestamp.toSeq.sortWith(_._2 < _._2): _*)
        } else {
          logError("Criteria should be either newest or oldest")
          System.exit(1)
        }
        ArrayBuffer(allPathsWithTimestamp.keys.toSeq.take(numberofEventLogs): _*)
      } else {
        // return event logs which contains the keyword.
        ArrayBuffer(allPathsWithTimestamp.keys.toSeq: _*)
      }
    } else { // send all event logs for processing
      ArrayBuffer(allPathsWithTimestamp.keys.toSeq: _*)
    }
  }
}
