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

package org.apache.spark.sql.rapids.tool

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}
import scala.collection.JavaConverters._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.EventLogPathProcessor.logError
import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import scala.collection.mutable.LinkedHashMap

class AppFilterImpl(
    numRows: Int,
    hadoopConf: Configuration,
    timeout: Option[Long],
    nThreads: Int) extends Logging {

  private val appsForFiltering = new ConcurrentLinkedQueue[AppFilterReturnParameters]()
  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("qualAppFilter" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val qualFilterthreadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
      .asInstanceOf[ThreadPoolExecutor]

  private class FilterThread(path: EventLogInfo) extends Runnable {
    def run: Unit = filterEventLog(path, numRows, hadoopConf)
  }

  def filterEventLogs(
      allPaths: Seq[EventLogInfo],
      appArgs: QualificationArgs): Seq[EventLogInfo] = {
    allPaths.foreach { path =>
      try {
        qualFilterthreadPool.submit(new FilterThread(path))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
    // wait for the threads to finish processing the files
    qualFilterthreadPool.shutdown()
    if (!qualFilterthreadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
          " stopping processing any more event logs")
      qualFilterthreadPool.shutdownNow()
    }

    // This will be required to do the actual filtering
    val apps = appsForFiltering.asScala
    val ascendingOrder = apps.toSeq.sortBy(_.appInfo.get.startTime)
    val descendingOrder = ascendingOrder.reverse

    println(s"ASCENDING IS $ascendingOrder\n\n")
    println(s"DESCENDING IS $descendingOrder\n\n")
    val filterAppName = appArgs.applicationName.getOrElse("")
    val filterCriteria = appArgs.filterCriteria.getOrElse("")
    if (appArgs.filterCriteria.isSupplied && filterCriteria.nonEmpty) {
      if (filterCriteria.endsWith("-overall")) {
        println("ENDS WITH OVERALL")
        val filteredInfo = filterCriteria.split("-")
        val numberofEventLogs = filteredInfo(0).toInt
        val criteria = filteredInfo(1)
        val matched = if (criteria.equals("newest")) {
          //LinkedHashMap(apps.toSeq.sortWith(_._2 > _._2): _*)
         //LinkedHashMap(apps.toSeq.sortWith(_.appInfo.get.startTime > _.appInfo.get.startTime): _*)
         //val t = apps.toSeq.sortBy(_.appInfo.get.startTime).reverse
         val ascendingOrder = apps.toSeq.sortBy(_.appInfo.get.startTime)
         ascendingOrder
        } else if (criteria.equals("oldest")) {
          apps.toSeq.sortBy(_.appInfo.get.startTime).reverse
        } else {
          logError("Criteria should be either newest or oldest")
          //Map.empty[EventLogInfo, Long]
          Seq[AppFilterReturnParameters]()
        }
        val t = matched.map(_.eventlog).take(numberofEventLogs)

      } else {
        println("ENDS WITH PER-APP-NAME")
      }
    }
    if (appArgs.applicationName.isSupplied && filterAppName.nonEmpty) {
      val filtered = apps.filter { app =>
        val appNameOpt = app.appInfo.map(_.appName)
        if (appNameOpt.isDefined) {
          appNameOpt.get.equals(filterAppName)
        } else {
          // in complete log file
          false
        }
      }
      filtered.map(_.eventlog).toSeq
    } else {
      apps.map(x => x.eventlog).toSeq
    }
  }

  case class AppFilterReturnParameters(
      appInfo: Option[ApplicationStartInfo],
      eventlog: EventLogInfo)

  private def filterEventLog(
      path: EventLogInfo,
      numRows: Int,
      hadoopConf: Configuration): Unit = {

    val startAppInfo = new FilterAppInfo(numRows, path, hadoopConf)
    val appInfo = AppFilterReturnParameters(startAppInfo.appInfo, path)
    appsForFiltering.add(appInfo)
  }
}
