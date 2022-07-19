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

import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * ConsoleProgressBar shows the progress of the tool (Qualification/Profiler) in the console.
 * This class is inspired from org.apache.spark.ui.ConsoleProgressBar.
 *
 * The implementation defines three counters: succeeded, failed, and N/A.
 * The list can be extended to add new counters during runtime which gives flexibility to have
 * custom statistics for different contexts.
 *
 * By default, the progress bar will be shown in the stdout.
 * Sample generated line:
 *   (.)+ Progress (\\d+)% [==> ] ((\\d+) succeeded + (\\d+) failed + (\\d+) N/A) / [(\\d+)]
 *   toolName Progress 71% [=======>   ] (83 succeeded + 2 failed + 0 N/A) / 119
 *
 * At the end of execution, it dumps all the defined the counters.
 *
 * @param prefix Gives a title to the PB
 * @param totalCount The total number of items to be processed by the Tool.
 *                   Caller needs to verify the value is not 0.
 * @param inlinePBEnabled When enabled, the progress bar will be displayed and re-drawn in a single
 *                        console line. Note that redirecting other log messages to stdout while the
 *                        flag is enabled would cause the PB to be overridden.
 */
class ConsoleProgressBar(
    prefix: String, totalCount: Long, inlinePBEnabled: Boolean = false) extends Logging {
  import ConsoleProgressBar._
  private val successCounter = new AtomicLong(0)
  private val failureCounter = new AtomicLong(0)
  private val statusNotAvailableCounter = new AtomicLong(0)
  private val totalCounter = new AtomicLong(0)

  private val metrics = mutable.LinkedHashMap[String, AtomicLong](
    PROCESS_SUCCESS_COUNT -> successCounter,
    PROCESS_FAILURE_COUNT -> failureCounter,
    PROCESS_NOT_AVAILABLE_COUNT -> statusNotAvailableCounter,
    EXECUTION_TOTAL_COUNTER -> totalCounter)

  // Delay to show up a progress bar, in milliseconds
  private val updatePeriodMSec = 500L
  private val firstDelayMSec = 1000L
  private val updateTimeOut =
    if (inlinePBEnabled) { // shorter timeout when PB is inlined
      10 * 1000L
    } else {
      60 * 1000L
    }

  private val launchTime = System.currentTimeMillis()
  private var lastFinishTime = 0L
  private var lastUpdateTime = 0L
  private var lastProgressBar = ""
  private var currentCount = 0L
  private var lastUpdatedCount = 0L

  // Schedule a refresh thread to run periodically
  private val timer = new Timer("refresh tool progress", true)
  timer.schedule(new TimerTask{
    override def run(): Unit = {
      refresh()
    }
  }, firstDelayMSec, updatePeriodMSec)

  /**
   * Add a new counter with a unique key.
   * @param key a unique name to define the counter.
   * @return if key is defined in metrics, return its associated value.
   *         Otherwise, update the metrics with the mapping key -> d and return d.
   */
  def addCounter(key: String): AtomicLong = {
    this.synchronized {
      metrics.getOrElseUpdate(key, new AtomicLong(0))
    }
  }

  // Get the counter by name. This is used to access newly defined counters.
  def getCounterByName(key: String): Option[AtomicLong] = {
    metrics.get(key)
  }

  // Increment a counter by key. This is used to access newly defined counters.
  def incCounterByName(key: String): Long = {
    metrics.get(key) match {
      case Some(e) => e.incrementAndGet()
      case None => 0
    }
  }

  // Decrement a counter by key. This is used to access newly defined counters.
  def decCounterByName(key: String): Long = {
    metrics.get(key) match {
      case Some(e) => e.decrementAndGet()
      case None => 0
    }
  }

  def reportSuccessfulProcess(): Unit = {
    successCounter.incrementAndGet()
    totalCounter.incrementAndGet()
  }

  def reportFailedProcess(): Unit = {
    failureCounter.incrementAndGet()
    totalCounter.incrementAndGet()
  }

  def reportUnkownStatusProcess(): Unit = {
    statusNotAvailableCounter.incrementAndGet()
    totalCounter.incrementAndGet()
  }

  def metricsToString: String = {
    val sb = new mutable.StringBuilder()
    metrics.foreach { case (name, counter) =>
      sb.append('\t').append(name).append(" = ").append(counter.longValue()).append('\n')
    }
    sb.toString()
  }

  private def show(now: Long): Unit = {
    val percent = currentCount * 100 / totalCount
    val header = s"$prefix Progress $percent% ["
    val tailer =
      s"] (${successCounter.longValue()} succeeded + " +
        s"${failureCounter.longValue()} failed + " +
        s"${statusNotAvailableCounter.longValue()} N/A) / $totalCount"
    val w = TerminalWidth - header.length - tailer.length
    val bar = if (w > 0) {
      val percent = w * currentCount / totalCount
      (0 until w).map { i =>
        if (i < percent) "=" else if (i == percent) ">" else " "
      }.mkString("")
    } else {
      ""
    }
    val consoleLine = header + bar + tailer
    if (inlinePBEnabled) {
      System.out.print(CR + consoleLine + CR)
    } else {
      System.out.println(consoleLine)
    }
    lastUpdateTime = now
    lastUpdatedCount = currentCount
    lastProgressBar = consoleLine
  }

  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    if (now - lastFinishTime < firstDelayMSec) {
      return
    }
    currentCount = totalCounter.longValue()
    // only refresh if it's changed OR after updateTimeOut seconds
    if (lastUpdatedCount == currentCount && now - lastUpdateTime < updateTimeOut) {
      return
    }
    show(now)
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clear(): Unit = {
    if (inlinePBEnabled && lastProgressBar.nonEmpty) {
      System.out.printf(CR + " " * TerminalWidth + CR)
    }
    lastProgressBar = ""
    lastUpdatedCount = 0
  }

  /**
   * Mark all processing as finished.
   */
  def finishAll(): Unit = synchronized {
    stop()
    currentCount = totalCounter.longValue()
    lastFinishTime = System.currentTimeMillis()
    show(lastFinishTime)
    clear()
    System.out.printf(
      CR + s"$prefix execution time: ${lastFinishTime - launchTime}ms\n$metricsToString")
  }

  /**
   * Tear down the timer thread.
   */
  private def stop(): Unit = timer.cancel()
}

object ConsoleProgressBar {
  private val CR = '\r'
  // The width of terminal
  private val TerminalWidth = sys.env.getOrElse("COLUMNS", "120").toInt

  val PROCESS_SUCCESS_COUNT = "process.success.count"
  val PROCESS_FAILURE_COUNT = "process.failure.count"
  val PROCESS_NOT_AVAILABLE_COUNT = "process.NA.count"
  val EXECUTION_TOTAL_COUNTER = "execution.total.count"
}
