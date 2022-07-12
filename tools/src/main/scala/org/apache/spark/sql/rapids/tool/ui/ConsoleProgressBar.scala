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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging

/**
 * ConsoleProgressBar shows the progress of qualification in the console.
 * This class is inspired from org.apache.spark.ui.ConsoleProgressBar.
 *
 * By default, the progress bar will be shown in the stdout.
 * If the inline flag is enabled, the progress bar will be displayed and updated in a single console
 * line. When enabled, setting log level to INFO could interfere with the progress bar updates.
 */
class ConsoleProgressBar(
    prefix: String, totalCount: Integer, inlinePBEnabled: Boolean = false) extends Logging {
  private val CR = '\r'
  // The width of terminal
  private val TerminalWidth = sys.env.getOrElse("COLUMNS", "120").toInt
  private val successCounter = new AtomicInteger(0)
  private val failureCounter = new AtomicInteger(0)
  // Delay to show up a progress bar, in milliseconds
  private val updatePeriodMSec = 500L
  private val firstDelayMSec = 1000L
  private val updateTimeOut =
    if (inlinePBEnabled) { // shorter timeout when PBis inlined
      10 * 1000L
    } else {
      60 * 1000L
    }

  private var lastFinishTime = 0L
  private var lastUpdateTime = 0L
  private var lastProgressBar = ""
  private var currentCount = 0
  private var lastUpdatedCount = 0

  // Schedule a refresh thread to run periodically
  private val timer = new Timer("refresh progress", true)
  timer.schedule(new TimerTask{
    override def run(): Unit = {
      refresh()
    }
  }, firstDelayMSec, updatePeriodMSec)

  private def show(now: Long): Unit = {
    val percent = currentCount * 100 / totalCount
    val header = s"$prefix Progress $percent% ["
    val tailer = s"] (${successCounter.intValue()} succeeded + " +
      s"${failureCounter.intValue()} failed) / $totalCount"
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
    currentCount = successCounter.intValue() + failureCounter.intValue()
    // only refresh if it's changed OR after updateTimeOut seconds
    if (lastUpdatedCount == currentCount && now - lastUpdateTime < updateTimeOut) {
      return
    }
    show(now)
  }

  def incrementSuccessful(): Unit = {
    successCounter.incrementAndGet()
  }

  def incrementFailed(): Unit = {
    failureCounter.incrementAndGet()
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
    currentCount = successCounter.intValue() + failureCounter.intValue()
    lastFinishTime = System.currentTimeMillis()
    show(lastFinishTime)
    clear()
  }

  /**
   * Tear down the timer thread.
   */
  private def stop(): Unit = timer.cancel()
}
