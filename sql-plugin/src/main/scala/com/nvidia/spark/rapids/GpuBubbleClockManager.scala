/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

/**
 * GpuBubbleTime, a metric that measures periods when the GPU pipeline contains "bubbles" - gaps
 * where the GPU is underutilized. Currently, whether the GPU is fully utilized or not is
 * determined by checking if there are any pending requests for the GpuSemaphore through the
 * `GpuSemaphore.isDeviceBusy` API.
 *
 * GpuBubbleClockManager runs on a daemon thread to track GpuBubbleTime for asynchronous
 * non-device runners across the Spark executor. The manager maintains wall-clock timers that
 * measure elapsed time during periods when the GPU is underloaded (not busy).
 *
 * The manager periodically polls GPU utility through `GpuSemaphore.isDeviceBusy` and coordinates
 * timer state changes when GPU status transitions occur. All registered timers start
 * accumulating time when the GPU becomes underloaded and pause when GPU being saturated.
 *
 * The update of registered timers are synchronized with a lock to ensure thread-safe access while
 * minimizing lock contention. The manager uses 1ms polling intervals to balance responsiveness
 * with CPU overhead, regarding that 1ms is precise enough for measuring wall time.
 */
private class GpuBubbleClockManager extends Runnable {

  /**
   * Periodical loop to check GPU utility and update clocks accordingly.
   */
  override def run(): Unit = {
    while (true) {
      val newVal = !GpuSemaphore.isDeviceBusy
      if (isDeviceIdle != newVal) {
        // Enter a critical section to update `clocks` and `isDeviceIdle` atomically
        lock.lock()
        val ts = System.nanoTime()
        if (newVal) {
          // GPU just exit a busy period, start all clocks
          clocks.foreach { case (_, clk) => clk.tic(ts) }
        } else {
          // GPU just enter a busy period, stop all clocks
          clocks.foreach { case (_, clk) => clk.toc(ts) }
        }
        isDeviceIdle = newVal
        lock.unlock()
      }
      Thread.sleep(1L)
    }
  }

  /**
   * Register a new clock to the manager, a synchronized operation.
   *
   * @param clk the clock to be registered
   * @return a unique id for the registered clock
   */
  def registerClock(clk: GpuBubbleWallClock): Long = {
    lock.lock()
    try {
      // Sync the clock with current GPU status during initialization
      clk.init(isDeviceIdle)
      // Find a unique id for the new clock
      var id = getNextId
      while (clocks.contains(id)) {
        id = getNextId
      }
      // Register the clock
      clocks(id) = clk
      id
    } finally {
      lock.unlock()
    }
  }

  /**
   * Complete a clock and remove it from the manager, a synchronized operation.
   *
   * @param id the id of the clock to be completed
   * @return the elapsed time in nanoseconds recorded by the clock
   */
  def completeClock(id: Long): Long = {
    lock.lock()
    try {
      clocks.remove(id) match {
        case None =>
          throw new IllegalArgumentException(s"Invalid clock id: $id")
        case Some(clk) =>
          clk.finish()
      }
    } finally {
      lock.unlock()
    }
  }

  // Lock to synchronize modifications to `clocks` and `isDeviceIdle`
  private val lock = new ReentrantLock()

  // Map to hold all registered clocks, only be accessed under lock
  private val clocks = mutable.Map.empty[Long, GpuBubbleWallClock]

  // Initialize the GPU status
  private var isDeviceIdle: Boolean = !GpuSemaphore.isDeviceBusy

  // This generator shall only be accessed under lock
  private def getNextId: Long = {
    val id = nextId
    if (id == Long.MaxValue) {
      nextId = 0L
    } else {
      nextId += 1
    }
    id
  }

  @volatile private var nextId: Long = 0L
}

/**
 * A wall-clock timer that measures elapsed time during periods when the GPU is underloaded.
 * The timer is designed to be managed by GpuBubbleClockManager.
 */
private class GpuBubbleWallClock {

  def init(isDeviceIdle: Boolean): Unit = {
    require(!initialized, "Timer was already initialized")
    initialized = true
    if (isDeviceIdle) {
      startTime = System.nanoTime()
    }
  }

  def tic(ts: Long): Unit = {
    require(initialized, "Timer was not initialized")
    if (startTime == 0L) {
      startTime = ts
    }
  }

  def toc(ts: Long): Unit = {
    require(initialized, "Timer was not initialized")
    if (startTime > 0L) {
      elapsedNanos += ts - startTime
      startTime = 0L
    }
  }

  def finish(): Long = {
    require(initialized, "Timer was not initialized")
    require(!finished, "Timer was already finished")
    toc(System.nanoTime())
    finished = true
    elapsedNanos
  }

  private var initialized = false

  private var finished = false

  private var startTime: Long = 0L

  private var elapsedNanos: Long = 0L
}

object GpuBubbleClockManager {

  /**
   * Create and register a new GpuBubbleWallClock to the singleton manager instance.
   */
  def newClock(): Long = {
    val clk = new GpuBubbleWallClock()
    instance.registerClock(clk)
  }

  /**
   * Complete a clock and remove it from the singleton manager instance, pairing with `newClock()`.
   */
  def completeClock(id: Long): Long = {
    instance.completeClock(id)
  }

  // Singleton instance that manages GpuBubbleTime tracking across the entire Spark executor.
  private lazy val instance: GpuBubbleClockManager = {
    val mgr = new GpuBubbleClockManager()
    val thread = new Thread(mgr, "GpuBubbleClockManager")
    thread.setDaemon(true)
    thread.start()
    mgr
  }
}
