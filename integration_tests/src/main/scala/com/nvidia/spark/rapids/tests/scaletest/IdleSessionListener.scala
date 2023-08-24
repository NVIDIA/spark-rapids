package com.nvidia.spark.rapids.tests.scaletest

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

class IdleSessionListener extends SparkListener{

  val runningJobCount = new AtomicLong(0)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // A new job has started; increment the running job count.
    runningJobCount.incrementAndGet()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    // A job has ended; decrement the running job count.
    runningJobCount.decrementAndGet()
  }

  def isIdle(): Boolean = {
    // Determine idleness based on the running job count.
    runningJobCount.get() == 0
  }

  def isBusy(): Boolean = {
    !isIdle()
  }

}
