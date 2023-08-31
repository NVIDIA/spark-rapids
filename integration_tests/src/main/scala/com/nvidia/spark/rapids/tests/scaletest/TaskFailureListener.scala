package com.nvidia.spark.rapids.tests.scaletest

import scala.collection.mutable.ListBuffer

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.{Success, TaskEndReason}

/**
 * This Listener Class is used to track the "retry tasks" in Spark jobs.
 * It is possible that a job finally finished successfully with tasks failed at first but succeeded
 * after retry while such exceptions cannot be caught by try-catch around a query execution.
 * The onTaskEnd event carries task failure reasons that we will record in the test report.
 */
class TaskFailureListener extends SparkListener {
  val taskFailures = new ListBuffer[TaskEndReason]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.reason match {
      case Success =>
      case reason => taskFailures += reason
    }
    super.onTaskEnd(taskEnd)
  }
}
