package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.EventLogInfo

import org.apache.spark.sql.SparkSession

abstract class AppBase(
    val numOutputRows: Int,
    val sparkSession: SparkSession,
    val eventLogInfo: EventLogInfo) {

  var sparkVersion: String = ""
  var appEndTime: Option[Long] = None
}
