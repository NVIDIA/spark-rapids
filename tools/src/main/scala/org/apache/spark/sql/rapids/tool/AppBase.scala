package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.EventLogInfo

abstract class AppBase(
    val numOutputRows: Int,
    val eventLogInfo: EventLogInfo) {

  var sparkVersion: String = ""
  var appEndTime: Option[Long] = None
}
