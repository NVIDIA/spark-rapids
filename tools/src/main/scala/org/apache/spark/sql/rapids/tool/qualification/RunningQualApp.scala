package org.apache.spark.sql.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkEnv

class RunningQualApp(
    hadoopConf: Configuration,
    pluginTypeChecker: Option[PluginTypeChecker],
    readScorePercent: Int)
  extends QualAppInfo(None, hadoopConf, pluginTypeChecker, readScorePercent) {

  def initApp(): Unit = {
    val appName = SparkEnv.get.conf.get("spark.app.name", "")
    val appIdConf = SparkEnv.get.conf.getOption("spark.app.id")
    val appStartTime = SparkEnv.get.conf.get("spark.app.startTime", "-1")

    // start event doesn't happen so initial it
    val thisAppInfo = QualApplicationInfo(
      appName,
      appIdConf,
      appStartTime.toLong,
      "",
      None,
      None,
      endDurationEstimated = false
    )
    appInfo = Some(thisAppInfo)
  }

  initApp()
}
