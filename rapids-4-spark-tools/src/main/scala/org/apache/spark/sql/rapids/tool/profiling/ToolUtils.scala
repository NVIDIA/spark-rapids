package org.apache.spark.sql.rapids.tool.profiling

import org.apache.spark.internal.config

object ToolUtils {

  def isGPUMode(properties: collection.mutable.Map[String, String]): Boolean = {
    (properties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin")
        && properties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean)
  }
}
