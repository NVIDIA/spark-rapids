package com.nvidia.spark.rapids.shims

object RegExpShim {
  // Handle regexp_replace inconsistency from https://issues.apache.org/jira/browse/SPARK-39107
  def reproduceEmptyStringBug(): Boolean = false
}
