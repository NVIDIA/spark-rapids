package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkUpgradeException

object SparkUpgradeExceptionShims {

  def newSparkUpgradeException(
                                version: String,
                                message: String,
                                cause: Throwable): SparkUpgradeException = {
    new SparkUpgradeException(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      Map(version -> message),
      cause)
  }
}
