package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.sql.delta.stats.DeltaStatistics

trait ShimUsesMetadataFields {
  val NUM_RECORDS = DeltaStatistics.NUM_RECORDS
  val MIN = DeltaStatistics.MIN
  val MAX = DeltaStatistics.MAX
  val NULL_COUNT = DeltaStatistics.NULL_COUNT
}