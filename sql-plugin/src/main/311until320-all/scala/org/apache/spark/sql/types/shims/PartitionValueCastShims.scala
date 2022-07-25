package org.apache.spark.sql.types.shims

import java.time.ZoneId

import org.apache.spark.sql.types.DataType

object PartitionValueCastShims {
  def isSupportedType(dt: DataType): Boolean = false

  def castTo(desiredType: DataType, value: String, zoneId: ZoneId): Any = {
    throw new IllegalArgumentException(s"Unexpected type $desiredType")
  }
}
