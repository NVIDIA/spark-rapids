package org.apache.spark.sql.types.shims

import java.time.ZoneId

import scala.util.Try

import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types.{AnsiIntervalType, AnyTimestampType, DataType, DateType}

object PartitionValueCastShims {
  def isSupportedType(dt: DataType): Boolean = dt match {
    // Timestamp types
    case dt if AnyTimestampType.acceptsType(dt) => true
    case it: AnsiIntervalType => true
    case _ => false
  }

  // only for TimestampType TimestampNTZType
  def castTo(desiredType: DataType, value: String, zoneId: ZoneId): Any = desiredType match {
    case dt if AnyTimestampType.acceptsType(desiredType) =>
      Try {
        Cast(Literal(unescapePathName(value)), dt, Some(zoneId.getId)).eval()
      }.getOrElse {
        Cast(Cast(Literal(value), DateType, Some(zoneId.getId)), dt).eval()
      }
  }
}
