/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.functions

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.{ExprMeta, GpuExpression, StaticInvokeMeta}
import com.nvidia.spark.rapids.iceberg.fieldIndex
import org.apache.iceberg.Schema
import org.apache.iceberg.transforms.Transform

import org.apache.iceberg.spark.functions.BucketFunction.{BucketInt, BucketLong}
import org.apache.iceberg.spark.functions.DaysFunction.{DateToDaysFunction, TimestampToDaysFunction}
import org.apache.iceberg.spark.functions.HoursFunction.TimestampToHoursFunction
import org.apache.iceberg.spark.functions.MonthsFunction.{DateToMonthsFunction, TimestampToMonthsFunction}
import org.apache.iceberg.spark.functions.YearsFunction.{DateToYearsFunction, TimestampToYearsFunction}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{ByteType, DataType, DataTypes, DateType, IntegerType, LongType, ShortType, StructType, TimestampNTZType, TimestampType}


/**
 * Iceberg partition transform types.
 * <br/>
 * We need to build our own transform types because the ones in iceberg are
 */
sealed trait GpuTransform {
  def support(inputType: DataType, nullable: Boolean): Boolean
  def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit
  def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression
}

case class GpuBucket(bucket: Int) extends GpuTransform {
  override def support(inputType: DataType, nullable: Boolean): Boolean = {
    !nullable && GpuBucket.isSupported(inputType)
  }

  override def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    val bucketExpr = meta.wrapped.arguments.head
    if (bucketExpr.dataType != DataTypes.IntegerType) {
      throw new IllegalStateException(
        s"BucketFunction number of buckets must be an integer, got ${bucketExpr.dataType}")
    }

    val valueExpr = meta.wrapped.arguments(1)
    if (valueExpr.nullable) {
      meta.willNotWorkOnGpu(s"Gpu bucket function does not support nullable values for type " +
          s"${valueExpr.dataType}")
    }

    if (!GpuBucket.isSupported(valueExpr.dataType)) {
      meta.willNotWorkOnGpu(s"Gpu bucket function does not support type ${valueExpr.dataType} " +
          s"as values")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
    GpuBucketExpression(left, right)
  }
}

object GpuBucket {
  def isSupported(dataType: DataType): Boolean = {
    dataType match {
      case ByteType |  ShortType | IntegerType | DateType |
           LongType | TimestampType | TimestampNTZType => true
      case _ => false
    }
  }
}

case object GpuYears extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true

  override def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    val valueExpr = meta.wrapped.arguments.head
    // Years is supported for Date and Timestamp
    if (valueExpr.dataType != DateType && valueExpr.dataType != TimestampType) {
       meta.willNotWorkOnGpu(s"Gpu years function does not support type ${valueExpr.dataType} " +
           s"as values")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    GpuYearsExpression(meta.childExprs.head.convertToGpu())
  }
}

case object GpuMonths extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true

  override def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    val valueExpr = meta.wrapped.arguments.head
    // Months is supported for Date and Timestamp
    if (valueExpr.dataType != DateType && valueExpr.dataType != TimestampType) {
      meta.willNotWorkOnGpu(s"Gpu months function does not support type ${valueExpr.dataType} " +
          s"as values")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    GpuMonthsExpression(meta.childExprs.head.convertToGpu())
  }
}

case object GpuDays extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true

  override def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    val valueExpr = meta.wrapped.arguments.head
    // Days is supported for Date and Timestamp
    if (valueExpr.dataType != DateType && valueExpr.dataType != TimestampType) {
      meta.willNotWorkOnGpu(s"Gpu days function does not support type ${valueExpr.dataType} " +
          s"as values")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    GpuDaysExpression(meta.childExprs.head.convertToGpu())
  }
}

case object GpuHours extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true

  override def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    val valueExpr = meta.wrapped.arguments.head
    if (valueExpr.dataType != TimestampType) {
      meta.willNotWorkOnGpu(s"Gpu hours function does not support type ${valueExpr.dataType} " +
          s"as values")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    GpuHoursExpression(meta.childExprs.head.convertToGpu())
  }
}

object GpuTransform {
  def apply(transform: String): GpuTransform = {
    if (transform.startsWith("bucket")) {
      val bucket = transform.substring("bucket[".length, transform.length - 1).toInt
      GpuBucket(bucket)
    } else if (transform.startsWith("year")) {
      GpuYears
    } else if (transform.startsWith("month")) {
      GpuMonths
    } else if (transform.startsWith("day")) {
      GpuDays
    } else if (transform.startsWith("hour")) {
      GpuHours
    } else {
      throw new IllegalArgumentException(s"Unsupported transform: $transform")
    }
  }

  def apply(icebergTransform: Transform[_, _]): GpuTransform = {
    GpuTransform(icebergTransform.toString)
  }

  def apply(expr: StaticInvoke): Try[GpuTransform] = {
    val staticObj = expr.staticObject
    val name = staticObj.getName
    if (name.equals(classOf[BucketInt].getName) ||
        name.equals(classOf[BucketLong].getName)) {
      // For bucket, the number of buckets is the first argument.
      // However, here we only need the transform type to do the check.
      // The number of buckets is not used in tagExprForGpu.
      // So we can use a dummy bucket number here.
      Success(GpuBucket(0))
    } else if (name.equals(classOf[DateToYearsFunction].getName) ||
        name.equals(classOf[TimestampToYearsFunction].getName)) {
      Success(GpuYears)
    } else if (name.equals(classOf[DateToMonthsFunction].getName) ||
        name.equals(classOf[TimestampToMonthsFunction].getName)) {
      Success(GpuMonths)
    } else if (name.equals(classOf[DateToDaysFunction].getName) ||
        name.equals(classOf[TimestampToDaysFunction].getName)) {
      Success(GpuDays)
    } else if (name.equals(classOf[TimestampToHoursFunction].getName)) {
      Success(GpuHours)
    } else {
      Failure(new IllegalArgumentException(s"Unsupported transform: $name"))
    }
  }

  def tryFrom(icebergTransform: Transform[_, _]): Try[GpuTransform] = {
    Try {
      GpuTransform(icebergTransform)
    }
  }
}

case class GpuFieldTransform(sourceFieldId: Int, transform: GpuTransform) {
  def supports(inputType: StructType, inputSchema: Schema): Boolean = {
    val fieldIdx = fieldIndex(inputSchema, sourceFieldId)
    val sparkField = inputType.fields(fieldIdx)
    transform.support(sparkField.dataType, sparkField.nullable)
  }
}


