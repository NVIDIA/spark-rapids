/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class GpuInSet(
    child: Expression,
    list: Seq[Literal]) extends GpuUnaryExpression with Predicate {
  @transient private[this] lazy val _needles: ThreadLocal[ColumnVector] =
    new ThreadLocal[ColumnVector]

  require(list != null, "list should not be null")

  override def nullable: Boolean = child.nullable || list.contains(null)

  override def doColumnar(haystack: GpuColumnVector): ColumnVector = {
    val needles = getNeedles
    haystack.getBase.contains(needles)
  }

  private def getNeedles: ColumnVector = {
    var needleVec = _needles.get
    if (needleVec == null) {
      needleVec = buildNeedles
      _needles.set(needleVec)
      TaskContext.get.addTaskCompletionListener[Unit](_ => _needles.get.close())
    }
    needleVec
  }

  private def buildNeedles: ColumnVector = {
    val values = list.map(_.value)
    child.dataType match {
    case BooleanType =>
      val bools = values.asInstanceOf[Seq[java.lang.Boolean]]
      ColumnVector.fromBoxedBooleans(bools:_*)
    case ByteType =>
      val bytes = values.asInstanceOf[Seq[java.lang.Byte]]
      ColumnVector.fromBoxedBytes(bytes:_*)
    case ShortType =>
      val shorts = values.asInstanceOf[Seq[java.lang.Short]]
      ColumnVector.fromBoxedShorts(shorts:_*)
    case IntegerType =>
      val ints = values.asInstanceOf[Seq[java.lang.Integer]]
      ColumnVector.fromBoxedInts(ints:_*)
    case LongType =>
      val longs = values.asInstanceOf[Seq[java.lang.Long]]
      ColumnVector.fromBoxedLongs(longs:_*)
    case FloatType =>
      val floats = values.asInstanceOf[Seq[java.lang.Float]]
      ColumnVector.fromBoxedFloats(floats:_*)
    case DoubleType =>
      val doubles = values.asInstanceOf[Seq[java.lang.Double]]
      ColumnVector.fromBoxedDoubles(doubles:_*)
    case DateType =>
      val dates = values.asInstanceOf[Seq[java.lang.Integer]]
      ColumnVector.timestampDaysFromBoxedInts(dates:_*)
    case TimestampType =>
      val timestamps = values.asInstanceOf[Seq[java.lang.Long]]
      ColumnVector.timestampMicroSecondsFromBoxedLongs(timestamps:_*)
    case StringType =>
      val strings = values.asInstanceOf[Seq[UTF8String]]
      withResource(HostColumnVector.builder(DType.STRING, strings.size)) { builder =>
        strings.foreach(s => builder.appendUTF8String(s.getBytes))
        builder.buildAndPutOnDevice()
      }
    case t: DecimalType =>
      val decs = values.asInstanceOf[Seq[Decimal]]
      withResource(HostColumnVector.builder(
        DecimalUtil.createCudfDecimal(t.precision, t.scale), decs.size)) { builder =>
        if (DecimalType.is32BitDecimalType(t)) {
          decs.foreach(d => builder.appendUnscaledDecimal(d.toUnscaledLong.toInt))
        } else {
          decs.foreach(d => builder.appendUnscaledDecimal(d.toUnscaledLong))
        }
        builder.buildAndPutOnDevice()
      }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported list type: ${child.dataType}")
    }
  }

  override def toString: String = s"$child INSET ${list.mkString("(", ",", ")")}"

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = list.map(Literal(_).sql).mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}
