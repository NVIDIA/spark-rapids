/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics, Union}
import org.apache.spark.sql.types._

/**
 * Copied from Spark's UnionEstimation.scala
 *
 * Modifications:
 *
 * - Call GpuStatsPlanVisitor to get stats for child, instead of calling child.stats
 * - Removed check that union inputs have row count estimates, because they always do now
 * - Commented out TimestampNTZType and AnsiIntervalType temporarily (TODO: fix this)
 */
object GpuUnionEstimation extends Logging {

  private def createStatComparator(dt: DataType): (Any, Any) => Boolean = dt match {
    case ByteType => (a: Any, b: Any) =>
      implicitly[Ordering[Byte]].lt(a.asInstanceOf[Byte], b.asInstanceOf[Byte])
    case ShortType => (a: Any, b: Any) =>
      implicitly[Ordering[Short]].lt(a.asInstanceOf[Short], b.asInstanceOf[Short])
    case IntegerType => (a: Any, b: Any) =>
      implicitly[Ordering[Int]].lt(a.asInstanceOf[Int], b.asInstanceOf[Int])
    case LongType => (a: Any, b: Any) =>
      implicitly[Ordering[Long]].lt(a.asInstanceOf[Long], b.asInstanceOf[Long])
    case FloatType => (a: Any, b: Any) =>
      implicitly[Ordering[Float]].lt(a.asInstanceOf[Float], b.asInstanceOf[Float])
    case DoubleType => (a: Any, b: Any) =>
      implicitly[Ordering[Double]].lt(a.asInstanceOf[Double], b.asInstanceOf[Double])
    case _: DecimalType => (a: Any, b: Any) =>
      implicitly[Ordering[Decimal]].lt(a.asInstanceOf[Decimal], b.asInstanceOf[Decimal])
    case DateType => (a: Any, b: Any) =>
      implicitly[Ordering[Int]].lt(a.asInstanceOf[Int],
        b.asInstanceOf[Int])
    case TimestampType => (a: Any, b: Any) =>
      implicitly[Ordering[Long]].lt(a.asInstanceOf[Long],
        b.asInstanceOf[Long])
//    case TimestampNTZType => (a: Any, b: Any) =>
//      TimestampNTZType.ordering.lt(a.asInstanceOf[TimestampNTZType.InternalType],
//        b.asInstanceOf[TimestampNTZType.InternalType])
//    case i: AnsiIntervalType => (a: Any, b: Any) =>
//      i.ordering.lt(a.asInstanceOf[i.InternalType], b.asInstanceOf[i.InternalType])
    case _ =>
      throw new IllegalStateException(s"Unsupported data type: ${dt.catalogString}")
  }

  private def isTypeSupported(dt: DataType): Boolean = dt match {
    case ByteType | IntegerType | ShortType | FloatType | LongType |
         DoubleType | DateType | _: DecimalType | TimestampType  => true
    //TODO these are private
//    case TimestampNTZType | _: AnsiIntervalType => true
    case _ => false
  }

  def estimate(union: Union): Option[Statistics] = {
    logDebug("GpuUnionEstimation.estimate() BEGIN")
    val sizeInBytes = union.children.map(ch => GpuStatsPlanVisitor.visit(ch).sizeInBytes).sum
    val outputRows = Some(union.children.map(ch => GpuStatsPlanVisitor.visit(ch).rowCount.get).sum)

    val newMinMaxStats = computeMinMaxStats(union)
    val newNullCountStats = computeNullCountStats(union)
    val newAttrStats = {
      val baseStats = AttributeMap(newMinMaxStats)
      val overwriteStats = newNullCountStats.map { case attrStat@(attr, stat) =>
        baseStats.get(attr).map { baseStat =>
          attr -> baseStat.copy(nullCount = stat.nullCount)
        }.getOrElse(attrStat)
      }
      AttributeMap(newMinMaxStats ++ overwriteStats)
    }

    val x = Some(
      Statistics(
        sizeInBytes = sizeInBytes,
        rowCount = outputRows,
        attributeStats = newAttrStats))

    logDebug(s"GpuUnionEstimation.estimate() END -> $x")

    x
  }

  private def computeMinMaxStats(union: Union): Seq[(Attribute, ColumnStat)] = {
    val unionOutput = union.output
    val attrToComputeMinMaxStats = union.children.map(_.output).transpose.zipWithIndex.filter {
      case (attrs, outputIndex) => isTypeSupported(unionOutput(outputIndex).dataType) &&
        // checks if all the children has min/max stats for an attribute
        attrs.zipWithIndex.forall {
          case (attr, childIndex) =>
            val attrStats = GpuStatsPlanVisitor.visit(union.children(childIndex))
              .attributeStats
            attrStats.get(attr).isDefined && attrStats(attr).hasMinMaxStats
        }
    }
    attrToComputeMinMaxStats.map {
      case (attrs, outputIndex) =>
        val dataType = unionOutput(outputIndex).dataType
        val statComparator = createStatComparator(dataType)
        val minMaxValue = attrs.zipWithIndex.foldLeft[(Option[Any], Option[Any])]((None, None)) {
          case ((minVal, maxVal), (attr, childIndex)) =>
            val colStat = GpuStatsPlanVisitor.visit(union.children(childIndex))
              .attributeStats(attr)
            val min = if (minVal.isEmpty || statComparator(colStat.min.get, minVal.get)) {
              colStat.min
            } else {
              minVal
            }
            val max = if (maxVal.isEmpty || statComparator(maxVal.get, colStat.max.get)) {
              colStat.max
            } else {
              maxVal
            }
            (min, max)
        }
        val newStat = ColumnStat(min = minMaxValue._1, max = minMaxValue._2)
        unionOutput(outputIndex) -> newStat
    }
  }

  private def computeNullCountStats(union: Union): Seq[(Attribute, ColumnStat)] = {
    val unionOutput = union.output
    val attrToComputeNullCount = union.children.map(_.output).transpose.zipWithIndex.filter {
      case (attrs, _) => attrs.zipWithIndex.forall {
        case (attr, childIndex) =>
          val attrStats = GpuStatsPlanVisitor.visit(union.children(childIndex))
            .attributeStats
          attrStats.get(attr).isDefined && attrStats(attr).nullCount.isDefined
      }
    }
    attrToComputeNullCount.map {
      case (attrs, outputIndex) =>
        val firstStat = GpuStatsPlanVisitor.visit(union.children.head)
          .attributeStats(attrs.head)
        val firstNullCount = firstStat.nullCount.get
        val colWithNullStatValues = attrs.zipWithIndex.tail.foldLeft[BigInt](firstNullCount) {
          case (totalNullCount, (attr, childIndex)) =>
            val colStat = GpuStatsPlanVisitor.visit(union.children(childIndex))
              .attributeStats(attr)
            totalNullCount + colStat.nullCount.get
        }
        val newStat = ColumnStat(nullCount = Some(colWithNullStatValues))
        unionOutput(outputIndex) -> newStat
    }
  }
}
