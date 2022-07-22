/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.types.rapids

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{AbstractDataType, AtomicType, DataType, TimestampType}

// Copied from org/apache/spark/sql/types/AbstractDataType.scala
// for for https://github.com/NVIDIA/spark-rapids/issues/6026
// It can be removed when Spark 3.3.0 is the least supported Spark version
private[sql] object AnyTimestampType extends AbstractDataType with Serializable {
  override private[sql] def defaultConcreteType: DataType = TimestampType

  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[TimestampType] || other.isInstanceOf[TimestampNTZType]

  override private[sql] def simpleString = "(timestamp or timestamp without time zone)"

  def unapply(e: Expression): Boolean = acceptsType(e.dataType)
}

private[sql] abstract class DatetimeType extends AtomicType

/**
 * The interval type which conforms to the ANSI SQL standard.
 */
private[sql] abstract class AnsiIntervalType extends AtomicType
