/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids

import scala.io.Source

import com.nvidia.spark.rapids.{ConfEntry, ConfEntryWithDefault, OptionalConfEntry}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.internal.config.ConfigEntry

object RapidsPrivateUtil {
  def getPrivateConfigs(): Seq[ConfEntry[_]] = {
    withResource(Source.fromResource("spark-rapids-extra-configs-classes").bufferedReader()) { r =>
      val className = r.readLine().trim
      Class.forName(className)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[Iterable[ConfigEntry[_]]]
        .map(convert).toSeq
    }
  }

  /** Convert Spark ConfigEntry to Spark RAPIDS ConfEntry */
  private def convert(e: ConfigEntry[_]): ConfEntry[_] = {
    e.defaultValue match {
      case None => createEntry[String](e.key, e.doc, _.toString)
      case Some(value: Boolean) => createEntryWithDefault[Boolean](e.key, e.doc, _.toBoolean, value)
      case Some(value: Integer) => createEntryWithDefault[Integer](e.key, e.doc, _.toInt, value)
      case Some(value: Long) => createEntryWithDefault[Long](e.key, e.doc, _.toLong, value)
      case Some(value: Double) => createEntryWithDefault[Double](e.key, e.doc, _.toDouble, value)
      case Some(value: String) => createEntryWithDefault[String](e.key, e.doc, _.toString, value)
      case Some(other) => throw new IllegalStateException(
        s"Unsupported private config defaultValue type: $other")
    }
  }

  private def createEntryWithDefault[T](
      key: String,
      doc: String,
      converter: String => T,
      value: T) = {
    new ConfEntryWithDefault[T](key, converter, doc, isInternal = false,
      isStartupOnly = false, value)
  }

  private def createEntry[T](
     key: String,
     doc: String,
     converter: String => T) = {
    new OptionalConfEntry[T](key, converter, doc, isInternal = false, isStartupOnly = false)
  }
}
