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

  private lazy val extraConfigs = 
    getPrivateConfigs("spark-rapids-extra-configs-classes", isStartup = false)
  private lazy val extraStartupConfigs = 
    getPrivateConfigs("spark-rapids-extra-startup-configs-classes", isStartup = true)

  def getPrivateConfigs(): Seq[ConfEntry[_]] = {
    extraConfigs ++ extraStartupConfigs
  }

  private def getPrivateConfigs(resourceName: String, isStartup: Boolean): Seq[ConfEntry[_]] = {
    withResource(Source.fromResource(resourceName).bufferedReader()) { r =>
      val className = r.readLine().trim
      Class.forName(className)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[Iterable[ConfigEntry[_]]]
        .map(c => convert(c, isStartup)).toSeq
    }
  }

  /** Convert Spark ConfigEntry to Spark RAPIDS ConfEntry */
  private def convert(e: ConfigEntry[_], isStartup: Boolean): ConfEntry[_] = {
    e.defaultValue match {
      case None => createEntry[String](e.key, e.doc, _.toString, isStartup)
      case Some(value: Boolean) =>
        createEntryWithDefault[Boolean](e.key, e.doc, _.toBoolean, value, isStartup)
      case Some(value: Integer) =>
        createEntryWithDefault[Integer](e.key, e.doc, _.toInt, value, isStartup)
      case Some(value: Long) =>
        createEntryWithDefault[Long](e.key, e.doc, _.toLong, value, isStartup)
      case Some(value: Double) =>
        createEntryWithDefault[Double](e.key, e.doc, _.toDouble, value, isStartup)
      case Some(value: String) =>
        createEntryWithDefault[String](e.key, e.doc, _.toString, value, isStartup)
      case Some(other) => throw new IllegalStateException(
        s"Unsupported private config defaultValue type: $other")
    }
  }

  private def createEntryWithDefault[T](
      key: String,
      doc: String,
      converter: String => T,
      value: T,
      isStartup: Boolean) = {
    new ConfEntryWithDefault[T](key, converter, doc, isInternal = false,
      isStartupOnly = isStartup, defaultValue = value)
  }

  private def createEntry[T](
      key: String,
      doc: String,
      converter: String => T,
      isStartup: Boolean) = {
    new OptionalConfEntry[T](key, converter, doc, isInternal = false, isStartupOnly = isStartup)
  }
}
