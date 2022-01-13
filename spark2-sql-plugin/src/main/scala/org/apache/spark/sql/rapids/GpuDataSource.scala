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

package org.apache.spark.sql.rapids

import java.util.{Locale, ServiceConfigurationError, ServiceLoader}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.{RateStreamProvider, TextSocketSourceProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.util.Utils

object GpuDataSource extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val jdbc = classOf[JdbcRelationProvider].getCanonicalName
    val json = classOf[JsonFileFormat].getCanonicalName
    val parquet = classOf[ParquetFileFormat].getCanonicalName
    val csv = classOf[CSVFileFormat].getCanonicalName
    val libsvm = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"
    val orc = "org.apache.spark.sql.hive.orc.OrcFileFormat"
    val nativeOrc = classOf[OrcFileFormat].getCanonicalName
    val socket = classOf[TextSocketSourceProvider].getCanonicalName
    val rate = classOf[RateStreamProvider].getCanonicalName

    Map(
      "org.apache.spark.sql.jdbc" -> jdbc,
      "org.apache.spark.sql.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc" -> jdbc,
      "org.apache.spark.sql.json" -> json,
      "org.apache.spark.sql.json.DefaultSource" -> json,
      "org.apache.spark.sql.execution.datasources.json" -> json,
      "org.apache.spark.sql.execution.datasources.json.DefaultSource" -> json,
      "org.apache.spark.sql.parquet" -> parquet,
      "org.apache.spark.sql.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.hive.orc.DefaultSource" -> orc,
      "org.apache.spark.sql.hive.orc" -> orc,
      "org.apache.spark.sql.execution.datasources.orc.DefaultSource" -> nativeOrc,
      "org.apache.spark.sql.execution.datasources.orc" -> nativeOrc,
      "org.apache.spark.ml.source.libsvm.DefaultSource" -> libsvm,
      "org.apache.spark.ml.source.libsvm" -> libsvm,
      "com.databricks.spark.csv" -> csv,
      "org.apache.spark.sql.execution.streaming.TextSocketSourceProvider" -> socket,
      "org.apache.spark.sql.execution.streaming.RateSourceProvider" -> rate
    )
  }

  /**
   * Class that were removed in Spark 2.0. Used to detect incompatibility libraries for Spark 2.0.
   */
  private val spark2RemovedClasses = Set(
    "org.apache.spark.sql.DataFrame",
    "org.apache.spark.sql.sources.HadoopFsRelationProvider",
    "org.apache.spark.Logging")


  /** Given a provider name, look up the data source class definition. */
  def lookupDataSource(provider: String, conf: SQLConf): Class[_] = {
    val provider1 = backwardCompatibilityMap.getOrElse(provider, provider) match {
      case name if name.equalsIgnoreCase("orc") &&
          conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "native" =>
        classOf[OrcFileFormat].getCanonicalName
      case name if name.equalsIgnoreCase("orc") &&
          conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "hive" =>
        "org.apache.spark.sql.hive.orc.OrcFileFormat"
      case "com.databricks.spark.avro" if conf.replaceDatabricksSparkAvroEnabled =>
        "org.apache.spark.sql.avro.AvroFileFormat"
      case name => name
    }
    val provider2 = s"$provider1.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    try {
      serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider1)).toList match {
        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider2))) match {
              case Success(dataSource) =>
                // Found the data source using fully qualified path
                dataSource
              case Failure(error) =>
                if (provider1.startsWith("org.apache.spark.sql.hive.orc")) {
                  throw new AnalysisException(
                    "Hive built-in ORC data source must be used with Hive support enabled. " +
                    "Please use the native ORC data source by setting 'spark.sql.orc.impl' to " +
                    "'native'")
                } else if (provider1.toLowerCase(Locale.ROOT) == "avro" ||
                  provider1 == "com.databricks.spark.avro" ||
                  provider1 == "org.apache.spark.sql.avro") {
                  throw new AnalysisException(
                    s"Failed to find data source: $provider1. Avro is built-in but external data " +
                    "source module since Spark 2.4. Please deploy the application as per " +
                    "the deployment section of \"Apache Avro Data Source Guide\".")
                } else if (provider1.toLowerCase(Locale.ROOT) == "kafka") {
                  throw new AnalysisException(
                    s"Failed to find data source: $provider1. Please deploy the application as " +
                    "per the deployment section of " +
                    "\"Structured Streaming + Kafka Integration Guide\".")
                } else {
                  throw new ClassNotFoundException(
                    s"Failed to find data source: $provider1. Please find packages at " +
                      "http://spark.apache.org/third-party-projects.html",
                    error)
                }
            }
          } catch {
            case e: NoClassDefFoundError => // This one won't be caught by Scala NonFatal
              // NoClassDefFoundError's class name uses "/" rather than "." for packages
              val className = e.getMessage.replaceAll("/", ".")
              if (spark2RemovedClasses.contains(className)) {
                throw new ClassNotFoundException(s"$className was removed in Spark 2.0. " +
                  "Please check if your library is compatible with Spark 2.0", e)
              } else {
                throw e
              }
          }
        case head :: Nil =>
          // there is exactly one registered alias
          head.getClass
        case sources =>
          // There are multiple registered aliases for the input. If there is single datasource
          // that has "org.apache.spark" package in the prefix, we use it considering it is an
          // internal datasource within Spark.
          val sourceNames = sources.map(_.getClass.getName)
          val internalSources = sources.filter(_.getClass.getName.startsWith("org.apache.spark"))
          if (internalSources.size == 1) {
            logWarning(s"Multiple sources found for $provider1 (${sourceNames.mkString(", ")}), " +
              s"defaulting to the internal datasource (${internalSources.head.getClass.getName}).")
            internalSources.head.getClass
          } else {
            throw new AnalysisException(s"Multiple sources found for $provider1 " +
              s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
          }
      }
    } catch {
      case e: ServiceConfigurationError if e.getCause.isInstanceOf[NoClassDefFoundError] =>
        // NoClassDefFoundError's class name uses "/" rather than "." for packages
        val className = e.getCause.getMessage.replaceAll("/", ".")
        if (spark2RemovedClasses.contains(className)) {
          throw new ClassNotFoundException(s"Detected an incompatible DataSourceRegister. " +
            "Please remove the incompatible library from classpath or upgrade it. " +
            s"Error: ${e.getMessage}", e)
        } else {
          throw e
        }
    }
  }
}
