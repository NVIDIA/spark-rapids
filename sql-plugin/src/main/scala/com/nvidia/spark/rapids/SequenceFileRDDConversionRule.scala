/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat => OldFileInputFormat,
  SequenceFileAsBinaryInputFormat => OldSequenceFileAsBinaryInputFormat,
  SequenceFileInputFormat => OldSequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat,
  SequenceFileInputFormat => NewSequenceFileInputFormat}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SerializeFromObject}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex,
  LogicalRelation}

/**
 * A logical plan rule that converts RDD-based SequenceFile scans to FileFormat-based scans.
 *
 * This rule detects patterns like:
 * {{{
 *   sc.newAPIHadoopFile(path, classOf[SequenceFileAsBinaryInputFormat], ...)
 *     .map { case (k, v) => v.copyBytes() }
 *     .toDF("value")
 * }}}
 *
 * And converts them to FileFormat-based scan that can be GPU-accelerated.
 *
 * IMPORTANT: This conversion is disabled by default because:
 * 1. Compressed SequenceFiles will cause runtime failures (compression can only be detected
 *    by reading file headers at runtime, not at plan time)
 * 2. Complex RDD transformations (e.g., filter, flatMap) between the HadoopRDD and toDF()
 *    cannot be converted
 *
 * Enable via: spark.rapids.sql.sequenceFile.rddConversion.enabled=true
 *
 * If the conversion fails or GPU doesn't support the operation, the original RDD scan
 * will be preserved (no fallback to CPU FileFormat).
 */
case class SequenceFileRDDConversionRule(spark: SparkSession) extends Rule[LogicalPlan]
    with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Read config fresh each time to ensure we get the latest value
    val rapidsConf = new RapidsConf(spark.sessionState.conf)
    if (!rapidsConf.isSequenceFileRDDConversionEnabled) {
      return plan
    }

    plan.transformDown {
      case s: SerializeFromObject =>
        s.child match {
          case externalRdd: ExternalRDD[_] =>
            tryConvertSequenceFileRDD(s, externalRdd).getOrElse(s)
          case _ => s
        }
    }
  }

  /**
   * Attempts to convert an ExternalRDD-based SequenceFile scan to a FileFormat-based scan.
   * Returns None if the conversion is not applicable or fails.
   */
  private def tryConvertSequenceFileRDD(
      original: SerializeFromObject,
      externalRdd: ExternalRDD[_]): Option[LogicalPlan] = {
    try {
      val rdd = externalRdd.rdd

      // Determine the expected schema by looking at the original SerializeFromObject output
      // If it has 2 fields (key, value), use full schema; if 1 field, use value-only schema
      val numOutputFields = original.output.size
      val isValueOnly = numOutputFields == 1

      // Find the HadoopRDD at the root of the RDD lineage
      findSequenceFileRDDInfo(rdd) match {
        case Some(SequenceFileRDDInfo(paths, _)) =>
          logDebug(s"Found SequenceFile RDD with paths: ${paths.mkString(", ")}, " +
            s"valueOnly: $isValueOnly")

          // Determine the schema based on what the user is selecting
          val dataSchema = if (isValueOnly) {
            SequenceFileBinaryFileFormat.valueOnlySchema
          } else {
            SequenceFileBinaryFileFormat.dataSchema
          }

          // Expand glob patterns in paths before creating FileIndex
          // This is necessary because InMemoryFileIndex doesn't expand globs by default
          val expandedPaths = expandGlobPaths(paths)
          if (expandedPaths.isEmpty) {
            logWarning(s"No files found after expanding glob patterns: ${paths.mkString(", ")}")
            return None
          }
          logDebug(s"Expanded ${paths.size} path patterns to ${expandedPaths.size} paths")

          // Create the FileIndex with expanded paths
          val fileIndex = new InMemoryFileIndex(
            spark,
            expandedPaths,
            Map.empty[String, String],
            None,
            NoopCache)

          // Create the HadoopFsRelation with our internal FileFormat
          val relation = HadoopFsRelation(
            location = fileIndex,
            partitionSchema = org.apache.spark.sql.types.StructType(Nil),
            dataSchema = dataSchema,
            bucketSpec = None,
            fileFormat = new SequenceFileBinaryFileFormat,
            options = Map.empty)(spark)

          // Create LogicalRelation
          val logicalRelation = LogicalRelation(relation, isStreaming = false)

          logInfo(s"Successfully converted SequenceFile RDD scan to FileFormat scan: " +
            s"paths=${paths.mkString(",")}, schema=$dataSchema")

          Some(logicalRelation)

        case None =>
          logDebug(s"RDD lineage does not contain SequenceFile RDD, skipping conversion")
          None
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to convert SequenceFile RDD to FileFormat: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Information about a SequenceFile RDD
   * @param paths The input paths
   * @param isValueOnly Whether the RDD only contains values (not key-value pairs)
   */
  private case class SequenceFileRDDInfo(
      paths: Seq[String],
      isValueOnly: Boolean)

  /**
   * Traverses the RDD lineage to find a SequenceFile HadoopRDD/NewHadoopRDD.
   * Returns None if no SequenceFile RDD is found or if the transformation is too complex.
   */
  private def findSequenceFileRDDInfo(rdd: RDD[_]): Option[SequenceFileRDDInfo] = {
    rdd match {
      // NewHadoopRDD (new API: org.apache.hadoop.mapreduce)
      case newHadoop: NewHadoopRDD[_, _] =>
        if (isNewApiSequenceFileRDD(newHadoop)) {
          extractPathsFromNewHadoopRDD(newHadoop).map { paths =>
            SequenceFileRDDInfo(paths, isValueOnly = false)
          }
        } else {
          None
        }

      // HadoopRDD (old API: org.apache.hadoop.mapred)
      case hadoop: HadoopRDD[_, _] =>
        if (isOldApiSequenceFileRDD(hadoop)) {
          extractPathsFromHadoopRDD(hadoop).map { paths =>
            SequenceFileRDDInfo(paths, isValueOnly = false)
          }
        } else {
          None
        }

      case _ =>
        // For other RDD types (like MapPartitionsRDD), traverse the lineage
        if (rdd.dependencies.isEmpty) {
          None
        } else {
          findSequenceFileRDDInfo(rdd.dependencies.head.rdd).map { info =>
            info.copy(isValueOnly = true)
          }
        }
    }
  }

  /**
   * Check if a NewHadoopRDD uses SequenceFile input format using reflection.
   */
  private def isNewApiSequenceFileRDD(rdd: NewHadoopRDD[_, _]): Boolean = {
    try {
      getInputFormatClass(rdd) match {
        case Some(cls) =>
          classOf[NewSequenceFileInputFormat[_, _]].isAssignableFrom(cls) ||
            cls.getName.contains("SequenceFileAsBinaryInputFormat")
        case None => false
      }
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to check NewHadoopRDD input format: ${e.getMessage}")
        false
    }
  }

  /**
   * Get the input format class from a NewHadoopRDD using reflection.
   * Handles Scala name mangling for private fields.
   */
  private def getInputFormatClass(rdd: NewHadoopRDD[_, _]): Option[Class[_]] = {
    val clazz = classOf[NewHadoopRDD[_, _]]

    // Find fields containing "inputFormatClass" (handles Scala name mangling)
    val inputFormatFields = clazz.getDeclaredFields.filter(_.getName.contains("inputFormatClass"))

    for (field <- inputFormatFields) {
      try {
        field.setAccessible(true)
        val value = field.get(rdd)

        if (value != null) {
          val formatClass: Option[Class[_]] = value match {
            case c: Class[_] => Some(c)
            case other =>
              // Try to unwrap from wrapper types
              try {
                val valueField = other.getClass.getDeclaredField("value")
                valueField.setAccessible(true)
                valueField.get(other) match {
                  case c: Class[_] => Some(c)
                  case _ => None
                }
              } catch {
                case _: Exception => None
              }
          }
          if (formatClass.isDefined) {
            return formatClass
          }
        }
      } catch {
        case NonFatal(_) => // Continue to next field
      }
    }
    None
  }

  /**
   * Check if a HadoopRDD uses SequenceFile input format using reflection.
   * Supports both SequenceFileInputFormat and SequenceFileAsBinaryInputFormat (old API).
   */
  private def isOldApiSequenceFileRDD(rdd: HadoopRDD[_, _]): Boolean = {
    try {
      // First, try to get the input format class from JobConf
      val jobConfOpt = tryGetJobConfViaMethod(rdd)
      jobConfOpt match {
        case Some(jobConf) =>
          val inputFormatClassName = jobConf.get("mapred.input.format.class")
          if (inputFormatClassName != null && inputFormatClassName.contains("SequenceFile")) {
            return true
          }
        case None =>
      }

      // Fall back to checking fields - use actual runtime class, not just HadoopRDD
      val clazz = rdd.getClass
      val allFields = clazz.getDeclaredFields ++ classOf[HadoopRDD[_, _]].getDeclaredFields

      for (field <- allFields) {
        try {
          field.setAccessible(true)
          val fieldValue = field.get(rdd)
          
          // Try to extract Class from the field value
          val formatClass = extractClass(fieldValue)
          
          if (formatClass != null) {
            if (classOf[OldSequenceFileInputFormat[_, _]].isAssignableFrom(formatClass) ||
                classOf[OldSequenceFileAsBinaryInputFormat].isAssignableFrom(formatClass) ||
                formatClass.getName.contains("SequenceFile")) {
              return true
            }
          }
          
          // Also check if the field itself is a class with SequenceFile in the name
          if (fieldValue != null && fieldValue.isInstanceOf[Class[_]]) {
            val cls = fieldValue.asInstanceOf[Class[_]]
            if (cls.getName.contains("SequenceFile")) {
              return true
            }
          }
        } catch {
          case NonFatal(_) => // Continue to next field
        }
      }
      false
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to check HadoopRDD input format: ${e.getMessage}")
        false
    }
  }

  /**
   * Try to extract a Class from various wrapper types.
   */
  private def extractClass(value: Any): Class[_] = {
    if (value == null) return null
    
    value match {
      case c: Class[_] => c
      case _ =>
        // Try to get 'value' field or method (for wrapper types)
        try {
          val valueMethod = value.getClass.getMethod("value")
          valueMethod.invoke(value) match {
            case c: Class[_] => c
            case _ => null
          }
        } catch {
          case _: Exception =>
            try {
              val valueField = value.getClass.getDeclaredField("value")
              valueField.setAccessible(true)
              valueField.get(value) match {
                case c: Class[_] => c
                case _ => null
              }
            } catch {
              case _: Exception => null
            }
        }
    }
  }

  /**
   * Extract input paths from a NewHadoopRDD using reflection.
   */
  private def extractPathsFromNewHadoopRDD(rdd: NewHadoopRDD[_, _]): Option[Seq[String]] = {
    try {
      val clazz = classOf[NewHadoopRDD[_, _]]
      val confFields = clazz.getDeclaredFields.filter(f =>
        f.getName == "_conf" || f.getName.contains("_conf"))

      for (confField <- confFields) {
        try {
          confField.setAccessible(true)
          val confValue = confField.get(rdd)

          // Handle SerializableConfiguration wrapper
          val conf = confValue match {
            case c: org.apache.hadoop.conf.Configuration => c
            case other =>
              try {
                val valueField = other.getClass.getDeclaredField("value")
                valueField.setAccessible(true)
                valueField.get(other).asInstanceOf[org.apache.hadoop.conf.Configuration]
              } catch {
                case _: Exception => null
              }
          }

          if (conf != null) {
            val pathsStr = conf.get(NewFileInputFormat.INPUT_DIR)
            if (pathsStr != null && pathsStr.nonEmpty) {
              return Some(pathsStr.split(",").map(_.trim).toSeq)
            }
          }
        } catch {
          case NonFatal(_) => // Continue to next field
        }
      }

      // Fall back to RDD name
      Option(rdd.name).filter(_.nonEmpty).map(Seq(_))
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to extract paths from NewHadoopRDD: ${e.getMessage}")
        Option(rdd.name).filter(_.nonEmpty).map(Seq(_))
    }
  }

  /**
   * Extract input paths from a HadoopRDD using reflection.
   * The paths are stored in the JobConf within the HadoopRDD.
   *
   * HadoopRDD stores the JobConf in different fields depending on Spark version:
   * - `_broadcastedConf` (Broadcast[SerializableConfiguration])
   * - `jobConfCacheKey` or similar fields
   */
  private def extractPathsFromHadoopRDD(rdd: HadoopRDD[_, _]): Option[Seq[String]] = {
    try {
      // First, try to use HadoopRDD's getJobConf method if available
      val jobConfOpt = tryGetJobConfViaMethod(rdd)
      
      jobConfOpt match {
        case Some(jobConf) =>
          val inputPaths = OldFileInputFormat.getInputPaths(jobConf)
          if (inputPaths != null && inputPaths.nonEmpty) {
            return Some(inputPaths.map(_.toString).toSeq)
          }
        case None =>
      }

      // Fall back to field access - try all fields from actual class and HadoopRDD
      val clazz = rdd.getClass
      val allFields = clazz.getDeclaredFields ++ classOf[HadoopRDD[_, _]].getDeclaredFields

      for (field <- allFields) {
        try {
          field.setAccessible(true)
          val fieldValue = field.get(rdd)
          
          // Try to extract JobConf from the field value
          val jobConf = extractJobConf(fieldValue)
          
          if (jobConf != null) {
            // Get input paths from the old API FileInputFormat
            val inputPaths = OldFileInputFormat.getInputPaths(jobConf)
            if (inputPaths != null && inputPaths.nonEmpty) {
              return Some(inputPaths.map(_.toString).toSeq)
            } else {
              // Try getting paths from configuration string directly
              val pathStr = jobConf.get("mapreduce.input.fileinputformat.inputdir")
              if (pathStr != null && pathStr.nonEmpty) {
                return Some(pathStr.split(",").map(_.trim).toSeq)
              }
              val oldPathStr = jobConf.get("mapred.input.dir")
              if (oldPathStr != null && oldPathStr.nonEmpty) {
                return Some(oldPathStr.split(",").map(_.trim).toSeq)
              }
            }
          }
        } catch {
          case NonFatal(_) => // Continue to next field
        }
      }

      // Fall back to RDD name
      Option(rdd.name).filter(_.nonEmpty).map(Seq(_))
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to extract paths from HadoopRDD: ${e.getMessage}")
        Option(rdd.name).filter(_.nonEmpty).map(Seq(_))
    }
  }

  /**
   * Try to get JobConf via HadoopRDD's getJobConf method (available in some Spark versions).
   */
  private def tryGetJobConfViaMethod(rdd: HadoopRDD[_, _]): 
      Option[org.apache.hadoop.mapred.JobConf] = {
    try {
      val method = rdd.getClass.getMethod("getJobConf")
      method.invoke(rdd) match {
        case jc: org.apache.hadoop.mapred.JobConf => Some(jc)
        case _ => None
      }
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Try to extract a JobConf from various wrapper types.
   */
  private def extractJobConf(value: Any): org.apache.hadoop.mapred.JobConf = {
    if (value == null) return null
    
    value match {
      case jc: org.apache.hadoop.mapred.JobConf => jc
      case conf: org.apache.hadoop.conf.Configuration =>
        // Configuration might contain the paths we need
        new org.apache.hadoop.mapred.JobConf(conf)
      case _ =>
        // Handle Broadcast[SerializableConfiguration] or similar wrappers
        try {
          // Try Broadcast.value() method
          val valueMethod = try {
            value.getClass.getMethod("value")
          } catch {
            case _: NoSuchMethodException => null
          }
          
          if (valueMethod != null) {
            val innerValue = valueMethod.invoke(value)
            return extractJobConf(innerValue)
          }
          
          // Try SerializableConfiguration wrapper
          val valueField = try {
            value.getClass.getDeclaredField("value")
          } catch {
            case _: NoSuchFieldException => null
          }
          
          if (valueField != null) {
            valueField.setAccessible(true)
            val innerValue = valueField.get(value)
            return extractJobConf(innerValue)
          }
          
          // Try 't' field (SerializableWritable stores value in 't')
          val tField = try {
            value.getClass.getDeclaredField("t")
          } catch {
            case _: NoSuchFieldException => null
          }
          
          if (tField != null) {
            tField.setAccessible(true)
            val innerValue = tField.get(value)
            return extractJobConf(innerValue)
          }
          
          null
        } catch {
          case NonFatal(_) => null
        }
    }
  }

  /**
   * Expands glob patterns in paths using Hadoop FileSystem.
   * For example, a path like /data/2024/asterisk expands to matching directories.
   * Non-glob paths are returned as-is if they exist.
   */
  private def expandGlobPaths(paths: Seq[String]): Seq[Path] = {
    val hadoopConf = spark.sessionState.newHadoopConf()

    paths.flatMap { pathStr =>
      val path = new Path(pathStr)
      try {
        val fs = path.getFileSystem(hadoopConf)

        // Check if the path contains glob pattern characters
        val hasGlob = pathStr.contains("*") || pathStr.contains("?") ||
          pathStr.contains("[") || pathStr.contains("{")

        if (hasGlob) {
          // Expand glob pattern
          val globStatus = fs.globStatus(path)
          if (globStatus != null && globStatus.nonEmpty) {
            logDebug(s"Glob pattern '$pathStr' expanded to ${globStatus.length} paths")
            globStatus.map(_.getPath)
          } else {
            logWarning(s"Glob pattern '$pathStr' matched no files")
            Seq.empty
          }
        } else {
          // Not a glob pattern - check if path exists
          if (fs.exists(path)) {
            Seq(path)
          } else {
            logWarning(s"Path does not exist: $pathStr")
            Seq.empty
          }
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to expand glob path '$pathStr': ${e.getMessage}")
          // Return original path as fallback, let InMemoryFileIndex handle the error
          Seq(path)
      }
    }
  }
}

/**
 * A no-op file status cache for InMemoryFileIndex
 */
object NoopCache extends org.apache.spark.sql.execution.datasources.FileStatusCache {
  override def getLeafFiles(path: Path): Option[Array[org.apache.hadoop.fs.FileStatus]] = None
  override def putLeafFiles(path: Path, files: Array[org.apache.hadoop.fs.FileStatus]): Unit = {}
  override def invalidateAll(): Unit = {}
}
