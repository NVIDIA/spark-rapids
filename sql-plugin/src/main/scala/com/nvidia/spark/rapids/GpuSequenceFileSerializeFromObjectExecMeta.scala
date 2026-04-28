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

import scala.annotation.tailrec
import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat => OldFileInputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{
  FileInputFormat => NewFileInputFormat}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD, RDD}
import org.apache.spark.sql.execution.{ExternalRDDScanExec, ProjectExec,
  SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.rapids.GpuSequenceFileSerializeFromObjectExec
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

class GpuSequenceFileSerializeFromObjectExecMeta(
    plan: SerializeFromObjectExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SerializeFromObjectExec](plan, conf, parent, rule) with Logging {

  import GpuSequenceFileSerializeFromObjectExecMeta._

  // Override childExprs to empty: we replace the entire SerializeFromObjectExec including its
  // serializer expressions, so we don't need them to be individually GPU-compatible.
  // Without this, the framework's canExprTreeBeReplaced check rejects us because the
  // serializer contains object-related expressions (Invoke, StaticInvoke, etc.) that are
  // not registered as GPU expressions.
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  // Similarly, the child ExternalRDDScanExec is not a registered GPU exec, so we skip
  // wrapping child plans to avoid "not all children can be replaced" cascading failures.
  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Seq.empty

  private var scanAnalysis: Option[SequenceFileScanAnalysis] =
    None

  // Effective column names after any parent ProjectExec rename.
  // These determine which SequenceFile columns (key/value) the reader produces.
  // For example, rdd.map(kv => keyBytes).toDF("key") has internal name "value"
  // (encoder default) but effective name "key" (from the parent Project rename).
  private var effectiveColumnNames: Seq[String] = Seq.empty

  /**
   * Resolves the effective output column names by checking for a parent ProjectExec
   * that renames columns. Falls back to the internal names if no rename is detected.
   */
  private def resolveEffectiveColumnNames(): Seq[String] = {
    parent match {
      case Some(spm: SparkPlanMeta[_]) =>
        spm.wrapped match {
          case p: ProjectExec if p.output.length == wrapped.output.length =>
            p.output.map(_.name)
          case _ => wrapped.output.map(_.name)
        }
      case _ => wrapped.output.map(_.name)
    }
  }

  override def tagPlanForGpu(): Unit = {
    if (!conf.isSequenceFileRDDPhysicalReplaceEnabled) {
      willNotWorkOnGpu("SequenceFile RDD physical replacement is disabled")
      return
    }
    val typeOk = wrapped.output.nonEmpty &&
      wrapped.output.forall(_.dataType == BinaryType)
    if (!typeOk) {
      willNotWorkOnGpu("SequenceFile object replacement only supports BinaryType output")
      return
    }
    effectiveColumnNames = resolveEffectiveColumnNames()
    // Validate that this is a recognizable SequenceFile output pattern:
    //   (a) Single column: internal name "value"/"key" (Array[Byte] encoder default),
    //       effective name "key"/"value" — handles rdd.map(kv => kv._1).toDF("key").
    //   (b) Two columns: internal names "_1"/"_2" (tuple encoder), effective names
    //       exactly {"key","value"} in any order — handles
    //       rdd.map(kv => (kv._1,kv._2)).toDF("value","key").
    //       Reading is positional: col0=SF key, col1=SF value regardless of effective names.
    //   (c) Fallback: internal names already "key"/"value", effective names "key"/"value".
    val internalNames = wrapped.output.map(_.name)
    val namesOk = internalNames match {
      case Seq(n) if n.equalsIgnoreCase("value") || n.equalsIgnoreCase("key") =>
        effectiveColumnNames.forall(e => e.equalsIgnoreCase("key") || e.equalsIgnoreCase("value"))
      case Seq("_1", "_2") =>
        effectiveColumnNames.map(_.toLowerCase).toSet == Set("key", "value")
      case _ =>
        internalNames.forall(n => n.equalsIgnoreCase("key") || n.equalsIgnoreCase("value")) &&
          effectiveColumnNames.forall(n => n.equalsIgnoreCase("key") || n.equalsIgnoreCase("value"))
    }
    if (!namesOk) {
      willNotWorkOnGpu("Output columns do not match a supported SequenceFile pattern: " +
        s"internal=[${internalNames.mkString(",")}] " +
        s"effective=[${effectiveColumnNames.mkString(",")}]")
      return
    }
    wrapped.child match {
      case e: ExternalRDDScanExec[_] =>
        if (!isSimpleSequenceFileRDD(e.rdd)) {
          willNotWorkOnGpu("RDD lineage is not a simple SequenceFile scan")
          return
        }
        val analysis = analyzeSequenceFileScan(
          e, e.rdd.context.hadoopConfiguration)
        if (analysis.inputPaths.isEmpty) {
          willNotWorkOnGpu("Failed to collect SequenceFile input paths via reflection")
          return
        }
        scanAnalysis = Some(analysis)
        if (analysis.hasCompressedInput) {
          willNotWorkOnGpu("Compressed SequenceFile input falls back to CPU")
        }
      case _ =>
        willNotWorkOnGpu("SerializeFromObject child is not ExternalRDDScanExec")
        return
    }
  }

  override def convertToGpu(): GpuExec = {
    val analysis = scanAnalysis.getOrElse {
      val sourceScan = wrapped.child.asInstanceOf[ExternalRDDScanExec[_]]
      analyzeSequenceFileScan(
        sourceScan, sourceScan.rdd.context.hadoopConfiguration)
    }
    require(analysis.inputPaths.nonEmpty,
      "SequenceFile input paths should be collected before GPU physical replacement")
    // Build the SequenceFile read schema. The field names determine which SF column
    // (key or value) to read; fields are accessed positionally by the reader.
    //
    // For the tuple case (internal names "_1"/"_2"), always use canonical [key, value]
    // order so that col0 = SF key and col1 = SF value regardless of how the user
    // named the output columns via toDF(). The output attribute names (effective names)
    // may differ from the read schema names, but columnar access is positional, so
    // the correct data lands in each output position.
    //
    // For the single-column case, use the effective name directly: it tells us whether
    // to read the SF key or value column.
    val seqFileSchema = if (wrapped.output.map(_.name) == Seq("_1", "_2")) {
      StructType(Seq(
        StructField("key", BinaryType, nullable = true),
        StructField("value", BinaryType, nullable = true)))
    } else {
      StructType(effectiveColumnNames.zip(wrapped.output).map { case (effName, attr) =>
        StructField(effName, attr.dataType, attr.nullable)
      })
    }
    GpuSequenceFileSerializeFromObjectExec(
      wrapped.output,
      wrapped.child,
      TargetSize(conf.gpuTargetBatchSizeBytes),
      analysis.inputPaths,
      seqFileSchema)(conf)
  }

  override def convertToCpu(): SparkPlan = wrapped
}

/**
 * Utilities for identifying simple SequenceFile RDD scans and extracting their input paths.
 *
 * This code uses reflection against `NewHadoopRDD`/`HadoopRDD` internals because the generic RDD
 * lineage API does not expose enough structured metadata for safe physical replacement.
 * Those internal fields/methods may change across Spark/Hadoop versions and can also be
 * restricted by JDK module access settings. Any reflection failure here intentionally falls back
 * to the CPU path by returning conservative defaults.
 */
object GpuSequenceFileSerializeFromObjectExecMeta extends Logging {
  // Hadoop SequenceFile.BLOCK_COMPRESS_VERSION = 4. The isBlockCompressed flag
  // is only present in the header when version >= 4.
  private val SequenceFileBlockCompressionVersion = 4

  private case class SequenceFileScanAnalysis(
      sourceScan: ExternalRDDScanExec[_],
      inputPaths: Seq[String],
      hasCompressedInput: Boolean)

  private def analyzeSequenceFileScan(
      sourceScan: ExternalRDDScanExec[_],
      conf: org.apache.hadoop.conf.Configuration): SequenceFileScanAnalysis = {
    val inputPaths = collectInputPaths(sourceScan.rdd)
    SequenceFileScanAnalysis(
      sourceScan = sourceScan,
      inputPaths = inputPaths,
      hasCompressedInput = hasCompressedInput(inputPaths, conf))
  }

  private def isNewApiSequenceFileRDD(rdd: NewHadoopRDD[_, _]): Boolean = {
    try {
      val clazz = classOf[NewHadoopRDD[_, _]]
      // Try the known field name first (stable across current Spark versions),
      // then fall back to locating the first public Class[_]-typed field.
      // This two-step approach avoids silently matching a wrong field if a future
      // Spark version adds another Class[_] field before inputFormatClass.
      val ifc = try {
        val f = clazz.getField("inputFormatClass")
        f.get(rdd).asInstanceOf[Class[_]]
      } catch {
        case _: NoSuchFieldException =>
          clazz.getFields
            .find(f => classOf[Class[_]].isAssignableFrom(f.getType))
            .map(_.get(rdd).asInstanceOf[Class[_]])
            .orNull
      }
      ifc != null && ifc.getName.contains("SequenceFile")
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to inspect NewHadoopRDD input format: " +
          e.getMessage, e)
        false
    }
  }

  private def isOldApiSequenceFileRDD(rdd: HadoopRDD[_, _]): Boolean = {
    try {
      val m = rdd.getClass.getMethod("getJobConf")
      val jc = m.invoke(rdd).asInstanceOf[org.apache.hadoop.mapred.JobConf]
      val ifc = jc.get("mapred.input.format.class")
      ifc != null && ifc.contains("SequenceFile")
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to inspect HadoopRDD input format via reflection: ${e.getMessage}", e)
        false
    }
  }

  def isSimpleSequenceFileRDD(
      rdd: RDD[_]): Boolean = {
    @tailrec
    def recurse(current: RDD[_], seen: Set[RDD[_]]): Boolean = {
      if (seen.contains(current)) {
        false
      } else {
        current match {
          case n: NewHadoopRDD[_, _] => isNewApiSequenceFileRDD(n)
          case h: HadoopRDD[_, _] => isOldApiSequenceFileRDD(h)
          case other =>
            if (other.dependencies.size != 1) false
            else recurse(other.dependencies.head.rdd, seen + current)
        }
      }
    }

    recurse(rdd, Set.empty)
  }

  @tailrec
  private[rapids] def collectInputPaths(rdd: RDD[_]): Seq[String] = {
    rdd match {
      case n: NewHadoopRDD[_, _] =>
        try {
          val jobConf = n.getConf
          if (jobConf == null) {
            Seq.empty
          } else {
            // Reuse Hadoop's own parsing of INPUT_DIR instead of splitting the raw conf string.
            // This preserves any escaping/normalization semantics that FileInputFormat applies.
            val job = Job.getInstance(jobConf)
            val paths = NewFileInputFormat.getInputPaths(job)
            if (paths == null) Seq.empty else paths.map(_.toString).toSeq
          }
        } catch {
          case NonFatal(e) =>
            logDebug(s"Failed to collect input paths from NewHadoopRDD: ${e.getMessage}", e)
            Seq.empty
        }
      case h: HadoopRDD[_, _] =>
        try {
          val m = h.getClass.getMethod("getJobConf")
          val jc = m.invoke(h).asInstanceOf[org.apache.hadoop.mapred.JobConf]
          val paths = OldFileInputFormat.getInputPaths(jc)
          if (paths == null) Seq.empty else paths.map(_.toString).toSeq
        } catch {
          case NonFatal(e) =>
            logDebug(s"Failed to collect input paths from HadoopRDD: ${e.getMessage}", e)
            Seq.empty
        }
      case other if other.dependencies.size == 1 =>
        collectInputPaths(other.dependencies.head.rdd)
      case _ => Seq.empty
    }
  }

  /** Maximum number of files to sample per input path for compression detection. */
  private val CompressionSampleSize = 5

  /**
   * Returns up to `maxFiles` file paths under the given (possibly globbed) path.
   * Uses a lazy iterator so that callers can short-circuit as soon as a compressed
   * file is found without listing the entire directory tree.
   */
  private def findSampleFiles(
      path: Path,
      conf: org.apache.hadoop.conf.Configuration,
      maxFiles: Int): Iterator[Path] = {
    val fs = path.getFileSystem(conf)
    val statuses = fs.globStatus(path)
    if (statuses == null || statuses.isEmpty) {
      Iterator.empty
    } else {
      statuses.iterator.flatMap { s =>
        if (s.isFile) Iterator.single(s.getPath)
        else {
          val it = fs.listFiles(s.getPath, true)
          new Iterator[Path] {
            override def hasNext: Boolean = it.hasNext
            override def next(): Path = it.next().getPath
          }
        }
      }.take(maxFiles)
    }
  }

  private def isCompressedSequenceFile(
      file: Path,
      conf: org.apache.hadoop.conf.Configuration): Boolean = {
    var in: java.io.DataInputStream = null
    try {
      in = new java.io.DataInputStream(file.getFileSystem(conf).open(file))
      val magic = new Array[Byte](4)
      in.readFully(magic)
      if (!(magic(0) == 'S' && magic(1) == 'E' && magic(2) == 'Q')) {
        false
      } else {
        val version = magic(3) & 0xFF
        org.apache.hadoop.io.Text.readString(in)
        org.apache.hadoop.io.Text.readString(in)
        val isCompressed = in.readBoolean()
        val isBlockCompressed = if (version >= SequenceFileBlockCompressionVersion) {
          in.readBoolean()
        } else {
          false
        }
        isCompressed || isBlockCompressed
      }
    } catch {
      case NonFatal(_) => false
    } finally {
      if (in != null) in.close()
    }
  }

  // Samples up to CompressionSampleSize files per input path for compression detection.
  // If a directory contains a mix of compressed and uncompressed files this may still
  // miss some, but the executor-side ReadBatchRunner will throw
  // UnsupportedSequenceFileCompressionException for any compressed file it encounters.
  private def hasCompressedInput(
      inputPaths: Seq[String],
      conf: org.apache.hadoop.conf.Configuration): Boolean = {
    inputPaths.exists { p =>
      try {
        findSampleFiles(new Path(p), conf, CompressionSampleSize)
          .exists(f => isCompressedSequenceFile(f, conf))
      } catch {
        case NonFatal(_) => false
      }
    }
  }
}

