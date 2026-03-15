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
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat => OldFileInputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{
  FileInputFormat => NewFileInputFormat}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD, RDD}
import org.apache.spark.sql.execution.{ExternalRDDScanExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.rapids.GpuSequenceFileSerializeFromObjectExec
import org.apache.spark.sql.types.BinaryType

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

  override def tagPlanForGpu(): Unit = {
    if (!conf.isSequenceFileRDDPhysicalReplaceEnabled) {
      willNotWorkOnGpu("SequenceFile RDD physical replacement is disabled")
      return
    }
    val outOk = wrapped.output.nonEmpty && wrapped.output.forall { a =>
      val isKeyOrValue = a.name.equalsIgnoreCase("key") ||
        a.name.equalsIgnoreCase("value")
      isKeyOrValue && a.dataType == BinaryType
    }
    if (!outOk) {
      willNotWorkOnGpu("SequenceFile object replacement only supports BinaryType key/value output")
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
    GpuSequenceFileSerializeFromObjectExec(
      wrapped.output,
      wrapped.child,
      TargetSize(conf.gpuTargetBatchSizeBytes),
      analysis.inputPaths)(conf)
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
  private val SequenceFileBlockCompressionVersion = 5

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
      // Spark's Scala bytecode exposes inputFormatClass as a public field with the mangled
      // name below (verified with javap on Spark 3.5.1), so getField is intentional here.
      // This is not a JavaBean-style accessor method; using getMethod would look for a
      // zero-arg method that does not exist on the compiled class.
      val f = classOf[NewHadoopRDD[_, _]]
        .getField("org$apache$spark$rdd$NewHadoopRDD$$inputFormatClass")
      val ifc = f.get(rdd).asInstanceOf[Class[_]]
      ifc != null && ifc.getName.contains("SequenceFile")
    } catch {
      case NonFatal(e) =>
        logDebug(s"Failed to inspect NewHadoopRDD input format via reflection: ${e.getMessage}", e)
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

  private def findAnyFile(path: Path, conf: org.apache.hadoop.conf.Configuration): Option[Path] = {
    val fs = path.getFileSystem(conf)
    val statuses = fs.globStatus(path)
    if (statuses == null || statuses.isEmpty) {
      None
    } else {
      val first = statuses.head
      if (first.isFile) Some(first.getPath)
      else {
        val it = fs.listFiles(first.getPath, true)
        if (it.hasNext) Some(it.next().getPath) else None
      }
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

  private def hasCompressedInput(
      inputPaths: Seq[String],
      conf: org.apache.hadoop.conf.Configuration): Boolean = {
    inputPaths.exists { p =>
      try {
        findAnyFile(new Path(p), conf).exists(f => isCompressedSequenceFile(f, conf))
      } catch {
        case NonFatal(_) => false
      }
    }
  }
}

