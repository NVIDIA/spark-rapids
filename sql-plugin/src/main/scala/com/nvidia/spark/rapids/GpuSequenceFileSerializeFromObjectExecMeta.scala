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
import org.apache.hadoop.mapred.{FileInputFormat => OldFileInputFormat}
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

  // Override childExprs to empty: we replace the entire SerializeFromObjectExec including its
  // serializer expressions, so we don't need them to be individually GPU-compatible.
  // Without this, the framework's canExprTreeBeReplaced check rejects us because the
  // serializer contains object-related expressions (Invoke, StaticInvoke, etc.) that are
  // not registered as GPU expressions.
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  // Similarly, the child ExternalRDDScanExec is not a registered GPU exec, so we skip
  // wrapping child plans to avoid "not all children can be replaced" cascading failures.
  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Seq.empty

  private var sourceScan: ExternalRDDScanExec[_] = null

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
        sourceScan = e
      case _ =>
        willNotWorkOnGpu("SerializeFromObject child is not ExternalRDDScanExec")
        return
    }
    if (!GpuSequenceFileSerializeFromObjectExecMeta.isSimpleSequenceFileRDD(sourceScan.rdd)) {
      willNotWorkOnGpu("RDD lineage is not a simple SequenceFile scan")
      return
    }
    if (GpuSequenceFileSerializeFromObjectExecMeta.hasCompressedInput(
        sourceScan.rdd, sourceScan.rdd.context.hadoopConfiguration)) {
      willNotWorkOnGpu("Compressed SequenceFile input falls back to CPU")
    }
  }

  override def convertToGpu(): GpuExec = {
    val paths = GpuSequenceFileSerializeFromObjectExecMeta
      .collectInputPaths(sourceScan.rdd)
    GpuSequenceFileSerializeFromObjectExec(
      wrapped.output,
      wrapped.child,
      TargetSize(conf.gpuTargetBatchSizeBytes),
      paths)(conf)
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
  private def isNewApiSequenceFileRDD(rdd: NewHadoopRDD[_, _]): Boolean = {
    try {
      val cls = classOf[NewHadoopRDD[_, _]]
      cls.getDeclaredFields.filter(_.getName.contains("inputFormatClass")).exists { f =>
        f.setAccessible(true)
        val v = f.get(rdd)
        val c = v match {
          case c: Class[_] => c
          case other =>
            try {
              val vf = other.getClass.getDeclaredField("value")
              vf.setAccessible(true)
              vf.get(other).asInstanceOf[Class[_]]
            } catch {
              case NonFatal(_) => null
            }
        }
        c != null && c.getName.contains("SequenceFile")
      }
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
      rdd: RDD[_],
      seen: Set[Int] = Set.empty): Boolean = {
    val id = System.identityHashCode(rdd)
    if (seen.contains(id)) return false
    rdd match {
      case n: NewHadoopRDD[_, _] => isNewApiSequenceFileRDD(n)
      case h: HadoopRDD[_, _] => isOldApiSequenceFileRDD(h)
      case other =>
        if (other.dependencies.size != 1) false
        else isSimpleSequenceFileRDD(
          other.dependencies.head.rdd, seen + id)
    }
  }

  private[rapids] def collectInputPaths(rdd: RDD[_]): Seq[String] = {
    rdd match {
      case n: NewHadoopRDD[_, _] =>
        try {
          val cls = classOf[NewHadoopRDD[_, _]]
          cls.getDeclaredFields
            .filter(f => f.getName == "_conf" || f.getName.contains("_conf"))
            .flatMap { f =>
              f.setAccessible(true)
              val cv = f.get(n)
              val conf = cv match {
                case c: org.apache.hadoop.conf.Configuration => c
                case other =>
                  try {
                    val vf = other.getClass.getDeclaredField("value")
                    vf.setAccessible(true)
                    vf.get(other).asInstanceOf[org.apache.hadoop.conf.Configuration]
                  } catch {
                    case NonFatal(_) => null
                  }
              }
              val p = if (conf != null) conf.get(NewFileInputFormat.INPUT_DIR) else null
              Option(p).toSeq
            }.flatMap(_.split(",").map(_.trim)).filter(_.nonEmpty)
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
        org.apache.hadoop.io.Text.readString(in)
        org.apache.hadoop.io.Text.readString(in)
        val isCompressed = in.readBoolean()
        val isBlockCompressed = in.readBoolean()
        isCompressed || isBlockCompressed
      }
    } catch {
      case NonFatal(_) => false
    } finally {
      if (in != null) in.close()
    }
  }

  def hasCompressedInput(rdd: RDD[_], conf: org.apache.hadoop.conf.Configuration): Boolean = {
    collectInputPaths(rdd).exists { p =>
      try {
        findAnyFile(new Path(p), conf).exists(f => isCompressedSequenceFile(f, conf))
      } catch {
        case NonFatal(_) => false
      }
    }
  }
}

