/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import java.net.URI

import com.nvidia.spark.rapids.lore.{GpuLore, GpuLoreDumpExec}
import com.nvidia.spark.rapids.lore.GpuLore.{loreIdOf, LORE_DUMP_PATH_TAG, LORE_DUMP_RDD_TAG}
import com.nvidia.spark.rapids.shims.{ShimUnaryCommand, ShimUnaryExecNode}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * An extension of `DataWritingCommand` that allows columnar execution.
 */
trait GpuDataWritingCommand extends DataWritingCommand with ShimUnaryCommand {
  lazy val basicMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.basicMetrics
  lazy val taskMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.taskMetrics

  override lazy val metrics: Map[String, SQLMetric] = GpuMetric.unwrap(basicMetrics ++ taskMetrics)

  def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    Arm.withResource(runColumnar(sparkSession, child)) { batches =>
      assert(batches.isEmpty)
    }
    Seq.empty[Row]
  }

  def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch]

  def gpuWriteJobStatsTracker(
      hadoopConf: Configuration): GpuWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    GpuWriteJobStatsTracker(serializableHadoopConf, this)
  }

  def requireSingleBatch: Boolean
}

object GpuDataWritingCommand {
  private val allowNonEmptyLocationInCTASKey = "spark.sql.legacy.allowNonEmptyLocationInCTAS"

  private def getAllowNonEmptyLocationInCTAS: Boolean = {
    // config only exists in Spark 3.2+, so looking it up manually for now.
    val key = allowNonEmptyLocationInCTASKey
    val v = SQLConf.get.getConfString(key, "false")
    try {
      v.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $v")
    }
  }

  def assertEmptyRootPath(tablePath: URI, saveMode: SaveMode, hadoopConf: Configuration): Unit = {
    if (saveMode == SaveMode.ErrorIfExists && !getAllowNonEmptyLocationInCTAS) {
      val filePath = new org.apache.hadoop.fs.Path(tablePath)
      val fs = filePath.getFileSystem(hadoopConf)
      if (fs.exists(filePath) &&
          fs.getFileStatus(filePath).isDirectory &&
          fs.listStatus(filePath).length != 0) {
        throw RapidsErrorUtils.
          createTableAsSelectWithNonEmptyDirectoryError(tablePath.toString,
            allowNonEmptyLocationInCTASKey)
      }
    }
  }

  /**
   * When execute CTAS operators, the write can be delegated to a sub-command
   * and we need to propagate the metrics from that sub-command to the
   * parent command.
   * Derived from Spark's DataWritingCommand.propagateMetrics
   */
  def propogateMetrics(
      sparkContext: SparkContext,
      command: GpuDataWritingCommand,
      metrics: Map[String, SQLMetric]): Unit = {
    command.metrics.foreach { case (key, metric) => metrics(key).set(metric.value) }
    SQLMetrics.postDriverMetricUpdates(sparkContext,
      sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      metrics.values.toSeq)
  }
}

case class GpuDataWritingCommandExec(cmd: GpuDataWritingCommand, child: SparkPlan)
    extends ShimUnaryExecNode with GpuExec {
  override lazy val allMetrics: Map[String, GpuMetric] = GpuMetric.wrap(cmd.metrics)

  private lazy val sideEffectResult: Seq[ColumnarBatch] = {
    // Dump LoRE meta info if needed
    dumpLoreMetaInfo()
    // Execute the command with LoRE dumping if needed
    val childWithDumping = dumpLoreRDD(child)
    cmd.runColumnar(sparkSession, childWithDumping)
  }

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute " + cmd.nodeName

  // override the default one, otherwise the `cmd.nodeName` will appear twice from simpleString
  override def argString(maxFields: Int): String = {
    val baseArgs = cmd.argString(maxFields)
    val loreArgs = loreArgsString
    if (loreArgs.nonEmpty) {
      s"$baseArgs $loreArgs"
    } else {
      baseArgs
    }
  }

  override def executeCollect(): Array[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override def executeToIterator: Iterator[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override def executeTake(limit: Int): Array[InternalRow] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  protected override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    sparkContext.parallelize(sideEffectResult, 1)
  }

  // Need single batch in some cases
  override def childrenCoalesceGoal: Seq[CoalesceGoal] =
    if (cmd.requireSingleBatch) {
      Seq(RequireSingleBatch)
    } else {
      Seq(null)
    }

  // LoRE support methods
  protected def loreArgsString: String = {
    val loreIdStr = loreIdOf(this).map(id => s"[loreId=$id]")
    val lorePathStr = getTagValue(LORE_DUMP_PATH_TAG).map(path => s"[lorePath=$path]")
    val loreRDDInfoStr = getTagValue(LORE_DUMP_RDD_TAG).map(info => s"[loreRDDInfo=$info]")

    List(loreIdStr, lorePathStr, loreRDDInfoStr).flatten.mkString(" ")
  }

  private def dumpLoreMetaInfo(): Unit = {
    getTagValue(LORE_DUMP_PATH_TAG).foreach { rootPath =>
      GpuLore.dumpPlan(this, new Path(rootPath))
    }
  }

  private def dumpLoreRDD(inputChild: SparkPlan): SparkPlan = {
    getTagValue(LORE_DUMP_RDD_TAG).map { info =>
      // Check if child is supported in LORE before creating GpuLoreDumpExec
      if (!inputChild.isInstanceOf[GpuExec]) {
        throw new UnsupportedOperationException(
          s"LoRE dump is not supported for child of type ${inputChild.getClass.getSimpleName}. " +
          s"Only GpuExec instances are supported in LORE.")
      }
      // Create a new exec that wraps the child with LoRE dumping capability
      GpuLoreDumpExec(inputChild.asInstanceOf[GpuExec], info)
    }.getOrElse(inputChild)
  }
}


