/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.util.SerializableConfiguration

/**
 * An extension of [[DataWritingCommand]] that allows columnar execution.
 */
trait GpuDataWritingCommand extends DataWritingCommand {
  override lazy val metrics: Map[String, SQLMetric] = GpuWriteJobStatsTracker.metrics

  override final def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch]

  def gpuWriteJobStatsTracker(
      hadoopConf: Configuration): GpuWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    GpuWriteJobStatsTracker(serializableHadoopConf)
  }
}

case class GpuDataWritingCommandExec(cmd: GpuDataWritingCommand, child: SparkPlan)
    extends UnaryExecNode with GpuExec {
  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  private lazy val sideEffectResult: Seq[ColumnarBatch] =
    cmd.runColumnar(sqlContext.sparkSession, child)

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute " + cmd.nodeName

  // override the default one, otherwise the `cmd.nodeName` will appear twice from simpleString
  override def argString(maxFields: Int): String = cmd.argString(maxFields)

  override def executeCollect(): Array[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override def executeToIterator: Iterator[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override def executeTake(limit: Int): Array[InternalRow] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  protected override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}
