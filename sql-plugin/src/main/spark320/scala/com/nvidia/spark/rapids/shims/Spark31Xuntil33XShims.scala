/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType

trait Spark31Xuntil33XShims extends SparkShims {

  def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = {
    GpuOverrides.neverReplaceExec[ShowCurrentNamespaceExec]("Namespace metadata operation")
  }

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference],
      fileFormat: Option[FileFormat]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = {
    Seq(GpuOverrides.dataWriteCmd[CreateDataSourceTableAsSelectCommand](
      "Create table with select command",
      (a, conf, p, r) => new CreateDataSourceTableAsSelectCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap
  }

  override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Map.empty
  }
}

// First, Last and Collect have mistakenly been marked as non-deterministic until Spark-3.3.
// They are actually deterministic iff their child expression is deterministic.
trait GpuDeterministicFirstLastCollectShim extends Expression {
  override lazy val deterministic = false
}
