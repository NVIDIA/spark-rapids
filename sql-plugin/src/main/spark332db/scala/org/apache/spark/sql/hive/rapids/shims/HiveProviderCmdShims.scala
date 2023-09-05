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

/*** spark-rapids-shim-json-lines
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.hive.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF

import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

trait HiveProviderCmdShims extends HiveProvider {

  def createFunction(a: HiveSimpleUDF): UDF = {
    a.function
  }

  def createFunction(a: HiveGenericUDF): GenericUDF = {
    a.function
  }


  /**
   * Builds the data writing command rules that are specific to spark-hive Catalyst nodes.
   */
  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
    DataWritingCommandRule[_ <: DataWritingCommand]] = Seq (

    GpuOverrides.dataWriteCmd[InsertIntoHiveTable](
      desc = "Command to write to Hive tables",
      (insert, conf, parent, rule) => new GpuInsertIntoHiveTableMeta(insert, conf, parent, rule))

  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
    RunnableCommandRule[_ <: RunnableCommand]] =
    Map.empty
}