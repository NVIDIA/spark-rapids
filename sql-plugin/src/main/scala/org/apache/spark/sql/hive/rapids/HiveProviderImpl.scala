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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.{DataWritingCommandRule, ExecChecks, ExecRule, ExprChecks, ExprMeta, ExprRule, GpuExec, GpuExpression, GpuOverrides, HiveProvider, OptimizedCreateHiveTableAsSelectCommandMeta, RapidsConf, RepeatingParamCheck, SparkPlanMeta, TypeSig}
import com.nvidia.spark.rapids.GpuUserDefinedFunction.udfTypeSig

import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}
import org.apache.spark.sql.hive.execution.{HiveTableScanExec, OptimizedCreateHiveTableAsSelectCommand}

class HiveProviderImpl extends HiveProvider {

  /**
   * Builds the data writing command rules that are specific to spark-hive Catalyst nodes.
   */
  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = Seq (
    GpuOverrides.dataWriteCmd[OptimizedCreateHiveTableAsSelectCommand](
      "Create a Hive table from a query result using Spark data source APIs",
      (a, conf, p, r) => new OptimizedCreateHiveTableAsSelectCommandMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  /**
   * Builds the expression rules that are specific to spark-hive Catalyst nodes.
   */
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[HiveSimpleUDF](
        "Hive UDF, the UDF can choose to implement a RAPIDS accelerated interface to" +
            " get better performance",
        ExprChecks.projectOnly(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveSimpleUDF](a, conf, p, r) {
          private val opRapidsFunc = a.function match {
            case rapidsUDF: RapidsUDF => Some(rapidsUDF)
            case _ => None
          }

          override def tagExprForGpu(): Unit = {
            if (opRapidsFunc.isEmpty && !conf.isCpuBasedUDFEnabled) {
              willNotWorkOnGpu(s"Hive SimpleUDF ${a.name} implemented by " +
                  s"${a.funcWrapper.functionClassName} does not provide a GPU implementation " +
                  s"and CPU-based UDFs are not enabled by `${RapidsConf.ENABLE_CPU_BASED_UDF.key}`")
            }
          }

          override def convertToGpu(): GpuExpression = {
            opRapidsFunc.map { _ =>
              // We use the original HiveGenericUDF `deterministic` method as a proxy
              // for simplicity.
              GpuHiveSimpleUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()),
                a.dataType,
                a.deterministic)
            }.getOrElse {
              // This `require` is just for double check.
              require(conf.isCpuBasedUDFEnabled)
              GpuRowBasedHiveSimpleUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()))
            }
          }
        }),
      GpuOverrides.expr[HiveGenericUDF](
        "Hive Generic UDF, the UDF can choose to implement a RAPIDS accelerated interface to" +
            " get better performance",
        ExprChecks.projectOnly(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveGenericUDF](a, conf, p, r) {
          private val opRapidsFunc = a.function match {
            case rapidsUDF: RapidsUDF => Some(rapidsUDF)
            case _ => None
          }

          override def tagExprForGpu(): Unit = {
            if (opRapidsFunc.isEmpty && !conf.isCpuBasedUDFEnabled) {
              willNotWorkOnGpu(s"Hive GenericUDF ${a.name} implemented by " +
                  s"${a.funcWrapper.functionClassName} does not provide a GPU implementation " +
                  s"and CPU-based UDFs are not enabled by `${RapidsConf.ENABLE_CPU_BASED_UDF.key}`")
            }
          }

          override def convertToGpu(): GpuExpression = {
            opRapidsFunc.map { _ =>
              // We use the original HiveGenericUDF `deterministic` method as a proxy
              // for simplicity.
              GpuHiveGenericUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()),
                a.dataType,
                a.deterministic,
                a.foldable)
            }.getOrElse {
              // This `require` is just for double check.
              require(conf.isCpuBasedUDFEnabled)
              GpuRowBasedHiveGenericUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()))
            }
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    Seq(
      GpuOverrides.exec[HiveTableScanExec](
        desc = "Scan Exec to read Hive delimited text tables",
        ExecChecks(
          TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[HiveTableScanExec](p, conf, parent, r) {

          private def flagIfUnsupportedStorageFormat(storage: CatalogStorageFormat): Unit = {
            val textInputFormat      = "org.apache.hadoop.mapred.TextInputFormat"
            val lazySimpleSerDe      = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            val serializationKey     = "serialization.format"
            val ctrlASeparatedFormat = "1" // Implying '^A' field delimiter.
            val lineDelimiterKey     = "line.delim"
            val newLine              = "\n"

            if (storage.inputFormat.getOrElse("") != textInputFormat) {
              willNotWorkOnGpu(s"Unsupported input-format found: ${storage.inputFormat}. " +
                s"Only $textInputFormat is currently supported.")
            }

            if(storage.serde.getOrElse("") != lazySimpleSerDe) {
              willNotWorkOnGpu(s"Unsupported serde found: ${storage.serde}. " +
                s"Only $lazySimpleSerDe is currently supported.")
            }

            if(storage.properties.getOrElse(serializationKey, "") != ctrlASeparatedFormat) {
              willNotWorkOnGpu(s"Unsupported serialization format found: " +
                s"${storage.properties.getOrElse(serializationKey, "")}. " +
                s"Only \'^A\' separated text input (i.e. serialization.format=1) " +
                s"is currently supported.")
            }

            val lineTerminator = storage.properties.getOrElse(lineDelimiterKey, newLine)
            if(lineTerminator != newLine) {
              willNotWorkOnGpu(s"Unsupported line terminator found: " +
                s"$lineTerminator. " +
                s"Only newline (\\n) separated text input  is currently supported.")
            }
          }

          private def checkIfEnabled(): Unit = {
            if (!conf.isHiveDelimitedTextEnabled) {
              willNotWorkOnGpu("Hive Text I/O has been disabled. To enable this, " +
                               s"set ${RapidsConf.ENABLE_HIVE_TEXT} to true")
            }
            if (!conf.isHiveDelimitedTextReadEnabled) {
              willNotWorkOnGpu("Reading Hive delimited text tables has been disabled. " +
                               s"To enable this, set ${RapidsConf.ENABLE_HIVE_TEXT_READ} to true")
            }
          }

          override def tagPlanForGpu(): Unit = {
            checkIfEnabled()
            val tableRelation = wrapped.relation
            // Check that the table and all participating partitions
            // are '^A' separated.
            flagIfUnsupportedStorageFormat(tableRelation.tableMeta.storage)
            if (tableRelation.isPartitioned) {
              tableRelation.prunedPartitions.getOrElse(Seq.empty)
                                            .map(_.storage)
                                            .foreach(flagIfUnsupportedStorageFormat)
            }
          }

          override def convertToGpu(): GpuExec = {
            GpuHiveTableScanExec(wrapped.requestedAttributes,
              wrapped.relation,
              wrapped.partitionPruningPred)
          }
        })
    ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
}
