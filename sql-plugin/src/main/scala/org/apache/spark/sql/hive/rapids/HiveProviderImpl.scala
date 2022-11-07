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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}
import org.apache.spark.sql.hive.execution.{HiveTableScanExec, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StructType}

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
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[HiveTableScanExec](p, conf, parent, r) {

          def flagUnsupportedType(dataColumn: AttributeReference): Unit =
            willNotWorkOnGpu(s"Column ${dataColumn.name} of type ${dataColumn.dataType} " +
              s"is unsupported for Hive text tables. ")

          def flagIfUnsupportedType(dataColumn: AttributeReference): Unit =
            dataColumn.dataType match {
              // Unsupported types.
              case ArrayType(_, _) => flagUnsupportedType(dataColumn)
              case StructType(_)   => flagUnsupportedType(dataColumn)
              case MapType(_,_,_)  => flagUnsupportedType(dataColumn)
              case BinaryType      => flagUnsupportedType(dataColumn)
              // All else are supported.
              case _               =>
            }

          def flagIfUnsupportedStorageFormat(storage: CatalogStorageFormat): Unit = {
            val textInputFormat      = "org.apache.hadoop.mapred.TextInputFormat"
            val ignoreKeyOPFormat    = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            val lazySimpleSerDe      = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            val serializationKey     = "serialization.format"
            val ctrlASeparatedFormat = "1" // Implying '^A' field delimiter.
            if (  storage.inputFormat.getOrElse("")                  != textInputFormat
               || storage.outputFormat.getOrElse("")                 != ignoreKeyOPFormat
               || storage.serde.getOrElse("")                        != lazySimpleSerDe
               || storage.properties.getOrElse(serializationKey, "") != ctrlASeparatedFormat)
              {
                willNotWorkOnGpu("Only \'^A\' separated text input is currently supported.")
              }
          }

          override def convertToGpu(): GpuExec = {
            GpuHiveTableScanExec(wrapped.requestedAttributes,
                                 wrapped.relation,
                                 wrapped.partitionPruningPred)
          }

          override def tagPlanForGpu(): Unit = {
            val tableRelation = wrapped.relation

            // Bail out for unsupported column types.
            tableRelation.dataCols.foreach(flagIfUnsupportedType)

            // Check that the table and all participating partitions
            // are '^A' separated.
            flagIfUnsupportedStorageFormat(tableRelation.tableMeta.storage)
            if (tableRelation.isPartitioned) {
              tableRelation.prunedPartitions.getOrElse(Seq.empty)
                                            .map(_.storage)
                                            .foreach(flagIfUnsupportedStorageFormat)
            }
          }
        })
    ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
}
