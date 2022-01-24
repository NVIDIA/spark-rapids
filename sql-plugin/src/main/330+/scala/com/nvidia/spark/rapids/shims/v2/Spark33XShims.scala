/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids._
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BitLength, Expression, OctetLength}
import org.apache.spark.sql.catalyst.json.rapids.shims.v2.Spark33XFileOptionsShims
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.rapids.{GpuBitLength, GpuOctetLength}
import org.apache.spark.sql.types.StructType

trait Spark33XShims extends Spark33XFileOptionsShims {
  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      lookupFileMeta: String => String,
      dateTimeRebaseModeFromConf: String): ParquetFilters = {
    val datetimeRebaseMode = DataSourceUtils
      .datetimeRebaseSpec(lookupFileMeta, dateTimeRebaseModeFromConf)
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    (super.getExprs.values ++ Seq(
      GpuOverrides.expr[BitLength](
        "The bit length of string data",
        ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
                                TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
        (a, conf, p, r) => new UnaryExprMeta[BitLength](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuBitLength(child)
        }),
      GpuOverrides.expr[OctetLength](
        "The bit length of string data",
        ExprChecks.unaryProject(
          TypeSig.INT, TypeSig.INT,
          TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
        (a, conf, p, r) => new UnaryExprMeta[OctetLength](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuOctetLength(child)
        })
      )).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
