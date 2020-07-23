/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.shims.spark310

import java.util.concurrent.TimeUnit.NANOSECONDS

import com.nvidia.spark.rapids.{GpuExec, GpuReadCSVFileFormat, GpuReadOrcFileFormat, GpuReadParquetFileFormat, SparkPlanMeta}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{DataSourceScanExec, ExplainUtils, FileSourceScanExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuFileSourceScanExecBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

case class GpuFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
    extends DataSourceScanExec with GpuFileSourceScanExecBase with GpuExec {

  override val nodeName: String = {
    s"GpuScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  private[this] val wrapped: FileSourceScanExec = {
    val tclass = classOf[org.apache.spark.sql.execution.FileSourceScanExec]
    val constructors = tclass.getConstructors()
    if (constructors.size > 1) {
      throw new IllegalStateException(s"Only expected 1 constructor for FileSourceScanExec")
    }
    val constructor = constructors(0)
    val instance = if (constructor.getParameterCount() == 8) {
      // Some distributions of Spark modified FileSourceScanExec to take an additional parameter
      // that is the logicalRelation. We don't know what its used for exactly but haven't
      // run into any issues in testing using the one we create here.
      @transient val logicalRelation = LogicalRelation(relation)
      try {
        constructor.newInstance(relation, output, requiredSchema, partitionFilters,
          optionalBucketSet, dataFilters, tableIdentifier,
          logicalRelation).asInstanceOf[FileSourceScanExec]
      } catch {
        case il: IllegalArgumentException =>
          // TODO - workaround until https://github.com/NVIDIA/spark-rapids/issues/354
          constructor.newInstance(relation, output, requiredSchema, partitionFilters,
            optionalBucketSet, None, dataFilters, tableIdentifier).asInstanceOf[FileSourceScanExec]
      }
    } else {
      constructor.newInstance(relation, output, requiredSchema, partitionFilters,
        optionalBucketSet, dataFilters, tableIdentifier).asInstanceOf[FileSourceScanExec]
    }
    instance
  }

  override lazy val outputPartitioning: Partitioning  = wrapped.outputPartitioning

  override lazy val outputOrdering: Seq[SortOrder] = wrapped.outputOrdering

  override lazy val metadata: Map[String, String] = wrapped.metadata

  override lazy val metrics: Map[String, SQLMetric] = wrapped.metrics

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = wrapped.relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLoaction = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLoaction)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    wrapped.inputRDD :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    wrapped.inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override val nodeNamePrefix: String = "Gpu" + wrapped.nodeNamePrefix

  override def doCanonicalize(): GpuFileSourceScanExec = {
    val canonical = wrapped.doCanonicalize()
    GpuFileSourceScanExec(
      canonical.relation,
      canonical.output,
      canonical.requiredSchema,
      canonical.partitionFilters,
      canonical.optionalBucketSet,
      canonical.dataFilters,
      canonical.tableIdentifier)
  }
}

object GpuFileSourceScanExec {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    meta.wrapped.relation.fileFormat match {
      case _: CSVFileFormat => GpuReadCSVFileFormat.tagSupport(meta)
      case _: OrcFileFormat => GpuReadOrcFileFormat.tagSupport(meta)
      case _: ParquetFileFormat => GpuReadParquetFileFormat.tagSupport(meta)
      case f =>
        meta.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
    }
  }

  def convertFileFormat(format: FileFormat): FileFormat = {
    format match {
      case _: CSVFileFormat => new GpuReadCSVFileFormat
      case _: OrcFileFormat => new GpuReadOrcFileFormat
      case _: ParquetFileFormat => new GpuReadParquetFileFormat
      case f =>
        throw new IllegalArgumentException(s"${f.getClass.getCanonicalName} is not supported")
    }
  }
}
