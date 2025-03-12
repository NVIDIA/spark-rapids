package com.nvidia.spark.rapids.iceberg.data

import ai.rapids.cudf.{Table => CudfTable}
import com.nvidia.spark.rapids.{GpuColumnVector, LazySpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.parquet.{GpuCoalescingIcebergParquetReader, GpuIcebergParquetReader, GpuIcebergParquetReaderConf, GpuMultiThreadIcebergParquetReader, GpuSingleThreadIcebergParquetReader, IcebergPartitionedFile, MultiFile, MultiThread, SingleFile}
import org.apache.iceberg.{DeleteFile, Schema}
import org.apache.iceberg.io.InputFile
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuDeleteLoader {
  def loadDeletes(deletes: Seq[DeleteFile],
      schema: Schema,
      sparkTypes: Array[DataType]): LazySpillableColumnarBatch
}

class DefaultDeleteLoader(
    private val inputFiles: Map[String, InputFile],
    private val parquetConf: GpuIcebergParquetReaderConf) extends GpuDeleteLoader {

  def loadDeletes(deletes: Seq[DeleteFile],
      schema: Schema,
      sparkTypes: Array[DataType]): LazySpillableColumnarBatch = {
    val files = deletes.map(f => IcebergPartitionedFile(inputFiles(f.path().toString)))
    withResource(createReader(schema, files)) { reader =>
      withResource(new ArrayBuffer[ColumnarBatch]()) { batches =>
        while (reader.hasNext) {
          batches += reader.next()
        }

        withResource(new ArrayBuffer[CudfTable](batches.size)) { tables =>
          batches.foreach { batch =>
            tables += GpuColumnVector.from(batch)
          }

          if (tables.size > 1) {
            withResource(CudfTable.concatenate(tables.toArray: _*)) { combined =>
              withResource(GpuColumnVector.from(combined, sparkTypes)) { combinedBatch =>
                LazySpillableColumnarBatch(combinedBatch, "Eq deletes")
              }
            }
          } else {
            withResource(GpuColumnVector.from(tables.head, sparkTypes)) { singleBatch =>
              LazySpillableColumnarBatch(singleBatch, "Eq deletes")
            }
          }
        }
      }
    }
  }

  private def createReader(schema: Schema,
      files: Seq[IcebergPartitionedFile]): GpuIcebergParquetReader = {
    val newConf = parquetConf.copy(expectedSchema = schema)
    newConf.parquetConf.threadConf match {
      case SingleFile =>
        new GpuSingleThreadIcebergParquetReader(files,
          _ => Map.empty[Integer, Any].asJava,
          _ => None,
          newConf)
      case MultiThread(_, _) =>
        new GpuMultiThreadIcebergParquetReader(files,
          _ => Map.empty[Integer, Any].asJava,
          _ => None,
          newConf)
      case MultiFile(_) =>
        new GpuCoalescingIcebergParquetReader(files,
          _ => Map.empty[Integer, Any].asJava,
          newConf)
    }
  }
}
