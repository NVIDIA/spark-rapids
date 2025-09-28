package org.apache.iceberg.spark.functions

import com.nvidia.spark.rapids.iceberg.fieldIndex
import org.apache.iceberg.Schema
import org.apache.iceberg.transforms.Transform
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

import scala.util.Try

/**
 * Iceberg partition transform types.
 * <br/>
 * We need to build our own transform types because the ones in iceberg are
 */
sealed trait GpuTransform {
  def support(inputType: DataType, nullable: Boolean): Boolean
}

case class GpuBucket(bucket: Int) extends GpuTransform {
  override def support(inputType: DataType, nullable: Boolean): Boolean = {
    !nullable && (inputType == IntegerType || inputType == org.apache.spark.sql.types.LongType)
  }
}

object GpuTransform {
  def apply(transform: String): GpuTransform = {
    if (transform.startsWith("bucket")) {
      val bucket = transform.substring("bucket[".length, transform.length - 1).toInt
      GpuBucket(bucket)
    } else {
      throw new IllegalArgumentException(s"Unsupported transform: $transform")
    }
  }

  def apply(icebergTransform: Transform[_, _]): GpuTransform = {
    GpuTransform(icebergTransform.toString)
  }

  def tryFrom(icebergTransform: Transform[_, _]): Try[GpuTransform] = {
    Try {
      GpuTransform(icebergTransform)
    }
  }
}

case class GpuFieldTransform(sourceFieldId: Int, transform: GpuTransform) {
  def supports(inputType: StructType, inputSchema: Schema): Boolean = {
    val fieldIdx = fieldIndex(inputSchema, sourceFieldId)
    val sparkField = inputType.fields(fieldIdx)
    transform.support(sparkField.dataType, sparkField.nullable)
  }
}


