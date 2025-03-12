package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.iceberg.spark.TypeToSparkType
import org.apache.iceberg.Schema
import org.apache.iceberg.types.TypeUtil
import scala.collection.JavaConverters._

import org.apache.spark.sql.types.StructType

package object iceberg {
  private[iceberg] def toSparkType(schema: Schema): StructType = {
    TypeUtil.visit(schema, new TypeToSparkType).asInstanceOf[StructType]
  }

  private[iceberg] def fieldIndex(schema: Schema, fieldId: Int): Int = {
    val idx = schema
      .columns()
      .asScala
      .indexWhere(_.fieldId() == fieldId)
    if (idx == -1) {
      throw new IllegalArgumentException(s"Field id $fieldId not found in schema")
    } else {
      idx
    }
  }
}
