/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.functions

import scala.util.Try

import com.nvidia.spark.rapids.iceberg.fieldIndex
import org.apache.iceberg.Schema
import org.apache.iceberg.transforms.Transform

import org.apache.spark.sql.types.{DataType, IntegerType, StructType}


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


