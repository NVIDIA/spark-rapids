/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.iceberg.iceberg111x

import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded
import org.apache.iceberg.shaded.org.apache.parquet.schema.{LogicalTypeAnnotation => ShadedLogicalTypeAnnotation, MessageType => ShadedMessageType, Type => ShadedType}
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Type, Types}

// Keep Variant references in this shim: older Iceberg lacks the shaded class, and Spark 4.0
// lacks the unshaded factory. Recurse here so nested Variant groups use the same conversion.
object VariantUnshadeShim {
  def unshade(schema: ShadedMessageType): MessageType = {
    if (schema == null) {
      return null
    }
    val builder = Types.buildMessage()
    schema.getFields.forEach(field => builder.addField(unshade(field)))
    Option(schema.getId).foreach(id => builder.id(id.intValue()))
    builder.named(schema.getName)
  }

  private def unshade(field: ShadedType): Type = {
    if (field.isPrimitive) {
      FromIcebergShaded.unshade(field.asPrimitiveType())
    } else {
      val group = field.asGroupType()
      val builder = Types.buildGroup(FromIcebergShaded.unshade(group.getRepetition))
      group.getFields.forEach(child => builder.addField(unshade(child)))
      Option(group.getLogicalTypeAnnotation).foreach { annotation =>
        val unshaded = annotation match {
          case variant: ShadedLogicalTypeAnnotation.VariantLogicalTypeAnnotation =>
            LogicalTypeAnnotation.variantType(variant.getSpecVersion)
          case _ => FromIcebergShaded.unshade(annotation)
        }
        builder.as(unshaded)
      }
      Option(group.getId).foreach(id => builder.id(id.intValue()))
      builder.named(group.getName)
    }
  }
}
