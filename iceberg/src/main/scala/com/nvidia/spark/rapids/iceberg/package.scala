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

package com.nvidia.spark.rapids

import org.apache.iceberg.Schema
import scala.collection.JavaConverters._

package object iceberg {
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