/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.types.StructType

object SchemaMetadataShims {
  // SPARK-43123 removes the internal metadata from the schema. Not available for Spark < 3.5.0.
  def getCleanedSchema(inputSchema: StructType): StructType = {
    inputSchema
  }
}
