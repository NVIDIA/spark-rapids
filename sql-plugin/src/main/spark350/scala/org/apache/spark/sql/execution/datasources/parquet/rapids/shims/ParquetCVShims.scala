/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.StructType
object ParquetCVShims {

  def newParquetCV(
      sparkSchema: StructType,
      idx: Int,
      column: ParquetColumn,
      vector: WritableColumnVector,
      capacity: Int,
      memoryMode: MemoryMode,
      missingColumns: java.util.Set[ParquetColumn],
      isTopLevel: Boolean): ParquetColumnVector = {
    val defaultValue = if (sparkSchema != null) {
      ResolveDefaultColumns.getExistenceDefaultValues(sparkSchema)(idx)
    } else null
    new ParquetColumnVector(column, vector, capacity, memoryMode, missingColumns, isTopLevel,
      defaultValue)
  }
}
