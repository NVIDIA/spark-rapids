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
{"spark": "350db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.parquet

import java.time.ZoneId

import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore

object RapidsVectorizedColumnReader {
  def apply(descriptor: ColumnDescriptor,
      isRequired: Boolean,
      pageReadStore: PageReadStore,
      convertTz: ZoneId,
      datetimeRebaseMode: String,
      datetimeRebaseTz: String,
      int96RebaseMode: String,
      int96RebaseTz: String,
      writerVersion: ParsedVersion) = {
    val useNativeDictionary = false
    new VectorizedColumnReader(
      descriptor,
      useNativeDictionary,
      isRequired,
      pageReadStore,
      null,
      datetimeRebaseMode,
      datetimeRebaseTz,
      int96RebaseMode,
      null,
      writerVersion)
  }
}
