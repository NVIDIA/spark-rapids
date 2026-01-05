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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

/**
 * Type aliases for streaming classes that moved packages in Spark 4.1 / Databricks 17.3.
 * See: https://github.com/apache/spark/commit/d8dcfe778f33d4ebbaa74c8bd4b329097f37704b
 */
object StreamingShims {
  type FileStreamSinkType = org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
  type MetadataLogFileIndexType = org.apache.spark.sql.execution.streaming.runtime.MetadataLogFileIndex
  
  val FileStreamSink = org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
}

