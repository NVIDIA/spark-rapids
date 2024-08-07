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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}

object BucketSpecForHiveShim {
  // Only for GpuInsertIntoHiveTable. The "InsertIntoHiveTable" in normal Spark before 330
  // does not execute the bucketed write even given a bucket specification. But some customized
  // Spark binaries before 330 indeed will do it. So "forceHiveHash" is introduced to give a
  // chance to enable the bucket write for this case.
  def getBucketSpec(table: CatalogTable, forceHiveHash: Boolean): Option[BucketSpec] = {
    if (forceHiveHash) {
      table.bucketSpec
    } else {
      None
    }
  }
}
