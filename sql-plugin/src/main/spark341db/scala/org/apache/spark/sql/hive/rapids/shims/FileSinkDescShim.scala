/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "341db"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.hive.rapids.shims

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}

object FileSinkDescShim {
  def getPartitionColumns(tmpLocation: Path, tableDesc: TableDesc): String = {
    val fileSinkConf = new FileSinkDesc(tmpLocation, tableDesc, false)
    fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
  }
}