/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.internal.SQLConf

// 320+ rebase related shims
trait RebaseShims {
  final def parquetRebaseReadKey: String =
    SQLConf.PARQUET_REBASE_MODE_IN_READ.key
  final def parquetRebaseWriteKey: String =
    SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key
  final def avroRebaseReadKey: String =
    SQLConf.AVRO_REBASE_MODE_IN_READ.key
  final def avroRebaseWriteKey: String =
    SQLConf.AVRO_REBASE_MODE_IN_WRITE.key
  final def parquetRebaseRead(conf: SQLConf): String =
    conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
  final def parquetRebaseWrite(conf: SQLConf): String =
    conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE)
  def int96ParquetRebaseRead(conf: SQLConf): String =
    conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)
  def int96ParquetRebaseWrite(conf: SQLConf): String =
    conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE)
  def int96ParquetRebaseReadKey: String =
    SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key
  def int96ParquetRebaseWriteKey: String =
    SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key
}
