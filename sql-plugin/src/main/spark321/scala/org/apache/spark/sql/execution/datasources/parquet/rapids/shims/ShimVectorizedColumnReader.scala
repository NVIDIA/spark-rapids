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
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.parquet.rapids.shims

import java.time.ZoneId
import java.util.TimeZone

import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.{GroupType, Type}

import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{ParentContainerUpdater, ParquetRowConverter, ParquetToSparkSchemaConverter, VectorizedColumnReader}
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.StructType

class ShimParquetRowConverter(
    schemaConverter: ParquetToSparkSchemaConverter,
    parquetType: GroupType,
    catalystType: StructType,
    convertTz: Option[ZoneId],
    datetimeRebaseMode: String,  // always LegacyBehaviorPolicy.CORRECTED
    int96RebaseMode: String,  // always LegacyBehaviorPolicy.EXCEPTION
    int96CDPHive3Compatibility: Boolean,
    updater: ParentContainerUpdater
) extends ParquetRowConverter(
      schemaConverter,
      parquetType,
      catalystType,
      convertTz,
      RebaseSpec(LegacyBehaviorPolicy.withName(datetimeRebaseMode)), // no need to rebase, so set originTimeZone as default
      RebaseSpec(LegacyBehaviorPolicy.withName(int96RebaseMode)), // no need to rebase, so set originTimeZone as default
      updater)

class ShimVectorizedColumnReader(
    index: Int,
    columns: java.util.List[ColumnDescriptor],
    types: java.util.List[Type],
    pageReadStore: PageReadStore,
    convertTz: ZoneId,
    datetimeRebaseMode: String, // always LegacyBehaviorPolicy.CORRECTED
    int96RebaseMode: String, // always LegacyBehaviorPolicy.EXCEPTION
    int96CDPHive3Compatibility: Boolean,
    writerVersion: ParsedVersion
) extends VectorizedColumnReader(
      columns.get(index),
      types.get(index).getLogicalTypeAnnotation,
      pageReadStore.getPageReader(columns.get(index)),
      pageReadStore.getRowIndexes().orElse(null),
      convertTz,
      datetimeRebaseMode,
      TimeZone.getDefault.getID, // use default zone because of no rebase
      int96RebaseMode,
      TimeZone.getDefault.getID) // use default zone because of will throw exception if rebase
