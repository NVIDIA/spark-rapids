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

package com.nvidia.spark.rapids.iceberg.spark

import org.apache.iceberg.spark.SparkCatalog

import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * A drop-in replacement for [[SparkCatalog]] that wraps every iceberg
 * [[org.apache.iceberg.spark.source.SparkTable]] returned by `loadTable` with a
 * [[com.nvidia.spark.rapids.iceberg.spark.source.RapidsSparkTable]]. The wrapper
 * augments scan options with session-level overrides keyed by
 * `spark.rapids.iceberg.&lt;catalog&gt;.&lt;namespace&gt;.&lt;table&gt;.`.
 *
 * Configure via:
 * {{{
 *   spark.sql.catalog.&lt;catalog&gt; =
 *     com.nvidia.spark.rapids.iceberg.spark.RapidsSparkCatalog
 * }}}
 */
class RapidsSparkCatalog extends SparkCatalog {

  override def loadTable(ident: Identifier): Table =
    RapidsSparkSessionCatalog.wrap(name(), ident, super.loadTable(ident))

  override def loadTable(ident: Identifier, version: String): Table =
    RapidsSparkSessionCatalog.wrap(name(), ident, super.loadTable(ident, version))

  override def loadTable(ident: Identifier, timestamp: Long): Table =
    RapidsSparkSessionCatalog.wrap(name(), ident, super.loadTable(ident, timestamp))
}
