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

package com.nvidia.spark.rapids.iceberg.spark.source

import java.util.{HashMap => JHashMap, Map => JMap, Set => JSet}

import org.apache.iceberg.spark.SparkReadOptions
import org.apache.iceberg.spark.source.SparkTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsDeleteV2, SupportsMetadataColumns, SupportsRead, SupportsRowLevelOperations, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Wraps an iceberg [[SparkTable]] so that scan options can be augmented from Spark
 * session configuration entries with the prefix
 * `spark.rapids.iceberg.&lt;catalog&gt;.&lt;namespace&gt;.&lt;table&gt;.`.
 *
 * Supported suffixes (mapped to the corresponding Iceberg read option):
 *   - `read-split-target-size`       -> [[SparkReadOptions.SPLIT_SIZE]]
 *   - `read-split-planning-lookback` -> [[SparkReadOptions.LOOKBACK]]
 *   - `read-split-open-file-cost`    -> [[SparkReadOptions.FILE_OPEN_COST]]
 *
 * Suffixes mirror iceberg's TableProperties keys (`read.split.target-size`, etc.)
 * with `-` instead of `.` so the dot-separated session-conf path stays unambiguous.
 *
 * Explicit DataFrame read options take precedence over session-level overrides.
 */
class RapidsSparkTable(
    val delegate: SparkTable,
    catalogName: String)
  extends Table
    with SupportsRead
    with SupportsWrite
    with SupportsDeleteV2
    with SupportsRowLevelOperations
    with SupportsMetadataColumns {

  // Iceberg's `Table.name()` returns `<icebergCatalog>.<namespaceParts>.<table>`,
  // so namespace and table can be recovered from the delegate by dropping the
  // leading iceberg-catalog component.
  private lazy val (namespace: Array[String], tableName: String) = {
    val parts = delegate.table().name().split('.')
    if (parts.length < 2) {
      throw new IllegalStateException(
        s"Cannot derive namespace and table from iceberg table name " +
        s"'${delegate.table().name()}': expected " +
        s"'<catalog>.<namespace>.<table>'.")
    }
    (parts.slice(1, parts.length - 1), parts.last)
  }

  override def name(): String = delegate.name()

  override def schema(): StructType = delegate.schema()

  override def partitioning(): Array[Transform] = delegate.partitioning()

  override def properties(): JMap[String, String] = delegate.properties()

  override def capabilities(): JSet[TableCapability] = delegate.capabilities()

  override def metadataColumns(): Array[MetadataColumn] = delegate.metadataColumns()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val merged = RapidsSparkTable.mergeSessionOptions(
      options, catalogName, namespace, tableName)
    delegate.newScanBuilder(merged)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    delegate.newWriteBuilder(info)

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder =
    delegate.newRowLevelOperationBuilder(info)

  override def canDeleteWhere(filters: Array[Predicate]): Boolean =
    delegate.canDeleteWhere(filters)

  override def deleteWhere(filters: Array[Predicate]): Unit =
    delegate.deleteWhere(filters)

  override def toString: String = delegate.toString

  override def equals(other: Any): Boolean = other match {
    case that: RapidsSparkTable => delegate == that.delegate
    case _ => false
  }

  override def hashCode(): Int = delegate.hashCode()
}

object RapidsSparkTable {
  /** Prefix used for per-table session-level scan option overrides. */
  val CONF_PREFIX: String = "spark.rapids.iceberg."

  /**
   * Mapping from session-conf suffix to the iceberg read option key. Suffix names
   * mirror iceberg's TableProperties keys (`read.split.target-size`,
   * `read.split.planning-lookback`, `read.split.open-file-cost`) with `-` instead
   * of `.` so the dot-separated session-conf key remains unambiguous.
   */
  private val SUFFIX_TO_READ_OPTION: Seq[(String, String)] = Seq(
    "read-split-target-size" -> SparkReadOptions.SPLIT_SIZE,
    "read-split-planning-lookback" -> SparkReadOptions.LOOKBACK,
    "read-split-open-file-cost" -> SparkReadOptions.FILE_OPEN_COST)

  private[spark] def mergeSessionOptions(
      options: CaseInsensitiveStringMap,
      catalogName: String,
      namespace: Array[String],
      tableName: String): CaseInsensitiveStringMap = {
    // getActiveSession returns the thread-local active SparkSession. Empty for
    // call sites that do not run inside a Spark session (unit tests, tooling) —
    // in that case we have no session conf to read, so just return the original
    // options unchanged.
    val sparkOpt = SparkSession.getActiveSession
    if (sparkOpt.isEmpty) {
      return options
    }
    // Conf keys are joined with `.`, so a `.` in catalog / namespace / table makes
    // the resulting prefix ambiguous: e.g. catalog="hadoop.prod" + namespace=["ns"]
    // + table="tbl" and catalog="hadoop" + namespace=["prod", "ns"] + table="tbl"
    // both produce `spark.rapids.iceberg.hadoop.prod.ns.tbl.<suffix>`. Fail loudly
    // rather than silently picking the wrong override.
    val components = catalogName +: namespace :+ tableName
    val ambiguous = components.filter(_.contains('.'))
    if (ambiguous.nonEmpty) {
      throw new IllegalArgumentException(
        s"Cannot apply ${CONF_PREFIX}* session overrides for " +
        s"$catalogName.${namespace.mkString(".")}.$tableName: identifier " +
        s"component(s) ${ambiguous.mkString("[", ", ", "]")} contain '.', " +
        s"which makes the conf prefix ambiguous. Rename the identifier or " +
        s"set the iceberg read.split.* table property directly.")
    }
    val sparkConf = sparkOpt.get.conf
    val tableKey = components.mkString(".")
    val prefix = s"$CONF_PREFIX$tableKey."
    // Reject any conf under our prefix whose suffix is not one of the recognized
    // ones, so misspelled / unsupported keys fail loudly instead of being a no-op.
    val validSuffixes = SUFFIX_TO_READ_OPTION.map(_._1).toSet
    sparkConf.getAll.foreach { case (key, _) =>
      if (key.startsWith(prefix)) {
        val suffix = key.substring(prefix.length)
        if (!validSuffixes.contains(suffix)) {
          throw new IllegalArgumentException(
            s"Unrecognized suffix '$suffix' in conf '$key'. Supported " +
            s"suffixes: ${validSuffixes.toSeq.sorted.mkString("[", ", ", "]")}.")
        }
      }
    }
    val merged = new JHashMap[String, String](options.asCaseSensitiveMap())
    var changed = false
    SUFFIX_TO_READ_OPTION.foreach { case (suffix, optionKey) =>
      // Explicit DataFrame option always wins over session-level overrides.
      if (!options.containsKey(optionKey)) {
        sparkConf.getOption(prefix + suffix).foreach { value =>
          merged.put(optionKey, value)
          changed = true
        }
      }
    }
    if (changed) new CaseInsensitiveStringMap(merged) else options
  }
}
