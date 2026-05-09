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

package com.nvidia.spark.rapids.iceberg.spark.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkTable;

import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import scala.Option;
import scala.collection.JavaConverters;

/**
 * Wraps an iceberg {@link SparkTable} so that scan options can be augmented from
 * Spark session configuration entries with the prefix
 * {@code spark.rapids.iceberg.<catalog>.<namespace>.<table>.}.
 *
 * <p>Supported suffixes (mapped to the corresponding Iceberg read option):
 * <ul>
 *   <li>{@code read-split-target-size}       -&gt; {@link SparkReadOptions#SPLIT_SIZE}</li>
 *   <li>{@code read-split-planning-lookback} -&gt; {@link SparkReadOptions#LOOKBACK}</li>
 *   <li>{@code read-split-open-file-cost}    -&gt; {@link SparkReadOptions#FILE_OPEN_COST}</li>
 * </ul>
 *
 * <p>Suffixes mirror iceberg's TableProperties keys ({@code read.split.target-size}, etc.)
 * with {@code -} instead of {@code .} so the dot-separated session-conf path stays
 * unambiguous.
 *
 * <p>Explicit DataFrame read options take precedence over session-level overrides.
 */
public class RapidsSparkTable implements Table,
    SupportsRead,
    SupportsWrite,
    SupportsDeleteV2,
    SupportsRowLevelOperations,
    SupportsMetadataColumns {

  /** Prefix used for per-table session-level scan option overrides. */
  public static final String CONF_PREFIX = "spark.rapids.iceberg.";

  /**
   * Mapping from session-conf suffix to the iceberg read option key. Suffix names
   * mirror iceberg's TableProperties keys ({@code read.split.target-size},
   * {@code read.split.planning-lookback}, {@code read.split.open-file-cost}) with
   * {@code -} instead of {@code .} so the dot-separated session-conf key remains
   * unambiguous.
   */
  private static final Map<String, String> SUFFIX_TO_READ_OPTION;
  static {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("read-split-target-size", SparkReadOptions.SPLIT_SIZE);
    m.put("read-split-planning-lookback", SparkReadOptions.LOOKBACK);
    m.put("read-split-open-file-cost", SparkReadOptions.FILE_OPEN_COST);
    SUFFIX_TO_READ_OPTION = m;
  }

  private final SparkTable delegate;
  private final String catalogName;
  // Iceberg's Table.name() returns "<icebergCatalog>.<namespaceParts>.<table>",
  // so namespace and table are recovered from the delegate by dropping the
  // leading iceberg-catalog component. Computed lazily in initIdentifier().
  private String[] namespace;
  private String tableName;
  private boolean identifierInitialized;

  public RapidsSparkTable(SparkTable delegate, String catalogName) {
    this.delegate = delegate;
    this.catalogName = catalogName;
  }

  public SparkTable delegate() {
    return delegate;
  }

  private void initIdentifier() {
    if (identifierInitialized) {
      return;
    }
    String name = delegate.table().name();
    String[] parts = name.split("\\.");
    if (parts.length < 2) {
      throw new IllegalStateException(
          "Cannot derive namespace and table from iceberg table name '" + name +
          "': expected '<catalog>.<namespace>.<table>'.");
    }
    this.namespace = Arrays.copyOfRange(parts, 1, parts.length - 1);
    this.tableName = parts[parts.length - 1];
    this.identifierInitialized = true;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return delegate.schema();
  }

  @Override
  public Transform[] partitioning() {
    return delegate.partitioning();
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return delegate.capabilities();
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return delegate.metadataColumns();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    initIdentifier();
    CaseInsensitiveStringMap merged =
        mergeSessionOptions(options, catalogName, namespace, tableName);
    return delegate.newScanBuilder(merged);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return delegate.newWriteBuilder(info);
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    return delegate.newRowLevelOperationBuilder(info);
  }

  @Override
  public boolean canDeleteWhere(Predicate[] filters) {
    return delegate.canDeleteWhere(filters);
  }

  @Override
  public void deleteWhere(Predicate[] filters) {
    delegate.deleteWhere(filters);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RapidsSparkTable) {
      return delegate.equals(((RapidsSparkTable) other).delegate);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  static CaseInsensitiveStringMap mergeSessionOptions(
      CaseInsensitiveStringMap options,
      String catalogName,
      String[] namespace,
      String tableName) {
    // getActiveSession returns the thread-local active SparkSession. Empty for
    // call sites that do not run inside a Spark session (unit tests, tooling) —
    // in that case we have no session conf to read, so just return the original
    // options unchanged.
    Option<SparkSession> sparkOpt = SparkSession.getActiveSession();
    if (sparkOpt.isEmpty()) {
      return options;
    }
    RuntimeConfig sparkConf = sparkOpt.get().conf();
    Map<String, String> allConfs = JavaConverters.mapAsJavaMap(sparkConf.getAll());
    return mergeFromConfs(options, catalogName, namespace, tableName, allConfs);
  }

  // Pure-logic core of mergeSessionOptions: takes the conf map directly so it can
  // be exercised from unit tests without an active SparkSession.
  static CaseInsensitiveStringMap mergeFromConfs(
      CaseInsensitiveStringMap options,
      String catalogName,
      String[] namespace,
      String tableName,
      Map<String, String> allConfs) {
    String[] components = new String[1 + namespace.length + 1];
    components[0] = catalogName;
    System.arraycopy(namespace, 0, components, 1, namespace.length);
    components[components.length - 1] = tableName;
    String tableKey = String.join(".", components);
    String prefix = CONF_PREFIX + tableKey + ".";

    // Pure pass-through when nothing under our prefix is set for this table —
    // identifiers containing '.' must not break scans for tables the user has
    // not explicitly tuned.
    boolean hasMatchingConfs = false;
    for (String key : allConfs.keySet()) {
      if (key.startsWith(prefix)) {
        hasMatchingConfs = true;
        break;
      }
    }
    if (!hasMatchingConfs) {
      return options;
    }

    // Conf keys are joined with '.', so a '.' in catalog / namespace / table makes
    // the resulting prefix ambiguous (catalog="hadoop.prod" + namespace=["ns"] +
    // table="tbl" and catalog="hadoop" + namespace=["prod","ns"] + table="tbl"
    // both produce the same prefix). Fail loudly rather than silently picking the
    // wrong override — only when the user actually configured something for this
    // table.
    String[] ambiguous = Arrays.stream(components)
        .filter(c -> c.indexOf('.') >= 0)
        .toArray(String[]::new);
    if (ambiguous.length > 0) {
      throw new IllegalArgumentException(
          "Cannot apply " + CONF_PREFIX + "* session overrides for " +
          catalogName + "." + String.join(".", namespace) + "." + tableName +
          ": identifier component(s) " + Arrays.toString(ambiguous) +
          " contain '.', which makes the conf prefix ambiguous. Rename the " +
          "identifier or set the iceberg read.split.* table property directly.");
    }

    // Reject any conf under our prefix whose suffix is not one of the recognized
    // ones, so misspelled / unsupported keys fail loudly instead of being a no-op.
    for (String key : allConfs.keySet()) {
      if (key.startsWith(prefix)) {
        String suffix = key.substring(prefix.length());
        if (!SUFFIX_TO_READ_OPTION.containsKey(suffix)) {
          Set<String> sortedSuffixes = new TreeSet<>(SUFFIX_TO_READ_OPTION.keySet());
          throw new IllegalArgumentException(
              "Unrecognized suffix '" + suffix + "' in conf '" + key +
              "'. Supported suffixes: " + sortedSuffixes + ".");
        }
      }
    }

    Map<String, String> merged = new HashMap<>(options.asCaseSensitiveMap());
    boolean changed = false;
    for (Map.Entry<String, String> entry : SUFFIX_TO_READ_OPTION.entrySet()) {
      String suffix = entry.getKey();
      String optionKey = entry.getValue();
      // Explicit DataFrame option always wins over session-level overrides.
      if (!options.containsKey(optionKey)) {
        String value = allConfs.get(prefix + suffix);
        if (value != null) {
          merged.put(optionKey, value);
          changed = true;
        }
      }
    }
    return changed ? new CaseInsensitiveStringMap(merged) : options;
  }
}
