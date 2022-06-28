/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.iceberg.spark.Spark3Util;
import com.nvidia.spark.rapids.iceberg.spark.SparkFilters;
import com.nvidia.spark.rapids.iceberg.spark.SparkReadConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import com.nvidia.spark.rapids.shims.ShimSupportsRuntimeFiltering;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.sources.Filter;

/**
 * GPU-accelerated Iceberg batch scan.
 * This is derived from Apache Iceberg's BatchQueryScan class.
 */
public class GpuSparkBatchQueryScan extends GpuSparkScan implements ShimSupportsRuntimeFiltering {
  private static final Logger LOG = LoggerFactory.getLogger(GpuSparkBatchQueryScan.class);

  private final TableScan scan;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final List<Expression> runtimeFilterExpressions;

  private Set<Integer> specIds = null; // lazy cache of scanned spec IDs
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  // Check for file scan tasks that are reported as data tasks.
  // Null/empty tasks are assumed to be for scans not best performed by the GPU.
  @SuppressWarnings("unchecked")
  public static boolean isMetadataScan(Scan cpuInstance) throws IllegalAccessException {
    List<CombinedScanTask> tasks = (List<CombinedScanTask>) FieldUtils.readField(cpuInstance, "tasks", true);
    if (tasks == null || tasks.isEmpty()) {
      return true;
    }
    Iterator<FileScanTask> taskIter = tasks.get(0).files().iterator();
    return !taskIter.hasNext() || taskIter.next().isDataTask();
  }

  @SuppressWarnings("unchecked")
  public static GpuSparkBatchQueryScan fromCpu(Scan cpuInstance, RapidsConf rapidsConf) throws IllegalAccessException {
    Table table = (Table) FieldUtils.readField(cpuInstance, "table", true);
    SparkReadConf readConf = SparkReadConf.fromReflect(FieldUtils.readField(cpuInstance, "readConf", true));
    Schema expectedSchema = (Schema) FieldUtils.readField(cpuInstance, "expectedSchema", true);
    List<Expression> filters = (List<Expression>) FieldUtils.readField(cpuInstance, "filterExpressions", true);
    TableScan scan;
    try {
      scan = (TableScan) FieldUtils.readField(cpuInstance, "scan", true);
    } catch (IllegalArgumentException ignored) {
      // No TableScan instance, so try to build one now
      scan = buildScan(cpuInstance, table, readConf, expectedSchema, filters);
    }
    return new GpuSparkBatchQueryScan(SparkSession.active(), table, scan, readConf, expectedSchema, filters, rapidsConf);
  }

  // Try to build an Iceberg TableScan when one was not found in the CPU instance
  private static TableScan buildScan(Scan cpuInstance,
                                     Table table,
                                     SparkReadConf readConf,
                                     Schema expectedSchema,
                                     List<Expression> filterExpressions) throws IllegalAccessException {
    Long snapshotId = (Long) FieldUtils.readField(cpuInstance, "snapshotId", true);
    Long startSnapshotId = (Long) FieldUtils.readField(cpuInstance, "startSnapshotId", true);
    Long endSnapshotId = (Long) FieldUtils.readField(cpuInstance, "endSnapshotId", true);
    Long asOfTimestamp = (Long) FieldUtils.readField(cpuInstance, "asOfTimestamp", true);
    Long splitSize = (Long) FieldUtils.readField(cpuInstance, "splitSize", true);
    Integer splitLookback = (Integer) FieldUtils.readField(cpuInstance, "splitLookback", true);
    Long splitOpenFileCost = (Long) FieldUtils.readField(cpuInstance, "splitOpenFileCost", true);

    TableScan scan = table
        .newScan()
        .caseSensitive(readConf.caseSensitive())
        .project(expectedSchema);

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    if (asOfTimestamp != null) {
      scan = scan.asOfTime(asOfTimestamp);
    }

    if (startSnapshotId != null) {
      if (endSnapshotId != null) {
        scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
      } else {
        scan = scan.appendsAfter(startSnapshotId);
      }
    }

    if (splitSize != null) {
      scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.toString());
    }

    if (splitLookback != null) {
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookback.toString());
    }

    if (splitOpenFileCost != null) {
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.toString());
    }

    for (Expression filter : filterExpressions) {
      scan = scan.filter(filter);
    }

    return scan;
  }

  GpuSparkBatchQueryScan(SparkSession spark, Table table, TableScan scan, SparkReadConf readConf,
                         Schema expectedSchema, List<Expression> filters, RapidsConf rapidsConf) {

    super(spark, table, readConf, expectedSchema, filters, rapidsConf);

    this.scan = scan;
    this.snapshotId = readConf.snapshotId();
    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    this.asOfTimestamp = readConf.asOfTimestamp();
    this.runtimeFilterExpressions = Lists.newArrayList();

    if (scan == null) {
      this.specIds = Collections.emptySet();
      this.files = Collections.emptyList();
      this.tasks = Collections.emptyList();
    }
  }

  Long snapshotId() {
    return snapshotId;
  }

  private Set<Integer> specIds() {
    if (specIds == null) {
      Set<Integer> specIdSet = Sets.newHashSet();
      for (FileScanTask file : files()) {
        specIdSet.add(file.spec().specId());
      }
      this.specIds = specIdSet;
    }

    return specIds;
  }

  private List<FileScanTask> files() {
    if (files == null) {
      try (CloseableIterable<FileScanTask> filesIterable = scan.planFiles()) {
        this.files = Lists.newArrayList(filesIterable);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close table scan: " + scan, e);
      }
    }

    return files;
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files()),
          scan.targetSplitSize());
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, scan.targetSplitSize(),
          scan.splitLookback(), scan.splitOpenFileCost());
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public NamedReference[] filterAttributes() {
    Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

    for (Integer specId : specIds()) {
      PartitionSpec spec = table().specs().get(specId);
      for (PartitionField field : spec.fields()) {
        partitionFieldSourceIds.add(field.sourceId());
      }
    }

    Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(expectedSchema());

    // the optimizer will look for an equality condition with filter attributes in a join
    // as the scan has been already planned, filtering can only be done on projected attributes
    // that's why only partition source fields that are part of the read schema can be reported

    return partitionFieldSourceIds.stream()
        .filter(fieldId -> expectedSchema().findField(fieldId) != null)
        .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(Filter[] filters) {
    Expression runtimeFilterExpr = convertRuntimeFilters(filters);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (Integer specId : specIds()) {
        PartitionSpec spec = table().specs().get(specId);
        Expression inclusiveExpr = Projections.inclusive(spec, caseSensitive()).project(runtimeFilterExpr);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(specId, inclusive);
      }

      LOG.info("Trying to filter {} files using runtime filter {}", files().size(), runtimeFilterExpr);

      List<FileScanTask> filteredFiles = files().stream()
          .filter(file -> {
            Evaluator evaluator = evaluatorsBySpecId.get(file.spec().specId());
            return evaluator.eval(file.file().partition());
          })
          .collect(Collectors.toList());

      LOG.info("{}/{} files matched runtime filter {}", filteredFiles.size(), files().size(), runtimeFilterExpr);

      // don't invalidate tasks if the runtime filter had no effect to avoid planning splits again
      if (filteredFiles.size() < files().size()) {
        this.specIds = null;
        this.files = filteredFiles;
        this.tasks = null;
      }

      // save the evaluated filter for equals/hashCode
      runtimeFilterExpressions.add(runtimeFilterExpr);
    }
  }

  // at this moment, Spark can only pass IN filters for a single attribute
  // if there are multiple filter attributes, Spark will pass two separate IN filters
  private Expression convertRuntimeFilters(Filter[] filters) {
    Expression runtimeFilterExpr = Expressions.alwaysTrue();

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        try {
          Binder.bind(expectedSchema().asStruct(), expr, caseSensitive());
          runtimeFilterExpr = Expressions.and(runtimeFilterExpr, expr);
        } catch (ValidationException e) {
          LOG.warn("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", filter);
      }
    }

    return runtimeFilterExpr;
  }

  @Override
  public Statistics estimateStatistics() {
    if (scan == null) {
      return estimateStatistics(null);

    } else if (snapshotId != null) {
      Snapshot snapshot = table().snapshot(snapshotId);
      return estimateStatistics(snapshot);

    } else if (asOfTimestamp != null) {
      long snapshotIdAsOfTime = SnapshotUtil.snapshotIdAsOfTime(table(), asOfTimestamp);
      Snapshot snapshot = table().snapshot(snapshotIdAsOfTime);
      return estimateStatistics(snapshot);

    } else {
      Snapshot snapshot = table().currentSnapshot();
      return estimateStatistics(snapshot);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GpuSparkBatchQueryScan that = (GpuSparkBatchQueryScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        runtimeFilterExpressions.toString().equals(that.runtimeFilterExpressions.toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), readSchema(), filterExpressions().toString(), runtimeFilterExpressions.toString(),
        snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, runtimeFilters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), runtimeFilterExpressions, caseSensitive());
  }
}
