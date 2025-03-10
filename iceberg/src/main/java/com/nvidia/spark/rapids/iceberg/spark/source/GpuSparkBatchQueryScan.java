/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.iceberg.spark.Spark3Util;
import com.nvidia.spark.rapids.iceberg.spark.SparkReadConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import com.nvidia.spark.rapids.iceberg.spark.SparkV2Filters;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;

/**
 * GPU-accelerated Iceberg batch scan.
 * This is derived from Apache Iceberg's BatchQueryScan class.
 */
public class GpuSparkBatchQueryScan extends GpuSparkPartitioningAwareScan<PartitionScanTask>
    implements SupportsRuntimeV2Filtering {
  private static final Logger LOG = LoggerFactory.getLogger(GpuSparkBatchQueryScan.class);

  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final String tag;
  private final List<Expression> runtimeFilterExpressions;
  // Check for file scan tasks that are reported as data tasks.
  // Null/empty tasks are assumed to be for scans not best performed by the GPU.
  @SuppressWarnings("unchecked")
  public static boolean isMetadataScan(Scan cpuInstance) throws IllegalAccessException {
    Table icebergTable = (Table) FieldUtils.readField(cpuInstance, "table", true);
    return !(icebergTable instanceof BaseTable);
  }

  @SuppressWarnings("unchecked")
  public static GpuSparkBatchQueryScan fromCpu(Scan cpuInstance, RapidsConf rapidsConf) throws IllegalAccessException {
    Table table = (Table) FieldUtils.readField(cpuInstance, "table", true);
    org.apache.iceberg.Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan =
        (org.apache.iceberg.Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>>) FieldUtils
            .readField(cpuInstance, "scan", true);
    SparkReadConf readConf = SparkReadConf.fromReflect(FieldUtils.readField(cpuInstance, "readConf", true));
    Schema expectedSchema = (Schema) FieldUtils.readField(cpuInstance, "expectedSchema", true);
    List<Expression> filters = (List<Expression>) FieldUtils.readField(cpuInstance, "filterExpressions", true);
    Supplier<ScanReport> scanReportSupplier = (Supplier<ScanReport>) FieldUtils.readField(cpuInstance, "scanReportSupplier", true);
    return new GpuSparkBatchQueryScan(
        SparkSession.active(),
        table,
        scan,
        readConf,
        expectedSchema,
        filters,
        scanReportSupplier,
        rapidsConf,
        false // queryUsesInputFile
    );
  }

  GpuSparkBatchQueryScan(
      SparkSession spark,
      Table table,
      org.apache.iceberg.Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier,
      RapidsConf rapidsConf,
      boolean queryUsesInputFile) {
    super(spark, table, scan, readConf, expectedSchema, filters, scanReportSupplier,
        rapidsConf, queryUsesInputFile);

    this.snapshotId = readConf.snapshotId();
    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    this.asOfTimestamp = readConf.asOfTimestamp();
    this.tag = readConf.tag();
    this.runtimeFilterExpressions = Lists.newArrayList();
  }

  Long snapshotId() {
    return snapshotId;
  }

  @Override
  protected Class<PartitionScanTask> taskJavaClass() {
    return PartitionScanTask.class;
  }


  @Override
  public NamedReference[] filterAttributes() {
    Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

    for (PartitionSpec spec : specs()) {
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
  public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (PartitionSpec spec : specs()) {
        Expression inclusiveExpr =
            Projections.inclusive(spec, caseSensitive()).project(runtimeFilterExpr);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(spec.specId(), inclusive);
      }

      List<PartitionScanTask> filteredTasks =
          tasks().stream()
              .filter(
                  task -> {
                    Evaluator evaluator = evaluatorsBySpecId.get(task.spec().specId());
                    return evaluator.eval(task.partition());
                  })
              .collect(Collectors.toList());

      LOG.info(
          "{} of {} task(s) for table {} matched runtime filter {}",
          filteredTasks.size(),
          tasks().size(),
          table().name(),
          ExpressionUtil.toSanitizedString(runtimeFilterExpr));

      // don't invalidate tasks if the runtime filter had no effect to avoid planning splits again
      if (filteredTasks.size() < tasks().size()) {
        resetTasks(filteredTasks);
      }

      // save the evaluated filter for equals/hashCode
      runtimeFilterExpressions.add(runtimeFilterExpr);
    }
  }

  // at this moment, Spark can only pass IN filters for a single attribute
  // if there are multiple filter attributes, Spark will pass two separate IN filters
  private Expression convertRuntimeFilters(Predicate[] predicates) {
    Expression runtimeFilterExpr = Expressions.alwaysTrue();

    for (Predicate predicate : predicates) {
      Expression expr = SparkV2Filters.convert(predicate);
      if (expr != null) {
        try {
          Binder.bind(expectedSchema().asStruct(), expr, caseSensitive());
          runtimeFilterExpr = Expressions.and(runtimeFilterExpr, expr);
        } catch (ValidationException e) {
          LOG.warn("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", predicate);
      }
    }

    return runtimeFilterExpr;
  }

  @Override
  public Statistics estimateStatistics() {
    if (scan() == null) {
      return estimateStatistics(null);

    } else if (snapshotId != null) {
      Snapshot snapshot = table().snapshot(snapshotId);
      return estimateStatistics(snapshot);

    } else if (asOfTimestamp != null) {
      long snapshotIdAsOfTime = SnapshotUtil.snapshotIdAsOfTime(table(), asOfTimestamp);
      Snapshot snapshot = table().snapshot(snapshotIdAsOfTime);
      return estimateStatistics(snapshot);

    } else if (branch() != null) {
      Snapshot snapshot = table().snapshot(branch());
      return estimateStatistics(snapshot);

    } else if (tag != null) {
      Snapshot snapshot = table().snapshot(tag);
      return estimateStatistics(snapshot);

    } else {
      Snapshot snapshot = table().currentSnapshot();
      return estimateStatistics(snapshot);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GpuSparkBatchQueryScan that = (GpuSparkBatchQueryScan) o;
    return table().name().equals(that.table().name())
        && Objects.equals(branch(), that.branch())
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filterExpressions().toString().equals(that.filterExpressions().toString())
        && runtimeFilterExpressions.toString().equals(that.runtimeFilterExpressions.toString())
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(startSnapshotId, that.startSnapshotId)
        && Objects.equals(endSnapshotId, that.endSnapshotId)
        && Objects.equals(asOfTimestamp, that.asOfTimestamp)
        && Objects.equals(tag, that.tag)
        && Objects.equals(queryUsesInputFile(), that.queryUsesInputFile());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        branch(),
        readSchema(),
        filterExpressions().toString(),
        runtimeFilterExpressions.toString(),
        snapshotId,
        startSnapshotId,
        endSnapshotId,
        asOfTimestamp,
        tag);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, branch=%s, type=%s, filters=%s, runtimeFilters=%s, caseSensitive=%s)",
        table(),
        branch(),
        expectedSchema().asStruct(),
        filterExpressions(),
        runtimeFilterExpressions,
        caseSensitive());
  }

  /** Return a copy of "this" but with "queryUsesInputFile = true" */
  public GpuSparkBatchQueryScan withInputFile() {
    return new GpuSparkBatchQueryScan(
        SparkSession.active(),
        table(),
        scan(),
        readConf(),
        expectedSchema(),
        filterExpressions(),
        scanReportSupplier(),
        rapidsConf(),
        true // queryUsesInputFile
    );
  }
}
