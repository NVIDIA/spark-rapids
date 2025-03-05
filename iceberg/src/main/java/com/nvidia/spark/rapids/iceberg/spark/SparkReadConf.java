/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.spark;


import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.Util;

import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import static org.apache.iceberg.PlanningMode.LOCAL;

/**
 * A class for common Iceberg configs for Spark reads.
 * Derived from Apache Iceberg's SparkReadConf class.
 * <p>
 * If a config is set at multiple levels, the following order of precedence is used (top to bottom):
 * <ol>
 *   <li>Read options</li>
 *   <li>Session configuration</li>
 *   <li>Table metadata</li>
 * </ol>
 * The most specific value is set in read options and takes precedence over all other configs.
 * If no read option is provided, this class checks the session configuration for any overrides.
 * If no applicable value is found in the session configuration, this class uses the table metadata.
 * <p>
 * Note this class is NOT meant to be serialized and sent to executors.
 */
public class SparkReadConf {
  private static final String DRIVER_MAX_RESULT_SIZE = "spark.driver.maxResultSize";
  private static final String DRIVER_MAX_RESULT_SIZE_DEFAULT = "1G";
  private static final long DISTRIBUTED_PLANNING_MIN_RESULT_SIZE = 256L * 1024 * 1024; // 256 MB

  private final SparkSession spark;
  private final Table table;
  private final String branch;
  private final SparkConfParser confParser;

  public static SparkReadConf fromReflect(Object obj) throws IllegalAccessException {
    SparkSession spark = SparkSession.active();
    Table table = (Table) FieldUtils.readField(obj, "table", true);
    String branch = (String) FieldUtils.readField(obj, "branch", true);
    SparkConfParser confParser = SparkConfParser.fromReflect(FieldUtils.readField(obj, "confParser", true));
    return new SparkReadConf(spark, table, branch, confParser);
  }

  public SparkReadConf(SparkSession spark, Table table,
      String branch, SparkConfParser confParser) {
    this.spark = spark;
    this.table = table;
    this.branch = branch;
    this.confParser = confParser;
  }

  public boolean caseSensitive() {
    return Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
  }

  public boolean localityEnabled() {
    boolean defaultValue = Util.mayHaveBlockLocations(table.io(), table.location());
    return confParser
        .booleanConf()
        .option(SparkReadOptions.LOCALITY)
        .sessionConf(SparkSQLProperties.LOCALITY)
        .defaultValue(defaultValue)
        .parse();
  }

  public Long snapshotId() {
    return confParser.longConf().option(SparkReadOptions.SNAPSHOT_ID).parseOptional();
  }

  public Long asOfTimestamp() {
    return confParser.longConf().option(SparkReadOptions.AS_OF_TIMESTAMP).parseOptional();
  }

  public Long startSnapshotId() {
    return confParser.longConf().option(SparkReadOptions.START_SNAPSHOT_ID).parseOptional();
  }

  public Long endSnapshotId() {
    return confParser.longConf().option(SparkReadOptions.END_SNAPSHOT_ID).parseOptional();
  }

  public String branch() {
    String optionBranch = confParser.stringConf().option(SparkReadOptions.BRANCH).parseOptional();
    ValidationException.check(
        branch == null || optionBranch == null || optionBranch.equals(branch),
        "Must not specify different branches in both table identifier and read option, "
            + "got [%s] in identifier and [%s] in options",
        branch,
        optionBranch);
    String inputBranch = branch != null ? branch : optionBranch;
    if (inputBranch != null) {
      return inputBranch;
    }

    boolean wapEnabled =
        PropertyUtil.propertyAsBoolean(
            table.properties(), TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, false);
    if (wapEnabled) {
      String wapBranch = spark.conf().get(SparkSQLProperties.WAP_BRANCH, null);
      if (wapBranch != null && table.refs().containsKey(wapBranch)) {
        return wapBranch;
      }
    }

    return null;
  }

  public String tag() {
    return confParser.stringConf().option(SparkReadOptions.TAG).parseOptional();
  }

  public String scanTaskSetId() {
    return confParser.stringConf().option(SparkReadOptions.SCAN_TASK_SET_ID).parseOptional();
  }

  public boolean streamingSkipDeleteSnapshots() {
    return confParser
        .booleanConf()
        .option(SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS)
        .defaultValue(SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS_DEFAULT)
        .parse();
  }

  public boolean streamingSkipOverwriteSnapshots() {
    return confParser
        .booleanConf()
        .option(SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS)
        .defaultValue(SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS_DEFAULT)
        .parse();
  }

  public boolean parquetVectorizationEnabled() {
    return confParser
        .booleanConf()
        .option(SparkReadOptions.VECTORIZATION_ENABLED)
        .sessionConf(SparkSQLProperties.VECTORIZATION_ENABLED)
        .tableProperty(TableProperties.PARQUET_VECTORIZATION_ENABLED)
        .defaultValue(TableProperties.PARQUET_VECTORIZATION_ENABLED_DEFAULT)
        .parse();
  }

  public int parquetBatchSize() {
    return confParser
        .intConf()
        .option(SparkReadOptions.VECTORIZATION_BATCH_SIZE)
        .tableProperty(TableProperties.PARQUET_BATCH_SIZE)
        .defaultValue(TableProperties.PARQUET_BATCH_SIZE_DEFAULT)
        .parse();
  }

  public boolean orcVectorizationEnabled() {
    return confParser
        .booleanConf()
        .option(SparkReadOptions.VECTORIZATION_ENABLED)
        .sessionConf(SparkSQLProperties.VECTORIZATION_ENABLED)
        .tableProperty(TableProperties.ORC_VECTORIZATION_ENABLED)
        .defaultValue(TableProperties.ORC_VECTORIZATION_ENABLED_DEFAULT)
        .parse();
  }

  public int orcBatchSize() {
    return confParser
        .intConf()
        .option(SparkReadOptions.VECTORIZATION_BATCH_SIZE)
        .tableProperty(TableProperties.ORC_BATCH_SIZE)
        .defaultValue(TableProperties.ORC_BATCH_SIZE_DEFAULT)
        .parse();
  }

  public Long splitSizeOption() {
    return confParser.longConf().option(SparkReadOptions.SPLIT_SIZE).parseOptional();
  }

  public long splitSize() {
    return confParser
        .longConf()
        .option(SparkReadOptions.SPLIT_SIZE)
        .tableProperty(TableProperties.SPLIT_SIZE)
        .defaultValue(TableProperties.SPLIT_SIZE_DEFAULT)
        .parse();
  }

  public Integer splitLookbackOption() {
    return confParser.intConf().option(SparkReadOptions.LOOKBACK).parseOptional();
  }

  public int splitLookback() {
    return confParser
        .intConf()
        .option(SparkReadOptions.LOOKBACK)
        .tableProperty(TableProperties.SPLIT_LOOKBACK)
        .defaultValue(TableProperties.SPLIT_LOOKBACK_DEFAULT)
        .parse();
  }

  public Long splitOpenFileCostOption() {
    return confParser.longConf().option(SparkReadOptions.FILE_OPEN_COST).parseOptional();
  }

  public long splitOpenFileCost() {
    return confParser
        .longConf()
        .option(SparkReadOptions.FILE_OPEN_COST)
        .tableProperty(TableProperties.SPLIT_OPEN_FILE_COST)
        .defaultValue(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT)
        .parse();
  }

  public long streamFromTimestamp() {
    return confParser
        .longConf()
        .option(SparkReadOptions.STREAM_FROM_TIMESTAMP)
        .defaultValue(Long.MIN_VALUE)
        .parse();
  }

  public Long startTimestamp() {
    return confParser.longConf().option(SparkReadOptions.START_TIMESTAMP).parseOptional();
  }

  public Long endTimestamp() {
    return confParser.longConf().option(SparkReadOptions.END_TIMESTAMP).parseOptional();
  }

  public int maxFilesPerMicroBatch() {
    return confParser
        .intConf()
        .option(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH)
        .defaultValue(Integer.MAX_VALUE)
        .parse();
  }

  public int maxRecordsPerMicroBatch() {
    return confParser
        .intConf()
        .option(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH)
        .defaultValue(Integer.MAX_VALUE)
        .parse();
  }

  public boolean preserveDataGrouping() {
    return confParser
        .booleanConf()
        .sessionConf(SparkSQLProperties.PRESERVE_DATA_GROUPING)
        .defaultValue(SparkSQLProperties.PRESERVE_DATA_GROUPING_DEFAULT)
        .parse();
  }

  public boolean aggregatePushDownEnabled() {
    return confParser
        .booleanConf()
        .option(SparkReadOptions.AGGREGATE_PUSH_DOWN_ENABLED)
        .sessionConf(SparkSQLProperties.AGGREGATE_PUSH_DOWN_ENABLED)
        .defaultValue(SparkSQLProperties.AGGREGATE_PUSH_DOWN_ENABLED_DEFAULT)
        .parse();
  }

  public boolean adaptiveSplitSizeEnabled() {
    return confParser
        .booleanConf()
        .tableProperty(TableProperties.ADAPTIVE_SPLIT_SIZE_ENABLED)
        .defaultValue(TableProperties.ADAPTIVE_SPLIT_SIZE_ENABLED_DEFAULT)
        .parse();
  }

  public int parallelism() {
    int defaultParallelism = spark.sparkContext().defaultParallelism();
    int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();
    return Math.max(defaultParallelism, numShufflePartitions);
  }

  public boolean distributedPlanningEnabled() {
    return dataPlanningMode() != LOCAL || deletePlanningMode() != LOCAL;
  }

  public PlanningMode dataPlanningMode() {
    if (driverMaxResultSize() < DISTRIBUTED_PLANNING_MIN_RESULT_SIZE) {
      return LOCAL;
    }

    return confParser
        .enumConf(PlanningMode::fromName)
        .sessionConf(SparkSQLProperties.DATA_PLANNING_MODE)
        .tableProperty(TableProperties.DATA_PLANNING_MODE)
        .defaultValue(TableProperties.PLANNING_MODE_DEFAULT)
        .parse();
  }

  public PlanningMode deletePlanningMode() {
    if (driverMaxResultSize() < DISTRIBUTED_PLANNING_MIN_RESULT_SIZE) {
      return LOCAL;
    }

    String modeName =
        confParser
            .stringConf()
            .sessionConf(SparkSQLProperties.DELETE_PLANNING_MODE)
            .tableProperty(TableProperties.DELETE_PLANNING_MODE)
            .defaultValue(TableProperties.PLANNING_MODE_DEFAULT)
            .parse();
    return PlanningMode.fromName(modeName);
  }

  private long driverMaxResultSize() {
    SparkConf sparkConf = spark.sparkContext().conf();
    return sparkConf.getSizeAsBytes(DRIVER_MAX_RESULT_SIZE, DRIVER_MAX_RESULT_SIZE_DEFAULT);
  }

  public boolean executorCacheLocalityEnabled() {
    return executorCacheEnabled() && executorCacheLocalityEnabledInternal();
  }

  private boolean executorCacheEnabled() {
    return confParser
        .booleanConf()
        .sessionConf(SparkSQLProperties.EXECUTOR_CACHE_ENABLED)
        .defaultValue(SparkSQLProperties.EXECUTOR_CACHE_ENABLED_DEFAULT)
        .parse();
  }

  private boolean executorCacheLocalityEnabledInternal() {
    return confParser
        .booleanConf()
        .sessionConf(SparkSQLProperties.EXECUTOR_CACHE_LOCALITY_ENABLED)
        .defaultValue(SparkSQLProperties.EXECUTOR_CACHE_LOCALITY_ENABLED_DEFAULT)
        .parse();
  }

  public boolean reportColumnStats() {
    return confParser
        .booleanConf()
        .sessionConf(SparkSQLProperties.REPORT_COLUMN_STATS)
        .defaultValue(SparkSQLProperties.REPORT_COLUMN_STATS_DEFAULT)
        .parse();
  }

  /**
   * Enables reading a timestamp without time zone as a timestamp with time zone.
   * <p>
   * Generally, this is not safe as a timestamp without time zone is supposed to represent the wall-clock time,
   * i.e. no matter the reader/writer timezone 3PM should always be read as 3PM,
   * but a timestamp with time zone represents instant semantics, i.e. the timestamp
   * is adjusted so that the corresponding time in the reader timezone is displayed.
   * <p>
   * When set to false (default), an exception must be thrown while reading a timestamp without time zone.
   *
   * @return boolean indicating if reading timestamps without timezone is allowed
   */
  public boolean handleTimestampWithoutZone() {
    return confParser.booleanConf()
        .option(SparkReadOptions.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)
        .sessionConf(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)
        .defaultValue(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT)
        .parse();
  }

}
