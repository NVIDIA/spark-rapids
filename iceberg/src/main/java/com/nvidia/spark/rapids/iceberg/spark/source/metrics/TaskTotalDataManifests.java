/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.spark.source.metrics;

import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class TaskTotalDataManifests implements CustomTaskMetric {
  private final long value;

  private TaskTotalDataManifests(long value) {
    this.value = value;
  }

  @Override
  public String name() {
    return TotalDataManifests.NAME;
  }

  @Override
  public long value() {
    return value;
  }

  public static TaskTotalDataManifests from(ScanReport scanReport) {
    CounterResult counter = scanReport.scanMetrics().totalDataManifests();
    long value = counter != null ? counter.value() : 0L;
    return new TaskTotalDataManifests(value);
  }
}
