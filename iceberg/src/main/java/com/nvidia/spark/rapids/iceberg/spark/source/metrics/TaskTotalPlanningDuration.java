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

import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class TaskTotalPlanningDuration implements CustomTaskMetric {

  private final long value;

  private TaskTotalPlanningDuration(long value) {
    this.value = value;
  }

  @Override
  public String name() {
    return TotalPlanningDuration.NAME;
  }

  @Override
  public long value() {
    return value;
  }

  public static TaskTotalPlanningDuration from(ScanReport scanReport) {
    TimerResult timerResult = scanReport.scanMetrics().totalPlanningDuration();
    long value = timerResult != null ? timerResult.totalDuration().toMillis() : -1;
    return new TaskTotalPlanningDuration(value);
  }
}
