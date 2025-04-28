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

package com.nvidia.spark.rapids.iceberg.spark.source;

import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.colstats.Histogram;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * This class is derived from Apache Iceberg.
 */
class SparkColumnStatistics implements ColumnStatistics {

  private final OptionalLong distinctCount;
  private final Optional<Object> min;
  private final Optional<Object> max;
  private final OptionalLong nullCount;
  private final OptionalLong avgLen;
  private final OptionalLong maxLen;
  private final Optional<Histogram> histogram;

  SparkColumnStatistics(
      Long distinctCount,
      Object min,
      Object max,
      Long nullCount,
      Long avgLen,
      Long maxLen,
      Histogram histogram) {
    this.distinctCount =
        (distinctCount == null) ? OptionalLong.empty() : OptionalLong.of(distinctCount);
    this.min = Optional.ofNullable(min);
    this.max = Optional.ofNullable(max);
    this.nullCount = (nullCount == null) ? OptionalLong.empty() : OptionalLong.of(nullCount);
    this.avgLen = (avgLen == null) ? OptionalLong.empty() : OptionalLong.of(avgLen);
    this.maxLen = (maxLen == null) ? OptionalLong.empty() : OptionalLong.of(maxLen);
    this.histogram = Optional.ofNullable(histogram);
  }

  @Override
  public OptionalLong distinctCount() {
    return distinctCount;
  }

  @Override
  public Optional<Object> min() {
    return min;
  }

  @Override
  public Optional<Object> max() {
    return max;
  }

  @Override
  public OptionalLong nullCount() {
    return nullCount;
  }

  @Override
  public OptionalLong avgLen() {
    return avgLen;
  }

  @Override
  public OptionalLong maxLen() {
    return maxLen;
  }

  @Override
  public Optional<Histogram> histogram() {
    return histogram;
  }
}