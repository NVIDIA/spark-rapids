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

package org.apache.spark.sql.rapids.execution;

import java.io.Serializable;
import java.util.Objects;

import scala.Enumeration.Value;

/** Options to control join behavior. */
public final class JoinOptions implements Serializable {
  private static final long serialVersionUID = 0L;

  private final Value strategy;
  private final Value buildSideSelection;
  private final long targetSize;
  private final boolean logCardinalityEnabled;
  private final double sizeEstimateThreshold;

  public JoinOptions(
      Value strategy,
      Value buildSideSelection,
      long targetSize,
      boolean logCardinalityEnabled,
      double sizeEstimateThreshold) {
    this.strategy = strategy;
    this.buildSideSelection = buildSideSelection;
    this.targetSize = targetSize;
    this.logCardinalityEnabled = logCardinalityEnabled;
    this.sizeEstimateThreshold = sizeEstimateThreshold;
  }

  public Value strategy() {
    return strategy;
  }

  public Value buildSideSelection() {
    return buildSideSelection;
  }

  public long targetSize() {
    return targetSize;
  }

  public boolean logCardinalityEnabled() {
    return logCardinalityEnabled;
  }

  public double sizeEstimateThreshold() {
    return sizeEstimateThreshold;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof JoinOptions)) {
      return false;
    }
    JoinOptions that = (JoinOptions) other;
    return targetSize == that.targetSize
        && logCardinalityEnabled == that.logCardinalityEnabled
        && Double.compare(that.sizeEstimateThreshold, sizeEstimateThreshold) == 0
        && Objects.equals(strategy, that.strategy)
        && Objects.equals(buildSideSelection, that.buildSideSelection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        strategy, buildSideSelection, targetSize, logCardinalityEnabled, sizeEstimateThreshold);
  }

  @Override
  public String toString() {
    return "JoinOptions(" + strategy + "," + buildSideSelection + "," + targetSize + ","
        + logCardinalityEnabled + "," + sizeEstimateThreshold + ")";
  }
}
