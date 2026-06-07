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

package com.nvidia.spark.rapids;

import java.io.Serializable;
import java.util.Objects;

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateMode;
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete$;
import org.apache.spark.sql.catalyst.expressions.aggregate.Final$;
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial$;
import org.apache.spark.sql.catalyst.expressions.aggregate.PartialMerge$;

import scala.collection.Seq;

/**
 * Information on the aggregation modes being used.
 */
public class AggregateModeInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Seq<AggregateMode> uniqueModes;
  private final boolean hasPartialMode;
  private final boolean hasPartialMergeMode;
  private final boolean hasFinalMode;
  private final boolean hasCompleteMode;

  public AggregateModeInfo(
      Seq<AggregateMode> uniqueModes,
      boolean hasPartialMode,
      boolean hasPartialMergeMode,
      boolean hasFinalMode,
      boolean hasCompleteMode) {
    this.uniqueModes = uniqueModes;
    this.hasPartialMode = hasPartialMode;
    this.hasPartialMergeMode = hasPartialMergeMode;
    this.hasFinalMode = hasFinalMode;
    this.hasCompleteMode = hasCompleteMode;
  }

  public static AggregateModeInfo from(Seq<AggregateMode> uniqueModes) {
    return new AggregateModeInfo(
        uniqueModes,
        uniqueModes.contains(Partial$.MODULE$),
        uniqueModes.contains(PartialMerge$.MODULE$),
        uniqueModes.contains(Final$.MODULE$),
        uniqueModes.contains(Complete$.MODULE$));
  }

  public Seq<AggregateMode> uniqueModes() {
    return uniqueModes;
  }

  public boolean hasPartialMode() {
    return hasPartialMode;
  }

  public boolean hasPartialMergeMode() {
    return hasPartialMergeMode;
  }

  public boolean hasFinalMode() {
    return hasFinalMode;
  }

  public boolean hasCompleteMode() {
    return hasCompleteMode;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AggregateModeInfo)) {
      return false;
    }
    AggregateModeInfo that = (AggregateModeInfo) other;
    return hasPartialMode == that.hasPartialMode
        && hasPartialMergeMode == that.hasPartialMergeMode
        && hasFinalMode == that.hasFinalMode
        && hasCompleteMode == that.hasCompleteMode
        && Objects.equals(uniqueModes, that.uniqueModes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        uniqueModes, hasPartialMode, hasPartialMergeMode, hasFinalMode, hasCompleteMode);
  }

  @Override
  public String toString() {
    return "AggregateModeInfo(" + uniqueModes + "," + hasPartialMode + ","
        + hasPartialMergeMode + "," + hasFinalMode + "," + hasCompleteMode + ")";
  }
}
