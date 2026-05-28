# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Integration tests for the private optimizer rules.

These tests exercise four rules in the RAPIDS private optimizer plugin that
are dormant in default public IT because their gate confs are default-off
(or, for OptimizeSubquerySharedScanRule, the matching pattern doesn't appear
in any current public IT):

  - AggPushdownRule                     (gate: aggPushdownEnabled)
  - DecomposeStddevPop                  (gate: decomposeStddevPop.enabled)
  - OptimizeSkewedBHJJoinRule           (gate: adaptive.skewJoin.broadcast.enabled)
  - OptimizeSubquerySharedScanRule      (default-on; needs ParquetRelation pattern)

All four are verified REACHABLE by single-query probes. See the tracking issue
NVIDIA/spark-rapids#14900 (parent epic #14899) for probe verdicts.
"""

import pytest

from pyspark.sql.functions import broadcast

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal
from marks import ignore_order, inject_oom, approximate_float
from spark_session import with_cpu_session


@pytest.mark.private_optimizer
@ignore_order(local=True)
def test_agg_pushdown_rule():
    """Trigger AggPushdownRule via fact-dim inner join + groupBy + sum/count.

    The rule pushes a top-level aggregate through a fact-dim join when the
    fact/dim heuristic + agg-fn allow-list are satisfied. Without the conf
    flip the rule short-circuits at the apply()-level gate check; flipping
    it from a test conf={} produces real apply() body execution.

    Fact (20000 rows) / Dim (100 rows) gives a row-count ratio well over the
    rule's 0.15 fact/dim threshold. autoBroadcastJoinThreshold=-1 ensures
    the join is shuffle (not broadcast), so the rule's isLikelyBroadcastJoin
    guard does NOT short-circuit.

    Contributes to NVIDIA/spark-rapids#14900.
    """
    conf = {
        "spark.rapids.sql.private.enabled": "true",
        "spark.rapids.sql.optimizer.aggPushdownEnabled": "true",
        # Force shuffle hash join so the agg pushdown predicate is in scope
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    }

    def fn(spark):
        # Use spark.range() + selectExpr to produce a join that the rule's
        # fact/dim heuristic recognizes. The fact table (20000) is well
        # over 0.15x the dim (100) — i.e. fact dominates by row count.
        fact = (spark.range(20000)
                .selectExpr("id AS fact_id",
                            "id % 100 AS dim_fk",
                            "(id % 7 + 1) AS val"))
        dim = (spark.range(100)
               .selectExpr("id AS dim_id",
                           "concat('d_', id) AS name"))
        joined = fact.join(dim, fact.dim_fk == dim.dim_id, "inner")
        # Multiple agg fns from the allow-list {Min, Max, First, Last, Sum, Count}
        return joined.groupBy("name").agg({"val": "sum", "fact_id": "count"})

    assert_gpu_and_cpu_are_equal_collect(fn, conf=conf)


@pytest.mark.private_optimizer
@ignore_order(local=True)
@approximate_float
def test_decompose_stddev_pop():
    """Trigger DecomposeStddevPop via aggregate with stddev_pop.

    DecomposeStddevPop rewrites stddev_pop(x) into a sequence of sum/count/
    avg operations. The rewrite is mathematically equivalent to the direct
    formula EXCEPT for floating-point catastrophic cancellation when the
    mean is small relative to the stddev. To stay inside parity tolerance
    we use a small-range integer-derived double so the variance is on the
    same order of magnitude as the mean squared.

    Contributes to NVIDIA/spark-rapids#14900.
    """
    conf = {
        "spark.rapids.sql.private.enabled": "true",
        "spark.rapids.sql.optimizer.decomposeStddevPop.enabled": "true",
        # DecomposeStddevPop rewrites stddev_pop(x) into sum(x), sum(x*x),
        # count(x). Without variableFloatAgg, the post-rewrite Sum<double>
        # falls back to CPU, sandwiching CPU HashAggregate between Gpu
        # children and tripping the strict columnar-plan check in IT.
        "spark.rapids.sql.variableFloatAgg.enabled": "true",
    }

    def fn(spark):
        # Data range chosen so mean ~50 and variance is on the same order
        # as mean^2 — avoids the small-mean catastrophic-cancellation
        # regime documented in DecomposeStddevPop.scala (catastrophic
        # cancellation when mean is small relative to stddev).
        df = (spark.range(10000)
              .selectExpr("CAST(id % 100 AS DOUBLE) AS x",
                          "id % 10 AS g"))
        return df.groupBy("g").agg({"x": "stddev_pop"})

    assert_gpu_and_cpu_are_equal_collect(fn, conf=conf)


@pytest.mark.private_optimizer
@inject_oom
def test_optimize_skewed_bhj_join():
    """Trigger OptimizeSkewedBHJJoinRule via AQE-enabled BHJ over skewed key.

    The rule only fires when:
      1. AQE is enabled.
      2. spark.rapids.sql.adaptive.skewJoin.broadcast.enabled=true.
      3. The plan has a BroadcastHashJoinExec whose non-broadcast side is a
         materialized ShuffleQueryStage with at least one partition larger
         than the skew threshold.

    We lower skewedPartitionThresholdInBytes to 1MB and skewedPartitionFactor
    to 1 so a tiny IT fact (~95k rows skewed on key=1) can produce a
    "skewed" partition.

    Contributes to NVIDIA/spark-rapids#14900.
    """
    conf = {
        "spark.rapids.sql.private.enabled": "true",
        "spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "true",
        # AQE knobs to make the rule's runtime predicate fire on small data
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1048576",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "1",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1048576",
        # Force BHJ for a small build side
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
        "spark.sql.shuffle.partitions": "8",
    }

    def fn(spark):
        # Dim ~100 rows (will broadcast)
        dim = spark.range(100).selectExpr("id AS k", "concat('d_', id) AS name")
        # Fact ~95k rows on key=1 (skewed) plus 98 rows spread across 2..99
        skewed = spark.range(95000).selectExpr("CAST(1 AS LONG) AS k", "id AS v")
        normal = spark.range(2, 100).selectExpr("id AS k", "(id * 7) AS v")
        fact = skewed.unionByName(normal)
        return fact.join(broadcast(dim), on="k", how="inner")

    # Row-count parity rather than full-result parity: the join produces
    # 95K+ rows and the rule body fires at materialization time. Row-count
    # check confirms the BHJ + ShuffleQueryStage actually run without
    # paying the cost of collecting every row.
    assert_gpu_and_cpu_row_counts_equal(fn, conf=conf)


@pytest.mark.private_optimizer
@ignore_order(local=True)
def test_optimize_subquery_shared_scan(spark_tmp_path):
    """Trigger OptimizeSubquerySharedScanRule via 3 scalar subqueries on same parquet.

    The rule combines multiple ScalarSubquery -> Aggregate(no-grouping,
    1-agg) -> Project -> Filter -> LogicalRelation subtrees against the
    same LogicalRelation into a single shared scan. It requires a
    LogicalRelation source (parquet / temp view), not a Range.

    Contributes to NVIDIA/spark-rapids#14900.
    """
    data_path = spark_tmp_path + "/private_subquery_shared_scan"

    def write_table(spark):
        (spark.range(1000)
              .selectExpr("id", "id % 100 AS g")
              .write.mode("overwrite").parquet(data_path))

    with_cpu_session(write_table)

    # SELECT the 3 scalar subqueries from a 1-row inline source so we
    # avoid the outer LIMIT 1 + CollectLimitExec sandwich (CollectLimitExec
    # is default-off in RAPIDS, and a CPU CollectLimit over a GpuProject
    # would trip the strict columnar-plan check in IT).
    sql_text = (
        "SELECT "
        "  (SELECT max(id) FROM t WHERE g = 1) AS a, "
        "  (SELECT min(id) FROM t WHERE g = 2) AS b, "
        "  (SELECT count(*) FROM t WHERE g = 3) AS c"
    )
    conf = {
        "spark.rapids.sql.private.enabled": "true",
        # Conf is default-on; set explicitly for documentation
        "spark.rapids.sql.optimizer.optimizeScalarSubquery": "true",
        # AQE off so the rule runs in the logical optimizer (not AQE re-opt)
        "spark.sql.adaptive.enabled": "false",
    }

    def fn(spark):
        spark.read.parquet(data_path).createOrReplaceTempView("t")
        return spark.sql(sql_text)

    assert_gpu_and_cpu_are_equal_collect(fn, conf=conf)
