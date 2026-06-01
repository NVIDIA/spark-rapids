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

"""Integration tests for four RAPIDS private optimizer rules.

Each rule is semantics-preserving but default-off in public IT. Comparing a
same-conf CPU run against a same-conf GPU run cannot tell whether the rule
actually fired, so every test here does two things:

  1. Compares an OFF-rule CPU baseline against an ON-rule GPU run. Because the
     rules preserve semantics, OFF-CPU == ON-GPU proves both data correctness
     and GPU parity while keeping the CPU reference uncontaminated by the rule.
  2. Asserts a per-rule plan marker that appears only when the rule fires, so
     the test FAILS if the conf flip is a no-op (wrong conf, wrong query shape,
     or private jar not loaded).
"""

import pytest

from asserts import assert_equal, _sort_locally
from conftest import get_float_check
from marks import approximate_float
from spark_session import with_cpu_session, with_gpu_session

PRIVATE_OPTIMIZER_BASE_CONF = {
    "spark.rapids.sql.private.enabled": "true",
}


def private_optimizer_conf(extra_conf):
    conf = dict(PRIVATE_OPTIMIZER_BASE_CONF)
    conf.update(extra_conf)
    return conf


def _collect_and_plan(spark, fn, physical):
    """Run fn, returning (collected rows, plan-string). Uses the physical plan
    when the rule effect is only visible after AQE, else the optimized plan."""
    df = fn(spark)
    rows = df.collect()
    qe = df._jdf.queryExecution()
    plan = qe.executedPlan().toString() if physical else qe.optimizedPlan().toString()
    return rows, plan


def _assert_rule_fires(fn, on_conf, off_conf, marker, physical=False, float_compare=False):
    """OFF-rule CPU baseline vs ON-rule GPU run, plus a plan marker check.

    marker must be present in the ON plan and absent from the OFF plan; results
    of the OFF-CPU and ON-GPU runs must match.
    """
    cpu_rows, off_plan = with_cpu_session(
        lambda s: _collect_and_plan(s, fn, physical), conf=off_conf)
    gpu_rows, on_plan = with_gpu_session(
        lambda s: _collect_and_plan(s, fn, physical), conf=on_conf)

    assert marker in on_plan, \
        "rule did not fire: marker '%s' absent with rule ON\n%s" % (marker, on_plan)
    assert marker not in off_plan, \
        "marker '%s' present with rule OFF, not a valid discriminator\n%s" % (marker, off_plan)

    _sort_locally(cpu_rows, gpu_rows)
    if float_compare:
        float_check = get_float_check()
        assert len(cpu_rows) == len(gpu_rows)
        for cr, gr in zip(cpu_rows, gpu_rows):
            assert len(cr) == len(gr)
            for c, g in zip(cr, gr):
                if isinstance(c, float):
                    assert float_check(c, g), "off-CPU %s vs on-GPU %s" % (c, g)
                else:
                    assert c == g, "off-CPU %s vs on-GPU %s" % (c, g)
    else:
        assert_equal(cpu_rows, gpu_rows)


@pytest.mark.private_optimizer
def test_agg_pushdown_rule(spark_tmp_path):
    """AggPushdownRule pushes a partial aggregate onto the fact side of a
    fact-dim shuffle join. Fires only on a row-counted relation (parquet, not
    Range) where the fact dominates the 0.15 fact/dim ratio and the join is not
    a broadcast join. Marker: a partial 'sum(sum(' over the pushed aggregate."""
    fact_path = spark_tmp_path + "/agg_pushdown_fact"
    dim_path = spark_tmp_path + "/agg_pushdown_dim"
    with_cpu_session(lambda s: s.range(20000)
                     .selectExpr("id AS fact_id", "id % 100 AS dim_fk", "(id % 7 + 1) AS val")
                     .write.mode("overwrite").parquet(fact_path))
    with_cpu_session(lambda s: s.range(100)
                     .selectExpr("id AS dim_id", "concat('d_', id) AS name")
                     .write.mode("overwrite").parquet(dim_path))

    def fn(spark):
        fact = spark.read.parquet(fact_path)
        dim = spark.read.parquet(dim_path)
        joined = fact.join(dim, fact.dim_fk == dim.dim_id, "inner")
        return joined.groupBy("name").agg({"val": "sum", "fact_id": "count"})

    # autoBroadcastJoinThreshold low (positive) keeps the join a shuffle join.
    base = {"spark.sql.autoBroadcastJoinThreshold": "200"}
    on = private_optimizer_conf({**base, "spark.rapids.sql.optimizer.aggPushdownEnabled": "true"})
    off = private_optimizer_conf({**base, "spark.rapids.sql.optimizer.aggPushdownEnabled": "false"})
    _assert_rule_fires(fn, on, off, marker="sum(sum(")


@pytest.mark.private_optimizer
@approximate_float
def test_decompose_stddev_pop():
    """DecomposeStddevPop rewrites stddev_pop(x) into sum(x), sum(x*x),
    count(x) aliased as _stddev_*. variableFloatAgg keeps the rewritten
    Sum<double> on the GPU. Marker: a '_stddev_' intermediate alias."""
    def fn(spark):
        df = spark.range(10000).selectExpr("CAST(id % 100 AS DOUBLE) AS x", "id % 10 AS g")
        return df.groupBy("g").agg({"x": "stddev_pop"})

    base = {"spark.rapids.sql.variableFloatAgg.enabled": "true"}
    on = private_optimizer_conf({**base, "spark.rapids.sql.optimizer.decomposeStddevPop.enabled": "true"})
    off = private_optimizer_conf({**base, "spark.rapids.sql.optimizer.decomposeStddevPop.enabled": "false"})
    _assert_rule_fires(fn, on, off, marker="_stddev_", float_compare=True)


@pytest.mark.private_optimizer
def test_optimize_subquery_shared_scan(spark_tmp_path):
    """OptimizeSubquerySharedScanRule merges scalar subqueries that aggregate
    (no grouping, single agg fn) over the same LogicalRelation into one shared
    scan. Conf is default-on, so the OFF baseline sets it false explicitly.
    Marker: the combined 'generated_agg_list' named struct."""
    data_path = spark_tmp_path + "/subquery_shared_scan"
    with_cpu_session(lambda s: s.range(1000).selectExpr("id", "id % 100 AS g")
                     .write.mode("overwrite").parquet(data_path))
    sql_text = (
        "SELECT "
        "  (SELECT max(id) FROM t WHERE g = 1) AS a, "
        "  (SELECT min(id) FROM t WHERE g = 2) AS b, "
        "  (SELECT count(*) FROM t WHERE g = 3) AS c"
    )

    def fn(spark):
        spark.read.parquet(data_path).createOrReplaceTempView("t")
        return spark.sql(sql_text)

    # AQE off so the rule runs in the logical optimizer rather than AQE re-opt.
    base = {"spark.sql.adaptive.enabled": "false"}
    on = private_optimizer_conf({**base, "spark.rapids.sql.optimizer.optimizeScalarSubquery": "true"})
    off = private_optimizer_conf({**base, "spark.rapids.sql.optimizer.optimizeScalarSubquery": "false"})
    _assert_rule_fires(fn, on, off, marker="generated_agg_list")


@pytest.mark.private_optimizer
def test_optimize_skewed_bhj_join(spark_tmp_path):
    """OptimizeSkewedBHJJoinRule splits a skewed partition on the streamed side
    of an AQE broadcast hash join. Needs a runtime broadcast (static
    autoBroadcastJoinThreshold=-1, adaptive.autoBroadcastJoinThreshold=10m) so
    the streamed side is a materialized shuffle stage, plus small skew
    thresholds. Marker: the shuffle reader is 'coalesced and skewed'. Validated
    with a small global aggregate over the materialized skewed join."""
    conf_extra = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "10m",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
        "spark.sql.shuffle.partitions": "100",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "800",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "800",
        "spark.sql.adaptive.localShuffleReader.enabled": "false",
    }

    def fn(spark):
        spark.range(0, 2000, 1, 10).selectExpr(
            "CASE WHEN id < 1000 THEN 249 ELSE id END AS key2", "id AS value2"
        ).createOrReplaceTempView("skewData2")
        spark.range(0, 1000, 1, 10).selectExpr(
            "CASE WHEN id < 250 THEN 249 WHEN id >= 750 THEN 1000 ELSE id END AS key1", "id AS value1"
        ).createOrReplaceTempView("skewData1")
        return spark.sql(
            "SELECT count(*) AS cnt, min(value2) AS mn, max(value2) AS mx, sum(value1) AS sm "
            "FROM skewData1 JOIN skewData2 ON key1 = key2")

    on = private_optimizer_conf({**conf_extra, "spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "true"})
    off = private_optimizer_conf({**conf_extra, "spark.rapids.sql.adaptive.skewJoin.broadcast.enabled": "false"})
    _assert_rule_fires(fn, on, off, marker="coalesced and skewed", physical=True)
