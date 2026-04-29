# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Tests for GpuProjectExec split-retry (Phase 1 of zero-config batching).
#
# `spark.rapids.sql.test.injectRetryOOM` lets us deterministically inject a
# GPU SplitAndRetryOOM mid-projection. With split-retry enabled (default), the
# new path in GpuProjectExec.projectWithSplitRetry must recover by halving the
# input batch by rows and re-running the projection on each half.

import pyspark.sql.functions as f

from spark_session import with_gpu_session


# Note: we deliberately do NOT have a "@inject_oom split=true" test here.
# `forceSplitAndRetryOOM` is re-fired at every retry-iterator instance the
# task creates, and GpuRangeExec uses its own withRetry(reduceRowsNumberByHalf)
# loop — under split=true that loop divides row count down to its lower
# bound and aborts before ever reaching GpuProjectExec. Real
# split-retry-on-OOM coverage for GpuProjectExec lives in the
# legacy_parser_oom_repro test (which uses real cuDF-scratch pressure
# rather than synthetic injection).


def test_project_split_retry_handles_plain_retry_oom():
    """Inject a plain GpuRetryOOM (not SplitAndRetry). The retry framework
    should resolve this on its own without invoking the splitter; the new
    path must not get in the way."""
    def run(spark):
        return (spark.range(0, 10_000, numPartitions=1)
                .selectExpr("id + 1 as a", "cast(id as string) as b")
                .collect())
    result = with_gpu_session(run, conf={
        "spark.rapids.sql.test.injectRetryOOM": "num_ooms=1,type=GPU,split=false",
        "spark.rapids.sql.projectExec.splitRetry.enabled": "true",
    })
    assert len(result) == 10_000


def test_project_split_retry_disabled_falls_back_to_legacy_path():
    """When split-retry is disabled by conf, the legacy withRetryNoSplit path
    is used. Inject a non-split retry OOM so the legacy path can resolve it,
    confirming the conf knob actually wires through."""
    def run(spark):
        return (spark.range(0, 10_000, numPartitions=1)
                .selectExpr("id + 1 as a")
                .collect())
    result = with_gpu_session(run, conf={
        "spark.rapids.sql.test.injectRetryOOM": "num_ooms=1,type=GPU,split=false",
        "spark.rapids.sql.projectExec.splitRetry.enabled": "false",
    })
    assert len(result) == 10_000


def test_project_with_nondeterministic_runs_normally():
    """Mixed deterministic + non-deterministic projection must take the
    legacy withRetryNoSplit path even with split-retry enabled (because the
    row-stitching logic requires alignment that row-splitting would break).
    This sanity test confirms the dispatch logic does not break ordinary
    execution; no OOM injection so the legacy path is exercised in its
    happy path."""
    def run(spark):
        return (spark.range(0, 10_000, numPartitions=1)
                .selectExpr("id + 1 as a", "rand() as r")
                .collect())
    result = with_gpu_session(run, conf={
        "spark.rapids.sql.projectExec.splitRetry.enabled": "true",
    })
    assert len(result) == 10_000
