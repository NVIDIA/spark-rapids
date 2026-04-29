# Copyright (c) 2026, NVIDIA CORPORATION.
#
# End-to-end reproducer for the customer GPU OOM in the legacy timestamp
# parser. The interesting GPU work is the projection of
# `from_unixtime(... + unix_timestamp(ts_str, fmt))` under
# `spark.sql.legacy.timeParserPolicy=LEGACY`, which routes to
# GpuToTimestamp.parseStringAsTimestampWithLegacyParserPolicy and into cuDF
# stringReplaceWithBackrefs / matches_re. cuDF allocates a per-row scratch
# buffer (~600 B/row in our measurement) that the spark-rapids estimator
# cannot see, so before split-retry the projection would OOM the pool.
#
# With Phase 1 (GpuProjectExec.projectWithSplitRetry, default-on), the OOM is
# caught at the project layer; the input batch is halved by rows and the
# projection is re-run on each half until each piece fits.
#
# To actually reproduce the OOM-pressure path, the test must be run with:
#   - small RMM pool (PYSP_TEST_spark_rapids_memory_gpu_allocSize=8g)
#   - reader emitting a single large batch (large parquet row group +
#     spark.rapids.sql.reader.chunked=false +
#     spark.rapids.sql.reader.batchSizeBytes=3g)
#   - TEST_PARALLEL=1 so the pool isn't sliced across xdist workers
# Without those, the GPU reader chunks the input and no single batch is large
# enough to trigger the cuDF-scratch OOM.

import pytest
import pyspark.sql.functions as f

from spark_session import with_cpu_session, with_gpu_session


@pytest.mark.parametrize("rows", [100_000_000, 200_000_000], ids=["100M", "200M"])
def test_legacy_parser_oom_repro(spark_tmp_path, rows):
    data_path = spark_tmp_path + '/LEGACY_PARSER_OOM'

    # Force a single huge parquet row group so the GPU reader sees a single
    # big batch when chunked-reader is disabled at run time.
    write_conf = {
        'spark.sql.parquet.block.size': str(4 * 1024 * 1024 * 1024),
        'parquet.block.size': str(4 * 1024 * 1024 * 1024),
    }
    # The CASE keeps Catalyst from folding ts_str into table metadata; in
    # practice every row holds the same valid timestamp string.
    with_cpu_session(lambda spark: spark.range(0, rows, numPartitions=1)
        .selectExpr(
            "id as offset_long",
            "case when id >= 0 then '2024-06-15 12:34:56' "
            "else '2024-06-15 12:34:55' end as ts_str")
        .write.mode('overwrite').parquet(data_path), conf=write_conf)

    def run(spark):
        # Aggregate to a single scalar so the projected `t` column is fully
        # materialized on the GPU but we don't ship a billion rows back.
        spark.read.parquet(data_path).selectExpr(
            "from_unixtime(offset_long + "
            "unix_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss')) as t"
        ).agg(f.sum(f.length('t'))).collect()

    conf = {
        "spark.sql.legacy.timeParserPolicy": "LEGACY",
        # Required to let UnixTimestamp/FromUnixTime with LEGACY format reach
        # the GPU path (parseStringAsTimestampWithLegacyParserPolicy). This
        # is the knob the customer has turned on in production.
        "spark.rapids.sql.incompatibleDateFormats.enabled": "true",
        # Phase 1 split-retry is default-on; left explicit here to make the
        # test's expectation visible and to allow flipping it for debugging.
        "spark.rapids.sql.projectExec.splitRetry.enabled": "true",
    }
    with_gpu_session(run, conf=conf)
