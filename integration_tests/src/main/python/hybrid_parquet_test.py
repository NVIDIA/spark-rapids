# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import pytest

from asserts import *
from data_gen import *
from marks import *
from parquet_test import rebase_write_corrected_conf
from spark_session import *
import pyspark.sql.functions as f

"""
Hybrid Scan unsupported types:
1. Decimal with negative scale is NOT supported
2. Decimal128 inside nested types is NOT supported
3. BinaryType is NOT supported
4. MapType wrapped by NestedType (Struct of Map/Array of Map/Map of Map) is NOT fully supported
"""
parquet_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
     string_gen, boolean_gen, date_gen,
     TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)), ArrayGen(byte_gen),
     ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
     ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
     ArrayGen(decimal_gen_64bit),
     ArrayGen(ArrayGen(byte_gen)),
     StructGen([['child0', ArrayGen(byte_gen)],
                ['child1', byte_gen],
                ['child2', float_gen],
                ['child3', decimal_gen_64bit]]),
     ArrayGen(StructGen([['child0', string_gen],
                         ['child1', double_gen],
                         ['child2', int_gen]]))
     ],
    [MapGen(f(nullable=False), f()) for f in [
        BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen,
        lambda nullable=True: TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc), nullable=nullable)]
     ],
    [simple_string_to_string_map_gen,
     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
     ],
    decimal_gens,
]

parquet_gens_fallback_lists = [
    # Decimal128 inside nested types is NOT supported
    [MapGen(StringGen(pattern='key_[0-9]', nullable=False), decimal_gen_128bit)],
    # BinaryType is NOT supported
    [BinaryGen()],
    # MapType wrapped by NestedType is NOT fully supported
    [MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)],
    [ArrayGen(simple_string_to_string_map_gen)],
    [ArrayGen(ArrayGen(simple_string_to_string_map_gen))],
    [ArrayGen(StructGen([["c0", simple_string_to_string_map_gen]]))],
    [StructGen([["c0", simple_string_to_string_map_gen]])],
    [StructGen([["c0", ArrayGen(simple_string_to_string_map_gen)]])],
    [StructGen([["c0", StructGen([["cc0", simple_string_to_string_map_gen]])]])],
    [],
]


@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('gen_rows', [20, 100, 512, 1024, 4096], ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_round_trip(spark_tmp_path, parquet_gens, gen_rows):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=gen_rows).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })


# Creating scenarios in which CoalesceConverter will coalesce several input batches by adjusting
# reader_batch_size and coalesced_batch_size, tests if the CoalesceConverter functions correctly
# when coalescing is needed.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_batch_size', [512, 1024, 2048], ids=idfn)
@pytest.mark.parametrize('coalesced_batch_size', [1 << 25, 1 << 27], ids=idfn)
@pytest.mark.parametrize('gen_rows', [8192, 10000], ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_round_trip_multiple_batches(spark_tmp_path,
                                                         parquet_gens,
                                                         reader_batch_size,
                                                         coalesced_batch_size,
                                                         gen_rows):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=gen_rows).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.gluten.sql.columnar.maxBatchSize': reader_batch_size,
            'spark.rapids.sql.batchSizeBytes': coalesced_batch_size,
        })


# HybridScan shall NOT be enabled over unsupported data types. Instead, fallbacks to GpuScan.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_fallback_lists, ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_fallback_to_gpu(spark_tmp_path, parquet_gens):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    # check the fallback over empty schema(`SELECT COUNT(1)`) within the same case
    if len(parquet_gens) == 0:
        with_cpu_session(
            lambda spark: gen_df(spark, [('a', int_gen)], length=512).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
        read_fn = lambda spark: spark.read.parquet(data_path).selectExpr('count(1)')
    else:
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
        with_cpu_session(
            lambda spark: gen_df(spark, gen_list, length=512).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
        read_fn = lambda spark: spark.read.parquet(data_path)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        read_fn,
        exist_classes='GpuFileSourceScanExec',
        non_exist_classes='HybridFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })


# Test the preloading feature with extreme tiny target batch size (and source batch size), creating
# scenarios in which multiple target batches will be generated.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('coalesced_batch_size', [1 << 17, 1 << 20], ids=idfn)
@pytest.mark.parametrize('preloaded_batches', [1, 3, 5], ids=idfn)
@hybrid_test
def test_hybrid_parquet_preloading(spark_tmp_path, coalesced_batch_size, preloaded_batches):
    parquet_gens = parquet_gens_list[0].copy()
    parquet_gens.extend(parquet_gens_list[1])
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=4096).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.gluten.sql.columnar.maxBatchSize': 16,
            'spark.rapids.sql.batchSizeBytes': coalesced_batch_size,
            'spark.rapids.sql.hybrid.parquet.numPreloadedBatches': preloaded_batches,
        })


filter_split_conf = {
    'spark.sql.sources.useV1SourceList': 'parquet',
    'spark.rapids.sql.hybrid.parquet.enabled': 'true',
    'spark.rapids.sql.parquet.pushDownFiltersToHybrid': 'CPU',
    'spark.rapids.sql.expression.Ascii': False,
    'spark.rapids.sql.expression.StartsWith': False,
    'spark.rapids.sql.hybrid.whitelistExprs': 'StartsWith'
}

def check_filter_pushdown(plan, pushed_exprs, not_pushed_exprs):
    plan = str(plan)
    filter_part, scan_part = plan.split("Scan parquet")
    for expr in pushed_exprs:
        assert expr in scan_part
    for expr in not_pushed_exprs:
        assert expr in filter_part

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_gpu(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def add(a, b):
        return a + b
    my_udf = f.pandas_udf(add, returnType=LongType())
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', long_gen)]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    conf = filter_split_conf.copy()
    conf.update({
        'spark.rapids.sql.parquet.pushDownFiltersToHybrid': 'GPU'
    })
    # filter conditions should remain on the GPU
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(my_udf(f.col('a'), f.col('a')) > 0)._jdf.queryExecution().executedPlan(),
        conf=conf)
    check_filter_pushdown(plan, pushed_exprs=[], not_pushed_exprs=['pythonUDF'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(my_udf(f.col('a'), f.col('a')) > 0),
        conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_cpu(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', StringGen(pattern='[0-9]{1,5}'))]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    # filter conditions should be pushed down to the CPU, so the ascii will not fall back to CPU in the FilterExec
    # use f.startWith because sql function startswith is from spark 3.5.0
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a").startswith('1') & (f.ascii(f.col("a")) >= 50) & (f.col("a") < '1000'))._jdf.queryExecution().executedPlan(),
        conf=filter_split_conf)
    check_filter_pushdown(plan, pushed_exprs=['ascii', 'StartsWith', 'isnotnull'], not_pushed_exprs=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a").startswith('1') & (f.ascii(f.col("a")) >= 50) & (f.col("a") < '1000')),
        conf=filter_split_conf)

@allow_non_gpu('FilterExec', 'BatchEvalPythonExec', 'PythonUDF')
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_pushdown_unsupported(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', StringGen(pattern='[0-9]{1,5}'))]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    # UDf is not supported by GPU, so it should fallback to CPU in the FilterExec    
    def udf_fallback(s):
        return f'udf_{s}'
    
    with_cpu_session(lambda spark: spark.udf.register("udf_fallback", udf_fallback))
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter("ascii(a) >= 50 and udf_fallback(a) = 'udf_100'")._jdf.queryExecution().executedPlan(),
        conf=filter_split_conf)
    check_filter_pushdown(plan, pushed_exprs=['ascii', 'isnotnull'], not_pushed_exprs=['udf_fallback'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter("ascii(a) >= 50 and udf_fallback(a) = 'udf_100'"),
        conf=filter_split_conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
@allow_non_gpu(*non_utc_allow)
def test_hybrid_parquet_filter_pushdown_timestamp(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)))]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    # Timestamp is not fully supported in Hybrid Filter, so it should remain on the GPU
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a") > f.lit(datetime(2024, 1, 1, tzinfo=timezone.utc)))._jdf.queryExecution().executedPlan(),
        conf=filter_split_conf)
    check_filter_pushdown(plan, pushed_exprs=[], not_pushed_exprs=['isnotnull', '>'])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(f.col("a") > f.lit(datetime(2024, 1, 1, tzinfo=timezone.utc))),
        conf=filter_split_conf)
