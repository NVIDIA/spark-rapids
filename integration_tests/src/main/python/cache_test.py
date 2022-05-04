# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_equal
from data_gen import *
import pyspark.sql.functions as f
from spark_session import with_cpu_session, with_gpu_session, is_before_spark_330
from join_test import create_df
from marks import incompat, allow_non_gpu, ignore_order
import pyspark.mllib.linalg as mllib
import pyspark.ml.linalg as ml

enable_vectorized_confs = [{"spark.sql.inMemoryColumnarStorage.enableVectorizedReader": "true"},
                           {"spark.sql.inMemoryColumnarStorage.enableVectorizedReader": "false"}]

_cache_decimal_gens = [decimal_gen_32bit, decimal_gen_64bit, decimal_gen_128bit]
_cache_single_array_gens_no_null = [ArrayGen(gen) for gen in all_basic_gens_no_null + _cache_decimal_gens]

decimal_struct_gen= StructGen([['child0', sub_gen] for ind, sub_gen in enumerate(_cache_decimal_gens)])

@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu('CollectLimitExec')
@pytest.mark.xfail(condition=not is_before_spark_330(), reason='The tests are failing due to changes in parquet reader. More details can be found at https://github.com/NVIDIA/spark-rapids/issues/5357')
def test_passing_gpuExpr_as_Expr(enable_vectorized_conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, string_gen)
            .select(f.col("a")).na.drop()
            .groupBy(f.col("a"))
            .agg(f.count(f.col("a")).alias("count_a"))
            .orderBy(f.col("count_a").desc(), f.col("a"))
            .cache()
            .limit(50), enable_vectorized_conf)

# creating special cases to just remove -0.0 because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min Spark version we can drop this
double_special_cases = [
    DoubleGen.make_from(1, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(0, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(1, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(0, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    0.0, 1.0, -1.0, float('inf'), float('-inf'), float('nan'),
    NEG_DOUBLE_NAN_MAX_VALUE
]

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           pytest.param(FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), marks=[incompat]),
           pytest.param(DoubleGen(special_cases=double_special_cases), marks=[incompat]),
           BooleanGen(), DateGen(), TimestampGen()] + _cache_decimal_gens

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@ignore_order
def test_cache_join(data_gen, enable_vectorized_conf):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        cached = left.join(right, left.a == right.r_a, 'Inner').cache()
        cached.count() # populates cache
        return cached
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=enable_vectorized_conf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
# We are OK running everything on CPU until we complete 'https://github.com/NVIDIA/spark-rapids/issues/360'
# because we have an explicit check in our code that disallows InMemoryTableScan to have anything other than
# AttributeReference
@allow_non_gpu(any=True)
@ignore_order
def test_cached_join_filter(data_gen, enable_vectorized_conf):
    data = data_gen
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        cached = left.join(right, left.a == right.r_a, 'Inner').cache()
        cached.count() #populates the cache
        return cached.filter("a is not null")
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=enable_vectorized_conf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@ignore_order
def test_cache_expand_exec(data_gen, enable_vectorized_conf):
    def op_df(spark, length=2048, seed=0):
        cached = gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length, seed=seed).cache()
        cached.count() # populate the cache
        return cached.rollup(f.col("a"), f.col("b")).agg(f.col("b"))

    assert_gpu_and_cpu_are_equal_collect(op_df, conf=enable_vectorized_conf)

@pytest.mark.parametrize('data_gen', [all_basic_struct_gen, StructGen([['child0', StructGen([['child1', byte_gen]])]]),
                                      ArrayGen(
                                          StructGen([['child0', StringGen()],
                                                     ['child1',
                                                      StructGen([['child0', IntegerGen()]])]])),
                                      decimal_struct_gen] + _cache_single_array_gens_no_null + all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu('CollectLimitExec')
def test_cache_partial_load(data_gen, enable_vectorized_conf):
    def partial_return(col):
        def partial_return_cache(spark):
            return two_col_df(spark, data_gen, string_gen).select(f.col("a"), f.col("b")).cache().limit(50).select(col)
        return partial_return_cache
    assert_gpu_and_cpu_are_equal_collect(partial_return(f.col("a")), conf=enable_vectorized_conf)
    assert_gpu_and_cpu_are_equal_collect(partial_return(f.col("b")), conf=enable_vectorized_conf)

@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@allow_non_gpu('CollectLimitExec')
@ignore_order
def test_cache_reverse_order(enable_vectorized_conf):
    col0 = StructGen([['child0', StructGen([['child1', byte_gen]])]])
    col1 = StructGen([['child0', byte_gen]])
    def partial_return():
        def partial_return_cache(spark):
            return two_col_df(spark, col0, col1).select(f.col("a"), f.col("b")).cache().select(f.col("b"), f.col("a"))
        return partial_return_cache
    assert_gpu_and_cpu_are_equal_collect(partial_return(), conf=enable_vectorized_conf)

@allow_non_gpu('CollectLimitExec')
def test_cache_diff_req_order(spark_tmp_path):
    def n_fold(spark):
        data_path_cpu = spark_tmp_path + '/PARQUET_DATA/{}/{}'
        data = spark.range(100).selectExpr(
            "cast(id as double) as col0",
            "cast(id - 100 as double) as col1",
            "cast(id * 2 as double) as col2",
            "rand(100) as col3",
            "rand(200) as col4")

        num_buckets = 10
        with_random = data.selectExpr("*", "cast(rand(0) * {} as int) as BUCKET".format(num_buckets)).cache()
        for test_bucket in range(0, num_buckets):
            with_random.filter(with_random.BUCKET == test_bucket).drop("BUCKET") \
                .write.parquet(data_path_cpu.format("test_data", test_bucket))
            with_random.filter(with_random.BUCKET != test_bucket).drop("BUCKET") \
                .write.parquet(data_path_cpu.format("train_data", test_bucket))

    with_cpu_session(n_fold)

# This test doesn't allow negative scale for Decimals as ` df.write.mode('overwrite').parquet(data_path)`
# writes parquet which doesn't allow negative decimals
@pytest.mark.parametrize('data_gen', [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                      ArrayGen(
                                          StructGen([['child0', StringGen()],
                                                     ['child1',
                                                      StructGen([['child0', IntegerGen()]])]])),
                                     pytest.param(FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), marks=[incompat]),
                                     pytest.param(DoubleGen(special_cases=double_special_cases), marks=[incompat]),
                                     BooleanGen(), DateGen(), TimestampGen(), decimal_gen_32bit, decimal_gen_64bit,
                                     decimal_gen_128bit] + _cache_single_array_gens_no_null, ids=idfn)
@pytest.mark.parametrize('ts_write', ['TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS'])
@pytest.mark.parametrize('enable_vectorized', ['true', 'false'], ids=idfn)
@ignore_order
@allow_non_gpu("SortExec", "ShuffleExchangeExec", "RangePartitioning")
def test_cache_columnar(spark_tmp_path, data_gen, enable_vectorized, ts_write):
    data_path_gpu = spark_tmp_path + '/PARQUET_DATA'
    def read_parquet_cached(data_path):
        def write_read_parquet_cached(spark):
            df = unary_op_df(spark, data_gen)
            df.write.mode('overwrite').parquet(data_path)
            cached = spark.read.parquet(data_path).cache()
            cached.count()
            return cached.select(f.col("a"))
        return write_read_parquet_cached
    # rapids-spark doesn't support LEGACY read for parquet
    conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
          'spark.sql.legacy.parquet.datetimeRebaseModeInRead' : 'CORRECTED',
          # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
          'spark.sql.legacy.parquet.int96RebaseModeInWrite' : 'CORRECTED',
          'spark.sql.legacy.parquet.int96RebaseModeInRead' : 'CORRECTED',
          'spark.sql.inMemoryColumnarStorage.enableVectorizedReader' : enable_vectorized,
          'spark.sql.parquet.outputTimestampType': ts_write}

    assert_gpu_and_cpu_are_equal_collect(read_parquet_cached(data_path_gpu), conf)

@pytest.mark.parametrize('data_gen', [all_basic_struct_gen, StructGen([['child0', StructGen([['child1', byte_gen]])]]),
                                      decimal_struct_gen,
                                      ArrayGen(
                                          StructGen([['child0', StringGen()],
                                                     ['child1',
                                                      StructGen([['child0', IntegerGen()]])]]))] + _cache_single_array_gens_no_null + all_gen, ids=idfn)
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
def test_cache_cpu_gpu_mixed(data_gen, enable_vectorized_conf):
    def func(spark):
        df = unary_op_df(spark, data_gen)
        df.cache().count()
        enabled = spark.conf.get("spark.rapids.sql.enabled")
        spark.conf.set("spark.rapids.sql.enabled", not enabled)

        return df.selectExpr("a")

    assert_gpu_and_cpu_are_equal_collect(func, conf=enable_vectorized_conf)

@pytest.mark.parametrize('enable_vectorized', ['false', 'true'], ids=idfn)
@pytest.mark.parametrize('with_x_session', [with_gpu_session, with_cpu_session])
@allow_non_gpu("ProjectExec", "Alias", "Literal", "DateAddInterval", "MakeInterval", "Cast",
               "ExtractIntervalYears", "Year", "Month", "Second", "ExtractIntervalMonths",
               "ExtractIntervalSeconds", "SecondWithFraction", "ColumnarToRowExec")
@pytest.mark.parametrize('select_expr', [("NULL as d", "d"),
                                        # In order to compare the results, since pyspark doesn't
                                        # know how to parse interval types, we need to "extract"
                                        # values from the interval. NOTE, "extract" is a misnomer
                                        # because we are actually coverting the value to the
                                        # requested time precision, which is not actually extraction
                                        # i.e. 'extract(years from d) will actually convert the
                                        # entire interval to year
                                        ("make_interval(y,m,w,d,h,min,s) as d", ["cast(extract(years from d) as long)", "extract(months from d)", "extract(seconds from d)"])])
def test_cache_additional_types(enable_vectorized, with_x_session, select_expr):
    def with_cache(cache):
        select_expr_df, select_expr_project = select_expr

        def helper(spark):
            # the goal is to just get a DF of CalendarIntervalType, therefore limiting the values
            # so when we do get the individual parts of the interval, it doesn't overflow
            df = gen_df(spark, StructGen([('m', IntegerGen(min_val=-1000, max_val=1000, nullable=False)),
                                          ('y', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('w', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('h', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('min', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('d', IntegerGen(min_val=-10000, max_val=10000, nullable=False)),
                                          ('s', IntegerGen(min_val=-10000, max_val=10000, nullable=False))],
                                         nullable=False), seed=1)
            duration_df = df.selectExpr(select_expr_df)
            if (cache):
                duration_df.cache()
            duration_df.count
            df_1 = duration_df.selectExpr(select_expr_project)
            return df_1.collect()

        return helper

    cached_result = with_x_session(with_cache(True),
                                   conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
                                         'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
                                         'spark.sql.inMemoryColumnarStorage.enableVectorizedReader': enable_vectorized})
    reg_result = with_x_session(with_cache(False),
                                conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
                                      'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
                                      'spark.sql.inMemoryColumnarStorage.enableVectorizedReader': enable_vectorized})

    # NOTE: we aren't comparing cpu and gpu results, we are comparing the cached and non-cached results.
    assert_equal(reg_result, cached_result)

def function_to_test_on_cached_df(with_x_session, func, data_gen, test_conf):
    def with_cache(cached):
        def helper(spark):
            df = unary_op_df(spark, data_gen)
            if cached:
                df.cache().count()
            return func(df)
        return helper

    reg_result = with_x_session(with_cache(False), test_conf)
    cached_result = with_x_session(with_cache(True), test_conf)

    assert_equal(reg_result, cached_result)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('with_x_session', [with_gpu_session, with_cpu_session])
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@pytest.mark.parametrize('batch_size', [{"spark.rapids.sql.batchSizeBytes": "100"}, {}], ids=idfn)
@ignore_order
def test_cache_count(data_gen, with_x_session, enable_vectorized_conf, batch_size):
    test_conf = copy_and_update(enable_vectorized_conf, batch_size)
    function_to_test_on_cached_df(with_x_session, lambda df: df.count(), data_gen, test_conf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('with_x_session', [with_cpu_session, with_gpu_session])
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@pytest.mark.parametrize('batch_size', [{"spark.rapids.sql.batchSizeBytes": "100"}, {}], ids=idfn)
@ignore_order
# This tests the cached and uncached values returned by collect on the CPU and GPU.
# When running on the GPU with the DefaultCachedBatchSerializer, to project the results Spark adds a ColumnarToRowExec
# to be able to show the results which will cause this test to throw an exception as it's not on the GPU so we have to
# add that case to the `allowed` list. As of now there is no way for us to limit the scope of allow_non_gpu based on a
# condition therefore we must allow it in all cases
@allow_non_gpu('ColumnarToRowExec')
def test_cache_multi_batch(data_gen, with_x_session, enable_vectorized_conf, batch_size):
    test_conf = copy_and_update(enable_vectorized_conf, batch_size)
    function_to_test_on_cached_df(with_x_session, lambda df: df.collect(), data_gen, test_conf)

@pytest.mark.parametrize('data_gen', all_basic_map_gens + _cache_single_array_gens_no_null, ids=idfn)
@pytest.mark.parametrize('enable_vectorized', enable_vectorized_confs, ids=idfn)
def test_cache_map_and_array(data_gen, enable_vectorized):
    def helper(spark):
        df = gen_df(spark, StructGen([['a', data_gen]], nullable=False))
        df.persist()
        return df.selectExpr("a")

    assert_gpu_and_cpu_are_equal_collect(helper)

def test_cache_udt():
    def fun(spark):
        df = spark.sparkContext.parallelize([
            (mllib.DenseVector([1, ]), ml.DenseVector([1, ])),
            (mllib.SparseVector(1, [0, ], [1, ]), ml.SparseVector(1, [0, ], [1, ]))
        ]).toDF(["mllib_v", "ml_v"])
        df.cache().count()
        return df.selectExpr("mllib_v", "ml_v").collect()
    cpu_result = with_cpu_session(fun)
    gpu_result = with_gpu_session(fun)
    # assert_gpu_and_cpu_are_equal_collect method doesn't handle UDT so we just write a single
    # statement here to compare
    assert cpu_result == gpu_result, "not equal"

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Spark3.3.0')
@pytest.mark.parametrize('enable_vectorized_conf', enable_vectorized_confs, ids=idfn)
@ignore_order(local=True)
def test_cache_daytimeinterval(enable_vectorized_conf):
    def test_func(spark):
        df = two_col_df(spark, DayTimeIntervalGen(), int_gen)
        df.cache().count()
        return df.selectExpr("b", "a")
    assert_gpu_and_cpu_are_equal_collect(test_func, enable_vectorized_conf)
