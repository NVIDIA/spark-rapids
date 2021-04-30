# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
from datetime import date
import pyspark.sql.functions as f
from spark_session import with_cpu_session, with_gpu_session, is_spark_300
from join_test import create_df
from generate_expr_test import four_op_df
from marks import incompat, allow_non_gpu, ignore_order

enableVectorizedConf = [{"spark.sql.inMemoryColumnarStorage.enableVectorizedReader" : "true"},
                        {"spark.sql.inMemoryColumnarStorage.enableVectorizedReader" : "false"}]

@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@allow_non_gpu('CollectLimitExec')
def test_passing_gpuExpr_as_Expr(enableVectorizedConf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, string_gen)
            .select(f.col("a")).na.drop()
            .groupBy(f.col("a"))
            .agg(f.count(f.col("a")).alias("count_a"))
            .orderBy(f.col("count_a").desc(), f.col("a"))
            .cache()
            .limit(50), enableVectorizedConf)

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
           pytest.param(FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), marks=[incompat]), pytest.param(DoubleGen(special_cases=double_special_cases), marks=[incompat]), BooleanGen(), DateGen(), TimestampGen()]

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@ignore_order
def test_cache_join(data_gen, join_type, enableVectorizedConf):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        cached = left.join(right, left.a == right.r_a, join_type).cache()
        cached.count() # populates cache
        return cached

    assert_gpu_and_cpu_are_equal_collect(do_join, conf = enableVectorizedConf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
# We are OK running everything on CPU until we complete 'https://github.com/NVIDIA/spark-rapids/issues/360'
# because we have an explicit check in our code that disallows InMemoryTableScan to have anything other than
# AttributeReference
@allow_non_gpu(any=True)
@ignore_order
def test_cached_join_filter(data_gen, join_type, enableVectorizedConf):
    data = data_gen
    if is_spark_300() and data.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        cached = left.join(right, left.a == right.r_a, join_type).cache()
        cached.count() #populates the cache
        return cached.filter("a is not null")

    assert_gpu_and_cpu_are_equal_collect(do_join, conf = enableVectorizedConf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_broadcast_hash_join(data_gen, join_type, enableVectorizedConf):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        cached = left.join(right.hint("broadcast"), left.a == right.r_a, join_type).cache()
        cached.count()
        return cached

    assert_gpu_and_cpu_are_equal_collect(do_join, conf = enableVectorizedConf)

shuffled_conf = {"spark.sql.autoBroadcastJoinThreshold": "160",
                 "spark.sql.join.preferSortMergeJoin": "false",
                 "spark.sql.shuffle.partitions": "2",
                 "spark.rapids.sql.exec.BroadcastNestedLoopJoinExec": "true"}

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_shuffled_hash_join(data_gen, join_type, enableVectorizedConf):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 500)
        cached = left.join(right, left.a == right.r_a, join_type).cache()
        cached.count()
        return cached
    assert_gpu_and_cpu_are_equal_collect(do_join, conf = enableVectorizedConf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_broadcast_nested_loop_join(data_gen, join_type, enableVectorizedConf):
    enableVectorizedConf.update({'spark.rapids.sql.exec.BroadcastNestedLoopJoinExec':
                                            'true'})
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        cached = left.crossJoin(right.hint("broadcast")).cache()
        cached.count()
        return cached
    assert_gpu_and_cpu_are_equal_collect(do_join, conf = enableVectorizedConf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@ignore_order
def test_cache_expand_exec(data_gen, enableVectorizedConf):
    def op_df(spark, length=2048, seed=0):
        cached = gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length, seed=seed).cache()
        cached.count() # populate the cache
        return cached.rollup(f.col("a"), f.col("b")).agg(f.col("b"))

    assert_gpu_and_cpu_are_equal_collect(op_df, conf = enableVectorizedConf)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('enableVectorizedConf', enableVectorizedConf, ids=idfn)
@allow_non_gpu('CollectLimitExec')
def test_cache_partial_load(data_gen, enableVectorizedConf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, data_gen, string_gen)
            .select(f.col("a"), f.col("b"))
            .cache()
            .limit(50).select(f.col("b")), enableVectorizedConf
    )

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

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('ts_write', ['TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS'])
@pytest.mark.parametrize('enableVectorized', ['true', 'false'], ids=idfn)
@ignore_order
def test_cache_columnar(spark_tmp_path, data_gen, enableVectorized, ts_write):
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
          'spark.sql.inMemoryColumnarStorage.enableVectorizedReader' : enableVectorized,
          'spark.sql.parquet.outputTimestampType': ts_write}

    assert_gpu_and_cpu_are_equal_collect(read_parquet_cached(data_path_gpu), conf)

@pytest.mark.parametrize('enableVectorized', ['false', 'true'], ids=idfn)
@pytest.mark.parametrize('func', [with_cpu_session, with_gpu_session])
@allow_non_gpu("ProjectExec", "Alias", "Literal", "DateAddInterval", "MakeInterval", "Cast",
               "ExtractIntervalYears", "Year", "Month", "Second", "ExtractIntervalMonths",
               "ExtractIntervalSeconds", "SecondWithFraction", "ColumnarToRowExec")
@pytest.mark.parametrize('selectExpr', [("NULL as d", "d"),
                                        # In order to compare the results, since pyspark doesn't
                                        # know how to parse interval types, we need to "extract"
                                        # values from the interval. NOTE, "extract" is a misnomer
                                        # because we are actually coverting the value to the
                                        # requested time precision, which is not actually extraction
                                        # i.e. 'extract(years from d) will actually convert the
                                        # entire interval to year
                                        ("make_interval(y,m,w,d,h,min,s) as d", ["cast(extract(years from d) as long)", "extract(months from d)", "extract(seconds from d)"])])
def test_cache_additional_types(enableVectorized, func, selectExpr):
    def holder(cache):
        selectExprDF, selectExprProject = selectExpr
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
            duration_df = df.selectExpr(selectExprDF)
            if (cache):
                duration_df.cache()
            duration_df.count
            df_1 = duration_df.selectExpr(selectExprProject)
            return df_1.collect()
        return helper

    cached_result = func(holder(True),
       conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
       'spark.sql.legacy.parquet.datetimeRebaseModeInRead' : 'CORRECTED',
       'spark.sql.inMemoryColumnarStorage.enableVectorizedReader' : enableVectorized})
    reg_result = func(holder(False),
       conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
       'spark.sql.legacy.parquet.datetimeRebaseModeInRead' : 'CORRECTED',
       'spark.sql.inMemoryColumnarStorage.enableVectorizedReader' : enableVectorized})

    #NOTE: we aren't comparing cpu and gpu results, we are comparing the cached and non-cached results.
    assert_equal(reg_result, cached_result)
