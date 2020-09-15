# Copyright (c) 2020, NVIDIA CORPORATION.
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

def test_passing_gpuExpr_as_Expr():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, string_gen)
            .select(f.col("a")).na.drop()
            .groupBy(f.col("a"))
            .agg(f.count(f.col("a")).alias("count_a"))
            .orderBy(f.col("count_a").desc(), f.col("a"))
            .cache()
            .limit(50)
    )
# creating special cases to just remove -0.0 because of https://github.com/NVIDIA/spark-rapids/issues/84
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

all_gen_filters = [(StringGen(), "rlike(a, '^(?=.{1,5}$).*')"),
                            (ByteGen(), "a < 100"),
                            (ShortGen(), "a < 100"),
                            (IntegerGen(), "a < 1000"),
                            (LongGen(), "a < 1000"),
                            (BooleanGen(), "a == false"),
                            (DateGen(), "a > '1/21/2012'"),
                            (TimestampGen(), "a > '1/21/2012'"),
                            pytest.param((FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), "a < 1000"), marks=[incompat]),
                            pytest.param((DoubleGen(special_cases=double_special_cases),"a < 1000"), marks=[incompat])]

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_join(data_gen, join_type):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        cached = left.join(right, left.a == right.r_a, join_type).cache()
        cached.count() # populates cache
        return cached

    assert_gpu_and_cpu_are_equal_collect(do_join)

@pytest.mark.parametrize('data_gen', all_gen_filters, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
# We are OK running everything on CPU until we complete 'https://github.com/NVIDIA/spark-rapids/issues/360'
# because we have an explicit check in our code that disallows InMemoryTableScan to have anything other than
# AttributeReference
@allow_non_gpu(any=True)
@ignore_order
def test_cached_join_filter(data_gen, join_type):
    data, filter = data_gen
    if is_spark_300() and data.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        cached = left.join(right, left.a == right.r_a, join_type).cache()
        cached.count() #populates the cache
        return cached.filter(filter)

    assert_gpu_and_cpu_are_equal_collect(do_join)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_broadcast_hash_join(data_gen, join_type):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        cached = left.join(right.hint("broadcast"), left.a == right.r_a, join_type).cache()
        cached.count()
        return cached

    assert_gpu_and_cpu_are_equal_collect(do_join)

shuffled_conf = {"spark.sql.autoBroadcastJoinThreshold": "160",
                 "spark.sql.join.preferSortMergeJoin": "false",
                 "spark.sql.shuffle.partitions": "2",
                 "spark.rapids.sql.exec.BroadcastNestedLoopJoinExec": "true"}

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_shuffled_hash_join(data_gen, join_type):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")

    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 500)
        cached = left.join(right, left.a == right.r_a, join_type).cache()
        cached.count()
        return cached
    assert_gpu_and_cpu_are_equal_collect(do_join)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@ignore_order
def test_cache_broadcast_nested_loop_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        cached = left.crossJoin(right.hint("broadcast")).cache()
        cached.count()
        return cached

    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.exec.BroadcastNestedLoopJoinExec': 'true'})

all_gen_restricting_dates = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           pytest.param(FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), marks=[incompat]),
           pytest.param(DoubleGen(special_cases=double_special_cases), marks=[incompat]),
           BooleanGen(),
           # due to backward compatibility we are avoiding writing dates prior to 1582-10-15
           # For more detail please look at SPARK-31404
           # This issue is tracked by https://github.com/NVIDIA/spark-rapids/issues/133 in the plugin
           DateGen(start=date(1582, 10, 15)),
           TimestampGen()]

@pytest.mark.parametrize('data_gen', all_gen_restricting_dates, ids=idfn)
@allow_non_gpu('DataWritingCommandExec')
def test_cache_posexplode_makearray(spark_tmp_path, data_gen):
    if is_spark_300() and data_gen.data_type == BooleanType():
        pytest.xfail("https://issues.apache.org/jira/browse/SPARK-32672")
    data_path_cpu = spark_tmp_path + '/PARQUET_DATA_CPU'
    data_path_gpu = spark_tmp_path + '/PARQUET_DATA_GPU'
    def write_posExplode(data_path):
        def posExplode(spark):
            cached = four_op_df(spark, data_gen).selectExpr('posexplode(array(b, c, d))', 'a').cache()
            cached.count()
            cached.write.parquet(data_path)
            spark.read.parquet(data_path)
        return posExplode
    from_cpu = with_cpu_session(write_posExplode(data_path_cpu))
    from_gpu = with_gpu_session(write_posExplode(data_path_gpu))
    assert_equal(from_cpu, from_gpu)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@ignore_order
def test_cache_expand_exec(data_gen):
    def op_df(spark, length=2048, seed=0):
        cached = gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length, seed=seed).cache()
        cached.count() # populate the cache
        return cached.rollup(f.col("a"), f.col("b")).agg(f.count(f.col("b")))

    assert_gpu_and_cpu_are_equal_collect(op_df)

