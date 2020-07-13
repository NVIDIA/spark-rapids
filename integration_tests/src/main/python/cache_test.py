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
import pyspark.sql.functions as f
from spark_session import with_cpu_session, with_gpu_session
from join_test import create_df
from generate_expr_test import four_op_df
from marks import incompat, allow_non_gpu, ignore_order
from pyspark.sql.functions import asc

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
#creating special cases to just remove -0.0
double_special_cases = [
    DoubleGen._make_from(1, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen._make_from(0, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen._make_from(1, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen._make_from(0, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
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
def test_cache_join(data_gen, join_type):
    if data_gen.data_type == BooleanType():
        pytest.xfail("https://github.com/NVIDIA/spark-rapids/issues/350")
    from pyspark.sql.functions import asc
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort = [asc("a"), asc("b")]
    else:
        sort = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    from_cpu = cached_df_cpu.sort(sort).collect()
    cached_df_gpu = with_gpu_session(do_join)
    from_gpu = cached_df_gpu.sort(sort).collect()
    assert_equal(from_cpu, from_gpu)


@pytest.mark.parametrize('data_gen', all_gen_filters, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
# We are OK running everything on CPU until we complete 'git issue'
# because we have an explicit check in our code that disallows InMemoryTableScan to have anything other than
# AttributeReference
@allow_non_gpu(any=True)
def test_cached_join_filter(data_gen, join_type):
    from pyspark.sql.functions import asc
    data, filter = data_gen
    if data.data_type == BooleanType():
        pytest.xfail("https://github.com/NVIDIA/spark-rapids/issues/350")
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort_columns = [asc("a"), asc("b")]
    else:
        sort_columns = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    join_from_cpu = cached_df_cpu.sort(sort_columns).collect()
    filter_from_cpu = cached_df_cpu.filter(filter).sort(sort_columns).collect()

    cached_df_gpu = with_gpu_session(do_join)
    join_from_gpu = cached_df_gpu.sort(sort_columns).collect()
    filter_from_gpu = cached_df_gpu.filter(filter).sort(sort_columns).collect()

    assert_equal(join_from_cpu, join_from_gpu)
    assert_equal(filter_from_cpu, filter_from_gpu)

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_cache_broadcast_hash_join(data_gen, join_type):
    if data_gen.data_type == BooleanType():
        pytest.xfail("https://github.com/NVIDIA/spark-rapids/issues/350")
    from pyspark.sql.functions import asc
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right.hint("broadcast"), left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort = [asc("a"), asc("b")]
    else:
        sort = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    from_cpu = cached_df_cpu.sort(sort).collect()
    cached_df_gpu = with_gpu_session(do_join)
    from_gpu = cached_df_gpu.sort(sort).collect()
    assert_equal(from_cpu, from_gpu)


shuffled_conf = {"spark.sql.autoBroadcastJoinThreshold": "160",
                 "spark.sql.join.preferSortMergeJoin": "false",
                 "spark.sql.shuffle.partitions": "2",
                 "spark.rapids.sql.exec.BroadcastNestedLoopJoinExec": "true"}

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_cache_shuffled_hash_join(data_gen, join_type):
    if data_gen.data_type == BooleanType():
        pytest.xfail("https://github.com/NVIDIA/spark-rapids/issues/350")
    from pyspark.sql.functions import asc
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join, shuffled_conf)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort = [asc("a"), asc("b")]
    else:
        sort = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    from_cpu = cached_df_cpu.sort(sort).collect()
    cached_df_gpu = with_gpu_session(do_join, shuffled_conf)
    from_gpu = cached_df_gpu.sort(sort).collect()
    assert_equal(from_cpu, from_gpu)


@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.skip(reason="this isn't calling the broadcastNestedLoopJoin, come back to it")
def test_cache_broadcast_nested_loop_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 50)
        return left.join(right, left.a == left.a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join, shuffled_conf)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort = [asc("a"), asc("b")]
    else:
        sort = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    from_cpu = cached_df_cpu.sort(sort).collect()
    cached_df_gpu = with_gpu_session(do_join, shuffled_conf)
    from_gpu = cached_df_gpu.sort(sort).collect()
    assert_equal(from_cpu, from_gpu)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
#This is a copy of a test from generate_expr_test.py except for the fact that we are caching the df
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@allow_non_gpu('InMemoryTableScanExec', 'DataWritingCommandExec')
def test_cache_posexplode_makearray(spark_tmp_path, data_gen):
    if data_gen.data_type == BooleanType():
        pytest.xfail("https://github.com/NVIDIA/spark-rapids/issues/350")
    data_path_cpu = spark_tmp_path + '/PARQUET_DATA_CPU'
    def posExplode(spark):
        return four_op_df(spark, data_gen).selectExpr('posexplode(array(b, c, d))', 'a').cache()
    cached_df_cpu = with_cpu_session(posExplode)
    cached_df_cpu.write.parquet(data_path_cpu)
    from_cpu = with_cpu_session(lambda spark: spark.read.parquet(data_path_cpu))

    data_path_gpu = spark_tmp_path + '/PARQUET_DATA_GPU'
    cached_df_gpu = with_gpu_session(posExplode)
    cached_df_gpu.write.parquet(data_path_gpu)
    from_gpu = with_gpu_session(lambda spark: spark.read.parquet(data_path_gpu))

    sort_col = [asc("pos"), asc("col"), asc("a")]
    assert_equal(cached_df_cpu.sort(sort_col).collect(), cached_df_gpu.sort(sort_col).collect())
    assert_equal(from_cpu.sort(sort_col).collect(), from_gpu.sort(sort_col).collect())

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_cache_expand_exec(data_gen):
    def op_df(spark, length=2048, seed=0):
        return gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length, seed=seed)

    cached_df_cpu = with_cpu_session(op_df).cache()
    from_cpu = with_cpu_session(lambda spark: cached_df_cpu.rollup(f.col("a"), f.col("b")).agg(f.count(f.col("b"))))

    cached_df_gpu = with_gpu_session(op_df).cache()
    from_gpu = with_cpu_session(lambda spark: cached_df_gpu.rollup(f.col("a"), f.col("b")).agg(f.count(f.col("b"))))

    sort_col = [asc("a"), asc("b"), asc("count(b)")]
    assert_equal(cached_df_cpu.sort(asc("a"), asc("b")).collect(), cached_df_gpu.sort(asc("a"), asc("b")).collect())
    assert_equal(from_cpu.sort(sort_col).collect(), from_gpu.sort(sort_col).collect())


