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
from marks import ignore_order

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

def test_cache_table():
    spark.sql("CACHE TABLE range5 AS SELECT * FROM range(5)")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("select * from range5").limit(5))

gen=[StringGen(nullable=False), DateGen(nullable=False), TimestampGen(nullable=False)]
@ignore_order
@pytest.mark.parametrize('data_gen', gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_cached_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join)
    from_cpu = cached_df_cpu.collect()
    cached_df_gpu = with_gpu_session(do_join)
    from_gpu = cached_df_gpu.collect()
    assert_equal(from_cpu, from_gpu)

gen_filter=[(StringGen(nullable=False), "rlike(a, '^(?=.{1,5}$).*')"), (DateGen(nullable=False), "a > '1/21/2012'"), (TimestampGen(nullable=False), "a > '1/21/2012'")]
@ignore_order
@pytest.mark.parametrize('data_gen', gen_filter, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_cached_join_filter(data_gen, join_type):
    data, filter = data_gen
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join)
    join_from_cpu = cached_df_cpu.collect()
    filter_from_cpu = cached_df_cpu.filter(filter)

    cached_df_gpu = with_gpu_session(do_join)
    join_from_gpu = cached_df_gpu.collect()
    filter_from_gpu = cached_df_gpu.filter(filter)

    assert_equal(join_from_cpu, join_from_gpu)

    assert_equal(filter_from_cpu, filter_from_gpu)



