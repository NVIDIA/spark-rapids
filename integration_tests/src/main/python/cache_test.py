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
from marks import incompat, allow_non_gpu
from join_test import all_gen_no_nulls

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

conf={"spark.rapids.sql.explain":"ALL"}
@pytest.mark.xfail(reason="TODO: github issue")
@pytest.mark.parametrize('data_gen', all_gen_no_nulls, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_cache_join(data_gen, join_type):
    from pyspark.sql.functions import asc
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join, conf)
    from_cpu = debug_df(cached_df_cpu.sort(asc("a")).collect())
    cached_df_gpu = with_gpu_session(do_join, conf)
    from_gpu = debug_df(cached_df_gpu.sort(asc("a")).collect())
    assert_equal(from_cpu, from_gpu)

all_gen_no_nulls_filters = [(StringGen(nullable=False), "rlike(a, '^(?=.{1,5}$).*')"),
                            (ByteGen(nullable=False), "a < 100"),
                            (ShortGen(nullable=False), "a < 100"),
                            (IntegerGen(nullable=False), "a < 1000"),
                            (LongGen(nullable=False), "a < 1000"),
                            (BooleanGen(nullable=False), "a == false"),
                            (DateGen(nullable=False), "a > '1/21/2012'"),
                            (TimestampGen(nullable=False), "a > '1/21/2012'"),
                            pytest.param((FloatGen(nullable=False), "a < 1000"), marks=[incompat]),
                            pytest.param((DoubleGen(nullable=False),"a < 1000"), marks=[incompat])]

@pytest.mark.xfail(reason="TODO: github issue")
@pytest.mark.parametrize('data_gen', all_gen_no_nulls_filters, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu('InMemoryTableScanExec', 'RDDScanExec')
def test_cached_join_filter(data_gen, join_type):
    from pyspark.sql.functions import asc
    data, filter = data_gen
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join, conf)
    join_from_cpu = cached_df_cpu.sort(asc("a")).collect()
    filter_from_cpu = cached_df_cpu.filter(filter).collect()

    cached_df_gpu = with_gpu_session(do_join, conf)
    join_from_gpu = cached_df_gpu.sort(asc("a")).collect()
    filter_from_gpu = cached_df_gpu.filter(filter).collect()

    assert_equal(join_from_cpu, join_from_gpu)

    assert_equal(filter_from_cpu, filter_from_gpu)
