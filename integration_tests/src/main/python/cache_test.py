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

all_gen_no_nulls_filters = [(StringGen(nullable=False), "rlike(a, '^(?=.{1,5}$).*')"),
                            (ByteGen(nullable=False), "a < 100"),
                            (ShortGen(nullable=False), "a < 100"),
                            (IntegerGen(nullable=False), "a < 1000"),
                            (LongGen(nullable=False), "a < 1000"),
                            (BooleanGen(nullable=False), "a == false"),
                            (DateGen(nullable=False), "a > '1/21/2012'"),
                            (TimestampGen(nullable=False), "a > '1/21/2012'"),
                            pytest.param((FloatGen(nullable=False, special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), "a < 1000"), marks=[incompat]),
                            pytest.param((DoubleGen(nullable=False, special_cases=double_special_cases),"a < 1000"), marks=[incompat])]

conf={"spark.rapids.sql.explain":"ALL"}
@pytest.mark.xfail(reason="TODO: github issue")
@pytest.mark.parametrize('data_gen', all_gen_no_nulls_filters, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_cache_join(data_gen, join_type):
    from pyspark.sql.functions import asc
    data, filter = data_gen # we are not using the filter
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join, conf)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort = [asc("a"), asc("b")]
    else:
        sort = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    from_cpu = cached_df_cpu.sort(sort).collect()
    print('COLLECTED\n{}'.format(from_cpu))
    cached_df_gpu = with_gpu_session(do_join, conf)
    from_gpu = cached_df_gpu.sort(sort).collect()
    print('COLLECTED\n{}'.format(from_gpu))
    assert_equal(from_cpu, from_gpu)


@pytest.mark.xfail(reason="TODO: github issue")
@pytest.mark.parametrize('data_gen', all_gen_no_nulls_filters, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(any=True)
def test_cached_join_filter(data_gen, join_type):
    from pyspark.sql.functions import asc
    data, filter = data_gen
    def do_join(spark):
        left, right = create_df(spark, data, 500, 500)
        return left.join(right, left.a == right.r_a, join_type).cache()
    cached_df_cpu = with_cpu_session(do_join, conf)
    if (join_type == 'LeftAnti' or join_type == 'LeftSemi'):
        sort_columns = [asc("a"), asc("b")]
    else:
        sort_columns = [asc("a"), asc("b"), asc("r_a"), asc("r_b")]

    join_from_cpu = cached_df_cpu.sort(sort_columns).collect()
    filter_from_cpu = cached_df_cpu.filter(filter).sort(sort_columns).collect()

    cached_df_gpu = with_gpu_session(do_join, conf)
    join_from_gpu = cached_df_gpu.sort(sort_columns).collect()
    filter_from_gpu = cached_df_gpu.filter(filter).sort(sort_columns).collect()

    assert_equal(join_from_cpu, join_from_gpu)
    assert_equal(filter_from_cpu, filter_from_gpu)
