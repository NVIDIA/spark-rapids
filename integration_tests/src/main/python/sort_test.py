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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_310

orderable_not_null_gen = [ByteGen(nullable=False), ShortGen(nullable=False), IntegerGen(nullable=False),
        LongGen(nullable=False), FloatGen(nullable=False), DoubleGen(nullable=False), BooleanGen(nullable=False),
        TimestampGen(nullable=False), DateGen(nullable=False), StringGen(nullable=False), DecimalGen(nullable=False),
        DecimalGen(precision=7, scale=-3, nullable=False), DecimalGen(precision=7, scale=3, nullable=False),
        DecimalGen(precision=7, scale=7, nullable=False), DecimalGen(precision=12, scale=2, nullable=False)]

@pytest.mark.parametrize('data_gen', orderable_gens + orderable_not_null_gen, ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_orderby(data_gen, order):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order),
            conf = allow_negative_scale_of_decimal_conf)

# SPARK CPU itself has issue with negative scale for take ordered and project
orderable_without_neg_decimal = [n for n in (orderable_gens + orderable_not_null_gen) if not (isinstance(n, DecimalGen) and n.scale < 0)]
@pytest.mark.parametrize('data_gen', orderable_without_neg_decimal, ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_orderby_with_limit(data_gen, order):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order).limit(100))

@pytest.mark.parametrize('data_gen', orderable_gens + orderable_not_null_gen, ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_sort_in_part(data_gen, order):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).sortWithinPartitions(order),
            conf = allow_negative_scale_of_decimal_conf)

orderable_gens_sort = [byte_gen, short_gen, int_gen, long_gen,
        pytest.param(float_gen, marks=pytest.mark.xfail(condition=is_before_spark_310(),
            reason='Spark has -0.0 < 0.0 before Spark 3.1')),
        pytest.param(double_gen, marks=pytest.mark.xfail(condition=is_before_spark_310(),
            reason='Spark has -0.0 < 0.0 before Spark 3.1')),
        boolean_gen, timestamp_gen, date_gen, string_gen, null_gen] + decimal_gens
@pytest.mark.parametrize('data_gen', orderable_gens_sort, ids=idfn)
def test_multi_orderby(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).orderBy(f.col('a'), f.col('b').desc()),
            conf = allow_negative_scale_of_decimal_conf)

# SPARK CPU itself has issue with negative scale for take ordered and project
orderable_gens_sort_without_neg_decimal = [n for n in orderable_gens_sort if not (isinstance(n, DecimalGen) and n.scale < 0)]
@pytest.mark.parametrize('data_gen', orderable_gens_sort_without_neg_decimal, ids=idfn)
def test_multi_orderby_with_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).orderBy(f.col('a'), f.col('b').desc()).limit(100))

# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [byte_gen, long_gen, float_gen], ids=idfn)
def test_orderby_with_processing(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # avoid ambiguity in the order by statement for floating point by including a as a backup ordering column
            lambda spark : unary_op_df(spark, data_gen).orderBy(f.lit(100) - f.col('a'), f.col('a')))

# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [byte_gen, long_gen, float_gen], ids=idfn)
def test_orderby_with_processing_and_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # avoid ambiguity in the order by statement for floating point by including a as a backup ordering column
            lambda spark : unary_op_df(spark, data_gen).orderBy(f.lit(100) - f.col('a'), f.col('a')).limit(100))

# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [byte_gen, long_gen, float_gen], ids=idfn)
def test_single_orderby_with_skew(data_gen):
    # When doing range partitioning the upstream data is sampled to try and get the bounds for cutoffs.
    # If the data comes back with skewed partitions then those partitions will be resampled for more data.
    # This is to try and trigger it to happen.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen)\
                    .selectExpr('a', 'random(1) > 0.5 as b')\
                    .repartition(f.col('b'))\
                    .orderBy(f.col('a'))\
                    .selectExpr('a'),
            conf = allow_negative_scale_of_decimal_conf)
