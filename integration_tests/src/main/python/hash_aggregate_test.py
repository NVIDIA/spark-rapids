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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from pyspark.sql.types import *
from marks import *
import pyspark.sql.functions as f
from spark_session import with_cpu_session

_no_nans_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                       'spark.rapids.sql.hasNans': 'false',
                       'spark.rapids.sql.castStringToFloat.enabled': 'true'
                      }


# The input lists or schemas that are used by StructGen.

# grouping longs with nulls
_longs_with_nulls = [('a', LongGen()), ('b', IntegerGen()), ('c', LongGen())]
# grouping longs with no nulls
_longs_with_no_nulls = [
    ('a', LongGen(nullable=False)),
    ('b', IntegerGen(nullable=False)),
    ('c', LongGen(nullable=False))]
# grouping longs with nulls present
_grpkey_longs_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping doubles with nulls present
_grpkey_dbls_with_nulls = [
    ('a', RepeatSeqGen(DoubleGen(nullable=(True, 10.0), special_cases=[]), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping floats with nulls present
_grpkey_floats_with_nulls = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0), special_cases=[]), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping strings with nulls present
_grpkey_strings_with_nulls = [
    ('a', RepeatSeqGen(StringGen(pattern='[0-9]{0,30}'), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]

# grouping floats with other columns containing nans and nulls
_grpkey_floats_with_nulls_and_nans = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0)), length= 20)),
    ('b', FloatGen(nullable=(True, 10.0), special_cases=[(float('nan'), 10.0)])),
    ('c', LongGen())]
# Schema for xfail cases
struct_gens_xfail = [
    _grpkey_floats_with_nulls_and_nans
]

# List of schemas with no NaNs
_init_list_no_nans = [
    _longs_with_nulls,
    _longs_with_no_nulls,
    _grpkey_longs_with_nulls,
    _grpkey_dbls_with_nulls,
    _grpkey_floats_with_nulls,
    _grpkey_strings_with_nulls]

# List of schemas with NaNs included
_init_list_with_nans_and_no_nans = [
    _longs_with_nulls,
    _longs_with_no_nulls,
    _grpkey_longs_with_nulls,
    _grpkey_dbls_with_nulls,
    _grpkey_floats_with_nulls,
    _grpkey_strings_with_nulls,
    _grpkey_floats_with_nulls_and_nans]


def get_struct_gens(init_list=_init_list_no_nans, marked_params=[]):
    """
    A method to build the structGen inputs along with their passed in markers to allow testing
    specific params with their relevant markers.
    :arg init_list list of schemas to be tested, defaults to _init_list_no_nans from above.
    :arg marked_params A list of tuples of (schema, list of pytest markers)
    Look at params_markers_for_avg_sum as an example.
    """
    list = init_list.copy()
    for index in range(0, len(list)):
        for test_case,marks in marked_params:
            if list[index] == test_case:
                list[index] = pytest.param(list[index], marks=marks)
    return list


params_markers_for_avg_sum=[
    (_grpkey_strings_with_nulls, [pytest.mark.incompat, pytest.mark.approximate_float]),
    (_grpkey_dbls_with_nulls, [pytest.mark.incompat, pytest.mark.approximate_float]),
    (_grpkey_floats_with_nulls, [pytest.mark.incompat, pytest.mark.approximate_float]),
    (_grpkey_floats_with_nulls_and_nans, [pytest.mark.incompat, pytest.mark.approximate_float])]


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(
    marked_params=params_markers_for_avg_sum), ids=idfn)
def test_hash_grpby_sum(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.sum('b')),
        conf=_no_nans_float_conf
    )


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(init_list=_init_list_with_nans_and_no_nans,
    marked_params=params_markers_for_avg_sum), ids=idfn)
def test_hash_grpby_avg(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.avg('b')),
        conf=_no_nans_float_conf
    )


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(
    marked_params=params_markers_for_avg_sum), ids=idfn)
def test_hash_multiple_mode_query(data_gen):
    print_params(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.count('a'),
                 f.avg('b'),
                 f.avg('a'),
                 f.countDistinct('b'),
                 f.sum('a'),
                 f.min('a'),
                 f.max('a'),
                 f.countDistinct('c')), conf=_no_nans_float_conf)


@ignore_order
@approximate_float
@incompat
@pytest.mark.parametrize('data_gen', get_struct_gens(), ids=idfn)
def test_hash_multiple_mode_query_avg_distincts(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .selectExpr('avg(distinct a)', 'avg(distinct b)','avg(distinct c)'),
        conf=_no_nans_float_conf)


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(), ids=idfn)
def test_hash_count_with_filter(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .selectExpr('count(a) filter (where c > 50)'),
        conf=_no_nans_float_conf)


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(
    marked_params=params_markers_for_avg_sum), ids=idfn)
def test_hash_multiple_filters(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=100))
    df.createOrReplaceTempView("hash_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select count(a) filter (where c > 50),' +
            'count(b) filter (where c > 100),' +
            'avg(b) filter (where b > 20),' +
            'min(a), max(b) filter (where c > 250) from hash_agg_table group by a'),
        conf=_no_nans_float_conf)


@ignore_order
@allow_non_gpu('HashAggregateExec', 'AggregateExpression', 'AttributeReference', 'Alias', 'Max',
               'KnownFloatingPointNormalized', 'NormalizeNaNAndZero')
@pytest.mark.parametrize('data_gen', struct_gens_xfail, ids=idfn)
def test_hash_query_max_bug(data_gen):
    print_params(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.max('b')))

# TODO: Make config a param for partial and final only testing
# TODO: Why limit to 100, go bigger - keeping 100 for now to make it easier to debug
# TODO: String datagen combos in struct_gens which make more sense.
# TODO: Literal tests
# TODO: Port over sort aggregate tests
# TODO: First and Last tests
