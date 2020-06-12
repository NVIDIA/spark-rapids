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
from spark_session import with_cpu_session

_no_nans_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                       'spark.rapids.sql.hasNans': 'false',
                       'spark.rapids.sql.castStringToFloat.enabled': 'true'}

# The input lists or schemas that are used by StructGen.


_grpkey_longs_with_no_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=2048)),
    ('b', IntegerGen()),
    ('c', IntegerGen())]

# grouping longs with nulls present
_grpkey_longs_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length=2048)),
    ('b', IntegerGen()),
    ('c', IntegerGen())]

_grpkey_longs_with_timestamps = [
    ('a', RepeatSeqGen(LongGen(), length=2048)),
    ('b', DateGen(nullable=False, start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]

_grpkey_longs_with_nullable_timestamps = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=2048)),
    ('b', DateGen(nullable=(True, 5.0), start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]

# List of schemas
_init_list_rows_query = [_grpkey_longs_with_no_nulls, _grpkey_longs_with_nulls, _grpkey_longs_with_timestamps]
_init_list_range_query = [_grpkey_longs_with_timestamps, _grpkey_longs_with_nullable_timestamps]


def get_struct_gens(init_list=_init_list_rows_query, marked_params=[]):
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


params_markers_for_avg_sum=[]


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(
    marked_params=params_markers_for_avg_sum), ids=idfn)
def test_window_aggs_for_rows(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=100))
    df.createOrReplaceTempView("window_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select '
            ' sum(c) over '
            '   (partition by a order by b asc rows between 1 preceding and 1 following) as sum_c_asc, '
            ' max(c) over '
            '   (partition by a order by b desc rows between 2 preceding and 1 following) as max_c_desc, '
            ' min(c) over '
            '   (partition by a order by b asc  rows between 2 preceding and current row) as min_c_asc, '
            ' count(1) over '
            '   (partition by a order by b asc  rows between UNBOUNDED preceding and UNBOUNDED following) as count_1, '
            ' row_number() over '
            '   (partition by a order by b asc  rows between UNBOUNDED preceding and CURRENT ROW) as row_num '
            'from window_agg_table order by 1 desc, 2 desc'),
        conf=_no_nans_float_conf)


@ignore_order
@pytest.mark.parametrize('data_gen', get_struct_gens(init_list=_init_list_range_query,
    marked_params=params_markers_for_avg_sum), ids=idfn)
def test_window_aggs_for_ranges(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=100))
    df.createOrReplaceTempView("window_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select '
            ' sum(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between interval 1 day preceding and interval 1 day following) as sum_c_asc, '
            ' max(c) over '
            '   (partition by a order by cast(b as timestamp) desc '
            '       range between interval 2 days preceding and interval 1 days following) as max_c_desc, '
            ' min(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between interval 2 days preceding and current row) as min_c_asc, '
            ' count(1) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between  CURRENT ROW and UNBOUNDED following) as count_1_asc, '
            ' sum(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between UNBOUNDED preceding and CURRENT ROW) as sum_c_unbounded, '
            ' max(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between UNBOUNDED preceding and UNBOUNDED following) as max_c_unbounded '
            'from window_agg_table'),
        conf=_no_nans_float_conf)

