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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from conftest import is_dataproc_runtime
from data_gen import *
from pyspark.sql.types import *

@pytest.mark.parametrize('data_gen', [StructGen([["first", boolean_gen], ["second", byte_gen], ["third", float_gen]]),
    StructGen([["first", short_gen], ["second", int_gen], ["third", long_gen]]),
    StructGen([["first", double_gen], ["second", date_gen], ["third", timestamp_gen]]),
    StructGen([["first", string_gen], ["second", ArrayGen(byte_gen)], ["third", simple_string_to_string_map_gen]]),
    StructGen([["first", decimal_gen_default], ["second", decimal_gen_scale_precision], ["third", decimal_gen_same_scale_precision]])], ids=idfn)
def test_struct_get_item(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a.first',
                'a.second',
                'a.third'))


@pytest.mark.parametrize('data_gen', all_basic_gens + [decimal_gen_default, decimal_gen_scale_precision], ids=idfn)
def test_make_struct(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'struct(a, b)',
                'named_struct("foo", b, "bar", 5, "end", a)'))


@pytest.mark.xfail(condition=is_dataproc_runtime(),
                   reason='https://github.com/NVIDIA/spark-rapids/issues/1541')
@pytest.mark.parametrize('data_gen', [StructGen([["first", boolean_gen], ["second", byte_gen], ["third", float_gen]]),
                                      StructGen([["first", short_gen], ["second", int_gen], ["third", long_gen]]),
                                      StructGen([["first", long_gen], ["second", long_gen], ["third", long_gen]]),
                                      StructGen([["first", string_gen], ["second", ArrayGen(string_gen)], ["third", ArrayGen(string_gen)]])], ids=idfn)
def test_orderby_struct(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen),
        'struct_table',
        'select struct_table.a, struct_table.a.first as val from struct_table order by val')


@pytest.mark.xfail(condition=is_dataproc_runtime(),
                   reason='https://github.com/NVIDIA/spark-rapids/issues/1541')
@pytest.mark.parametrize('data_gen', [StructGen([["first", string_gen], ["second", ArrayGen(string_gen)], ["third", ArrayGen(string_gen)]])], ids=idfn)
def test_orderby_struct_2(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen),
        'struct_table',
        'select struct_table.a, struct_table.a.second[0] as val from struct_table order by val')