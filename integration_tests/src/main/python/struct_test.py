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


@pytest.mark.parametrize('data_gen', [StructGen([["first", boolean_gen], ["second", byte_gen], ["third", float_gen]]),
                                      StructGen([["first", short_gen], ["second", int_gen], ["third", long_gen]]),
                                      StructGen([["first", long_gen], ["second", long_gen], ["third", long_gen]]),
                                      StructGen([["first", string_gen], ["second", ArrayGen(string_gen)], ["third", ArrayGen(string_gen)]])], ids=idfn)
def test_orderby_struct(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'struct_table',
        'select struct_table.a, struct_table.uniq_int from struct_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [StructGen([["first", string_gen], ["second", ArrayGen(string_gen)], ["third", ArrayGen(string_gen)]])], ids=idfn)
def test_orderby_struct_2(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'struct_table',
        'select struct_table.a, struct_table.uniq_int from struct_table order by uniq_int')

# conf with legacy cast to string on
legacy_complex_types_to_string = {'spark.sql.legacy.castComplexTypesToString.enabled': 'true'}
@pytest.mark.parametrize('data_gen', [StructGen([["first", boolean_gen], ["second", byte_gen], ["third", short_gen], ["fourth", int_gen], ["fifth", long_gen], ["sixth", string_gen], ["seventh", date_gen]])], ids=idfn)
def test_legacy_cast_struct_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")),
            conf = legacy_complex_types_to_string)

# https://github.com/NVIDIA/spark-rapids/issues/2309
@pytest.mark.parametrize('cast_conf', ['LEGACY', 'SPARK311+'])
def test_one_nested_null_field_legacy_cast(cast_conf):
    def was_broken_for_nested_null(spark):
        data = [
            (('foo',),),
            ((None,),),
            (None,)
        ]
        df = spark.createDataFrame(data)
        return df.select(df._1.cast(StringType()))

    assert_gpu_and_cpu_are_equal_collect(was_broken_for_nested_null, {
        'spark.sql.legacy.castComplexTypesToString.enabled': cast_conf == 'LEGACY'
    })


# https://github.com/NVIDIA/spark-rapids/issues/2315
@pytest.mark.parametrize('cast_conf', ['LEGACY', 'SPARK311+'])
def test_two_col_struct_legacy_cast(cast_conf):
    def broken_df(spark):
        key_data_gen = StructGen([
            ('a', IntegerGen(min_val=0, max_val=4)),
            ('b', IntegerGen(min_val=5, max_val=9)),
        ], nullable=False)
        val_data_gen = IntegerGen()
        df = two_col_df(spark, key_data_gen, val_data_gen)
        return df.select(df.a.cast(StringType())).filter(df.b > 1)

    assert_gpu_and_cpu_are_equal_collect(broken_df, {
        'spark.sql.legacy.castComplexTypesToString.enabled': cast_conf == 'LEGACY'
    })

@pytest.mark.parametrize('data_gen', [StructGen([["first", float_gen]])], ids=idfn)
@pytest.mark.xfail(reason='casting float to string is not an exact match')
def test_legacy_cast_struct_with_float_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")),
            conf = legacy_complex_types_to_string)

@pytest.mark.parametrize('data_gen', [StructGen([["first", double_gen]])], ids=idfn)
@pytest.mark.xfail(reason='casting double to string is not an exact match')
def test_legacy_cast_struct_with_double_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")),
            conf = legacy_complex_types_to_string)

@pytest.mark.parametrize('data_gen', [StructGen([["first", timestamp_gen]])], ids=idfn)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/219')
def test_legacy_cast_struct_with_timestamp_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")),
            conf = legacy_complex_types_to_string)

@pytest.mark.parametrize('data_gen', [StructGen([["first", boolean_gen], ["second", byte_gen], ["third", short_gen], ["fourth", int_gen], ["fifth", long_gen], ["sixth", string_gen], ["seventh", date_gen]])], ids=idfn)
def test_cast_struct_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")))

@pytest.mark.parametrize('data_gen', [StructGen([["first", float_gen]])], ids=idfn)
@pytest.mark.xfail(reason='casting float to string is not an exact match')
def test_cast_struct_with_float_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")))

@pytest.mark.parametrize('data_gen', [StructGen([["first", double_gen]])], ids=idfn)
@pytest.mark.xfail(reason='casting double to string is not an exact match')
def test_cast_struct_with_double_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")))

@pytest.mark.parametrize('data_gen', [StructGen([["first", timestamp_gen]])], ids=idfn)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/219')
def test_cast_struct_with_timestamp_to_string(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast("STRING")))
