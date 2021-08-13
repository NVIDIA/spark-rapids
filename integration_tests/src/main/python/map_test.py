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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from marks import incompat, allow_non_gpu
from spark_session import is_before_spark_311
from pyspark.sql.types import *
import pyspark.sql.functions as f

basic_struct_gen = StructGen([
    ['child' + str(ind), sub_gen]
    for ind, sub_gen in enumerate([StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                   BooleanGen(), DateGen(), TimestampGen(), null_gen, decimal_gen_default])],
    nullable=False)

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a["key_0"]',
                'a["key_1"]',
                'a[null]',
                'a["key_9"]',
                'a["NOT_FOUND"]',
                'a["key_5"]'))

def test_map_expr_project_string_to_string():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                "map(a, b) as m1"))

def test_map_expr_project_int_to_int():
    data_gen = [('a', IntegerGen(nullable=False)), ('b', IntegerGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                "map(a, b) as m2"))

def test_map_expr_project_string_to_int():
    data_gen = [('a', StringGen(nullable=False)), ('b', IntegerGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                "map(a, b) as m1"))

def test_map_expr_project_int_to_string():
    data_gen = [('a', IntegerGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                "map(a, b) as m1"))

def test_map_expr_project_struct_to_struct():
    data_gen = [('a', basic_struct_gen), ('b', basic_struct_gen)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                "map(a, b) as m1"))

@allow_non_gpu('ProjectExec,Alias,CreateMap')
# until https://github.com/NVIDIA/spark-rapids/issues/3229 is implemented
def test_map_expr_multiple_pairs():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_fallback_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                "map(a, b, b, a) as m1"), 'ProjectExec')

def test_map_scalar_project():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(2).selectExpr(
                "map(1, 2, 3, 4) as i", 
                "map('a', 'b', 'c', 'd') as s",
                "map('a', named_struct('foo', 10, 'bar', 'bar')) as st"
                "id"))

@pytest.mark.skipif(is_before_spark_311(), reason="Only in Spark 3.1.1 + ANSI mode, map key throws on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_ansi_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["NOT_FOUND"]').collect(),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True},
                error_message='java.util.NoSuchElementException')

@pytest.mark.skipif(not is_before_spark_311(), reason="For Spark before 3.1.1 + ANSI mode, null will be returned instead of an exception if key is not found")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_get_map_value_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["NOT_FOUND"]'),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_element_at_map(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "key_0")',
                'element_at(a, "key_1")',
                'element_at(a, "null")',
                'element_at(a, "key_9")',
                'element_at(a, "NOT_FOUND")',
                'element_at(a, "key_5")'),
                conf={'spark.sql.ansi.enabled':False})

@pytest.mark.skipif(is_before_spark_311(), reason="Only in Spark 3.1.1 + ANSI mode, map key throws on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_element_at_ansi_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")').collect(),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True},
                error_message='java.util.NoSuchElementException')

@pytest.mark.skipif(not is_before_spark_311(), reason="For Spark before 3.1.1 + ANSI mode, null will be returned instead of an exception if key is not found")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_element_at_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")'),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True})
