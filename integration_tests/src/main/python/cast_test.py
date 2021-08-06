# Copyright (c) 2021, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error
from data_gen import *
from functools import reduce
from spark_session import is_before_spark_311
from marks import allow_non_gpu
from pyspark.sql.types import *
from pyspark.sql.functions import array_contains, col, first, isnan, lit, element_at

def test_cast_empty_string_to_int():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, StringGen(pattern="")).selectExpr(
                'CAST(a as BYTE)',
                'CAST(a as SHORT)',
                'CAST(a as INTEGER)',
                'CAST(a as LONG)'))

# These tests are not intended to be exhaustive. The scala test CastOpSuite should cover
# just about everything for non-nested values. This is intended to check that the
# recursive code in nested type checks, like arrays, is working properly. So we are going
# pick child types that are simple to cast. Upcasting integer values and casting them to strings
@pytest.mark.parametrize('data_gen,to_type', [
    (ArrayGen(byte_gen), ArrayType(IntegerType())),
    (ArrayGen(byte_gen), ArrayType(StringType())),
    (ArrayGen(byte_gen), ArrayType(DecimalType(6, 2))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(IntegerType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(StringType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(DecimalType(6, 2)))),
    (StructGen([('a', byte_gen)]), StructType([StructField('a', IntegerType())])),
    (StructGen([('a', byte_gen), ('c', short_gen)]), StructType([StructField('b', IntegerType()), StructField('c', ShortType())])),
    (StructGen([('a', ArrayGen(byte_gen)), ('c', short_gen)]), StructType([StructField('a', ArrayType(IntegerType())), StructField('c', LongType())])),
    (ArrayGen(StructGen([('a', byte_gen), ('b', byte_gen)])), ArrayType(StringType()))
    ], ids=idfn)
def test_cast_nested(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)))


