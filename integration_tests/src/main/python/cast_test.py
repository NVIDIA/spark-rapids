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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from functools import reduce
from spark_session import is_before_spark_311, is_before_spark_320
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
    (ArrayGen(StringGen('[0-9]{1,5}')), ArrayType(IntegerType())),
    (ArrayGen(byte_gen), ArrayType(StringType())),
    (ArrayGen(byte_gen), ArrayType(DecimalType(6, 2))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(IntegerType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(StringType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(DecimalType(6, 2)))),
    (StructGen([('a', byte_gen)]), StructType([StructField('a', IntegerType())])),
    (StructGen([('a', byte_gen), ('c', short_gen)]), StructType([StructField('b', IntegerType()), StructField('c', ShortType())])),
    (StructGen([('a', ArrayGen(byte_gen)), ('c', short_gen)]), StructType([StructField('a', ArrayType(IntegerType())), StructField('c', LongType())])),
    (ArrayGen(StructGen([('a', byte_gen), ('b', byte_gen)])), ArrayType(StringType())),
    (MapGen(ByteGen(nullable=False), byte_gen), MapType(StringType(), StringType())),
    (MapGen(ShortGen(nullable=False), ArrayGen(byte_gen)), MapType(IntegerType(), ArrayType(ShortType()))),
    (MapGen(ShortGen(nullable=False), ArrayGen(StructGen([('a', byte_gen)]))), MapType(IntegerType(), ArrayType(StructType([StructField('b', ShortType())]))))
    ], ids=idfn)
def test_cast_nested(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)))

@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
@pytest.mark.parametrize('data_gen,to_type', [
    # maps are not supported for casting to a String, but structs are, so we need to verify this
    (StructGen([('structF1', StructGen([('structF11', MapGen(ByteGen(nullable=False), byte_gen))]))]), StringType())])
def test_cast_nested_fallback(data_gen, to_type):
    assert_gpu_fallback_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)),
            'Cast')

def test_cast_string_date_valid_format():
    # In Spark 3.2.0+ the valid format changed, and we cannot support all of the format.
    # This provides values that are valid in all of those formats.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, StringGen('[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}')).select(f.col('a').cast(DateType())),
            conf = {'spark.rapids.sql.parseExtendedYears.enabled': 'true'})

def test_cast_string_ts_valid_format():
    # In Spark 3.2.0+ the valid format changed, and we cannot support all of the format.
    # This provides values that are valid in all of those formats.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, StringGen('[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}')).select(f.col('a').cast(TimestampType())),
            conf = {'spark.rapids.sql.parseExtendedYears.enabled': 'true',
                'spark.rapids.sql.castStringToTimestamp.enabled': 'true'})

@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ do we have issues with extended years")
def test_cast_string_date_fallback():
    assert_gpu_fallback_collect(
            # Cast back to String because this goes beyond what python can support for years
            lambda spark : unary_op_df(spark, StringGen('([0-9]|-|\+){4,12}')).select(f.col('a').cast(DateType()).cast(StringType())),
            'Cast')

@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ do we have issues with extended years")
def test_cast_string_timestamp_fallback():
    assert_gpu_fallback_collect(
            # Cast back to String because this goes beyond what python can support for years
            lambda spark : unary_op_df(spark, StringGen('([0-9]|-|\+){4,12}')).select(f.col('a').cast(TimestampType()).cast(StringType())),
            'Cast',
            conf = {'spark.rapids.sql.castStringToTimestamp.enabled': 'true'})

