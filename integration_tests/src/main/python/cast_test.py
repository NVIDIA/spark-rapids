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
from marks import allow_non_gpu, approximate_float
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
    (ArrayGen(decimal_gen_36_5), ArrayType(DecimalType(38, 5))),
    (ArrayGen(StringGen('[0-9]{1,5}')), ArrayType(IntegerType())),
    (ArrayGen(byte_gen), ArrayType(StringType())),
    (ArrayGen(byte_gen), ArrayType(DecimalType(6, 2))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(IntegerType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(StringType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(DecimalType(6, 2)))),
    (StructGen([('a', byte_gen)]), StructType([StructField('a', IntegerType())])),
    (StructGen([('a', decimal_gen_36_5)]), StructType([StructField('a', DecimalType(38, 5))])),
    (StructGen([('a', byte_gen), ('c', short_gen)]), StructType([StructField('b', IntegerType()), StructField('c', ShortType())])),
    (StructGen([('a', ArrayGen(byte_gen)), ('c', short_gen)]), StructType([StructField('a', ArrayType(IntegerType())), StructField('c', LongType())])),
    (ArrayGen(StructGen([('a', byte_gen), ('b', byte_gen)])), ArrayType(StringType())),
    (MapGen(ByteGen(nullable=False), byte_gen), MapType(StringType(), StringType())),
    (MapGen(ByteGen(nullable=False), decimal_gen_36_5), MapType(StringType(), DecimalType(38, 5))),
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
            conf = {'spark.rapids.sql.hasExtendedYearValues': 'false'})

def test_cast_string_ts_valid_format():
    # In Spark 3.2.0+ the valid format changed, and we cannot support all of the format.
    # This provides values that are valid in all of those formats.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, StringGen('[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}')).select(f.col('a').cast(TimestampType())),
            conf = {'spark.rapids.sql.hasExtendedYearValues': 'false',
                'spark.rapids.sql.castStringToTimestamp.enabled': 'true'})

@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ do we have issues with extended years")
def test_cast_string_date_fallback():
    assert_gpu_fallback_collect(
            # Cast back to String because this goes beyond what python can support for years
            lambda spark : unary_op_df(spark, StringGen('([0-9]|-|\\+){4,12}')).select(f.col('a').cast(DateType()).cast(StringType())),
            'Cast')

@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ do we have issues with extended years")
def test_cast_string_timestamp_fallback():
    assert_gpu_fallback_collect(
            # Cast back to String because this goes beyond what python can support for years
            lambda spark : unary_op_df(spark, StringGen('([0-9]|-|\\+){4,12}')).select(f.col('a').cast(TimestampType()).cast(StringType())),
            'Cast',
            conf = {'spark.rapids.sql.castStringToTimestamp.enabled': 'true'})


@approximate_float
@pytest.mark.parametrize('data_gen', decimal_gens + decimal_128_gens, ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType()], ids=meta_idfn('to:'))
def test_cast_decimal_to(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')),
            conf = copy_and_update(allow_negative_scale_of_decimal_conf, 
                {'spark.rapids.sql.castDecimalToFloat.enabled': 'true'}))

@pytest.mark.parametrize('data_gen', [
    DecimalGen(7, 1),
    DecimalGen(9, 9),
    DecimalGen(15, 2),
    DecimalGen(15, 15),
    DecimalGen(30, 3),
    DecimalGen(5, -3),
    DecimalGen(3, 0)], ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [
    DecimalType(9, 0),
    DecimalType(17, 2),
    DecimalType(35, 4),
    DecimalType(30, -4),
    DecimalType(38, -10),
    DecimalType(1, -1)], ids=meta_idfn('to:'))
def test_cast_decimal_to_decimal(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')),
            conf = allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@pytest.mark.parametrize('to_type', [
    DecimalType(2, 0),
    DecimalType(3, 0),
    DecimalType(5, 0),
    DecimalType(7, 2),
    DecimalType(10, 0),
    DecimalType(10, 2),
    DecimalType(18, 0),
    DecimalType(18, 2)], ids=idfn)
def test_cast_integral_to_decimal(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.col('a').cast(to_type)))

def test_cast_byte_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, byte_gen).select(
            f.col('a').cast(DecimalType(2, -1))),
        conf={'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

def test_cast_short_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, short_gen).select(
            f.col('a').cast(DecimalType(4, -1))),
        conf={'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

def test_cast_int_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, int_gen).select(
            f.col('a').cast(DecimalType(9, -1))),
        conf={'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

def test_cast_long_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, long_gen).select(
            f.col('a').cast(DecimalType(18, -1))),
        conf={'spark.sql.legacy.allowNegativeScaleOfDecimal': True})



# casting these types to string should be passed
basic_gens_for_cast_to_string = [byte_gen, short_gen, int_gen, long_gen, string_gen, boolean_gen, date_gen, null_gen, timestamp_gen] + decimal_gens_no_neg
# casting these types to string is not exact match, marked as xfail when testing
not_matched_gens_for_cast_to_string = [float_gen, double_gen, decimal_gen_neg_scale]
# casting these types to string is not supported, marked as xfail when testing
not_support_gens_for_cast_to_string = decimal_128_gens + [MapGen(ByteGen(False), ByteGen())]

single_level_array_gens_for_cast_to_string = [ArrayGen(sub_gen) for sub_gen in basic_gens_for_cast_to_string]
nested_array_gens_for_cast_to_string = [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(null_gen, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', date_gen]]))
    ]

all_gens_for_cast_to_string = single_level_array_gens_for_cast_to_string + nested_array_gens_for_cast_to_string

def _assert_cast_to_string_equal (data_gen, conf):
    """
    helper function for casting to string of supported type
    """
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast("STRING")),
        conf
    )

def _assert_cast_to_string_fallback (data_gen, conf):
    """
    helper function for casting to string of unsupported type
    """
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast("STRING")),
        "Cast",
        conf
    )

@pytest.mark.parametrize('data_gen', all_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
def test_cast_array_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled"    : 'true', 
        "spark.sql.legacy.castComplexTypesToString.enabled": legacy})


@pytest.mark.parametrize('data_gen', [ArrayGen(sub) for sub in not_matched_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@pytest.mark.xfail(reason='casting this type to string is not exact match')
def test_cast_array_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.sql.legacy.allowNegativeScaleOfDecimal"     : "true",
         "spark.rapids.sql.castDecimalToString.enabled"    : 'true',
         "spark.rapids.sql.castFloatToString.enabled"       : "true", 
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


@pytest.mark.parametrize('data_gen', [ArrayGen(sub) for sub in not_support_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
def test_cast_array_with_unsupported_element_to_string_fallback(data_gen, legacy):
    _assert_cast_to_string_fallback(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled"     : 'true',
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy, 
         "spark.sql.legacy.allowNegativeScaleOfDecimal": 'true'}
    )


@pytest.mark.parametrize('data_gen', [StructGen([[str(i), gen] for i, gen in enumerate(basic_gens_for_cast_to_string)])], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
def test_cast_struct_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled": 'true', 
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )

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

    assert_gpu_and_cpu_are_equal_collect(
        was_broken_for_nested_null, 
        {"spark.sql.legacy.castComplexTypesToString.enabled": 'true' if cast_conf == 'LEGACY' else 'false'}
    )

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

    assert_gpu_and_cpu_are_equal_collect(
        broken_df, 
        {"spark.sql.legacy.castComplexTypesToString.enabled": 'true' if cast_conf == 'LEGACY' else 'false'}
    )

@pytest.mark.parametrize('data_gen', [StructGen([["first", element_gen]]) for element_gen in not_matched_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@pytest.mark.xfail(reason='casting this type to string is not an exact match')
def test_cast_struct_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen, 
        {"spark.sql.legacy.allowNegativeScaleOfDecimal"     : "true",
          "spark.rapids.sql.castDecimalToString.enabled"    : 'true',
         "spark.rapids.sql.castFloatToString.enabled"       : "true", 
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )

@pytest.mark.parametrize('data_gen', [StructGen([["first", element_gen]]) for element_gen in not_support_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@allow_non_gpu('ProjectExec', 'Cast', 'Alias')
def test_cast_struct_with_unsupported_element_to_string_fallback(data_gen, legacy):
    _assert_cast_to_string_fallback(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled"     : 'true',
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy, 
         "spark.sql.legacy.allowNegativeScaleOfDecimal": 'true'}
    )
    