# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error, assert_gpu_fallback_collect, assert_py4j_exception
from data_gen import *
from spark_session import is_before_spark_320, is_before_spark_330, with_gpu_session
from marks import allow_non_gpu, approximate_float
from pyspark.sql.types import *
from spark_init_internal import spark_version

_decimal_gen_36_5 = DecimalGen(precision=36, scale=5)

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
    (ArrayGen(_decimal_gen_36_5), ArrayType(DecimalType(38, 5))),
    (ArrayGen(StringGen('[0-9]{1,5}')), ArrayType(IntegerType())),
    (ArrayGen(byte_gen), ArrayType(StringType())),
    (ArrayGen(byte_gen), ArrayType(DecimalType(6, 2))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(IntegerType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(StringType()))),
    (ArrayGen(ArrayGen(byte_gen)), ArrayType(ArrayType(DecimalType(6, 2)))),
    (StructGen([('a', byte_gen)]), StructType([StructField('a', IntegerType())])),
    (StructGen([('a', _decimal_gen_36_5)]), StructType([StructField('a', DecimalType(38, 5))])),
    (StructGen([('a', byte_gen), ('c', short_gen)]), StructType([StructField('b', IntegerType()), StructField('c', ShortType())])),
    (StructGen([('a', ArrayGen(byte_gen)), ('c', short_gen)]), StructType([StructField('a', ArrayType(IntegerType())), StructField('c', LongType())])),
    (ArrayGen(StructGen([('a', byte_gen), ('b', byte_gen)])), ArrayType(StringType())),
    (MapGen(ByteGen(nullable=False), byte_gen), MapType(StringType(), StringType())),
    (MapGen(ByteGen(nullable=False), _decimal_gen_36_5), MapType(StringType(), DecimalType(38, 5))),
    (MapGen(ShortGen(nullable=False), ArrayGen(byte_gen)), MapType(IntegerType(), ArrayType(ShortType()))),
    (MapGen(ShortGen(nullable=False), ArrayGen(StructGen([('a', byte_gen)]))), MapType(IntegerType(), ArrayType(StructType([StructField('b', ShortType())]))))
    ], ids=idfn)
def test_cast_nested(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)))

def test_cast_string_date_valid_format():
    # In Spark 3.2.0+ the valid format changed, and we cannot support all of the format.
    # This provides values that are valid in all of those formats.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, StringGen('[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}')).select(f.col('a').cast(DateType())),
            conf = {'spark.rapids.sql.hasExtendedYearValues': 'false'})

@pytest.mark.skipif(is_before_spark_320(), reason="ansi cast(string as date) throws exception only in 3.2.0+")
@pytest.mark.parametrize('invalid', ['200', '1970A', '1970 A', '1970T', '1970 T', '1970-01T', '1970-01 A',
                                     '1970-01-01A',  # 1970-01-01T is OK, 1970-01-01A is NOK
                                     '2022-02-29',  # nonexistent day
                                     '200-1-1',
                                     '2001-13-1',  # nonexistent day
                                     '2001-1-32',  # nonexistent day
                                     '2001-1-32'  # nonexistent day
                                     ])
def test_cast_string_date_invalid_ansi(invalid):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(invalid,)], "a string").select(f.col('a').cast(DateType())).collect(),
        conf={'spark.rapids.sql.hasExtendedYearValues': 'false',
              'spark.sql.ansi.enabled': 'true'},
        error_message="DateTimeException")

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
@pytest.mark.parametrize('data_gen', [
    decimal_gen_32bit, decimal_gen_32bit_neg_scale, DecimalGen(precision=7, scale=7),
    decimal_gen_64bit, decimal_gen_128bit, DecimalGen(precision=30, scale=2),
    DecimalGen(precision=36, scale=5), DecimalGen(precision=38, scale=0),
    DecimalGen(precision=38, scale=10), DecimalGen(precision=36, scale=-5),
    DecimalGen(precision=38, scale=-10)], ids=meta_idfn('from:'))
@pytest.mark.parametrize('to_type', [ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType()], ids=meta_idfn('to:'))
def test_cast_decimal_to(data_gen, to_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')),
            conf = {'spark.rapids.sql.castDecimalToFloat.enabled': 'true'})

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
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').cast(to_type), f.col('a')))

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
            f.col('a').cast(DecimalType(2, -1))))

def test_cast_short_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, short_gen).select(
            f.col('a').cast(DecimalType(4, -1))))

def test_cast_int_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, int_gen).select(
            f.col('a').cast(DecimalType(9, -1))))

def test_cast_long_to_decimal_overflow():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, long_gen).select(
            f.col('a').cast(DecimalType(18, -1))))

# casting these types to string should be passed
basic_gens_for_cast_to_string = [ByteGen, ShortGen, IntegerGen, LongGen, StringGen, BooleanGen, DateGen, TimestampGen] 
basic_array_struct_gens_for_cast_to_string = [f() for f in basic_gens_for_cast_to_string] + [null_gen] + decimal_gens
basic_map_gens_for_cast_to_string = [
    MapGen(f(nullable=False), f()) for f in basic_gens_for_cast_to_string] + [
    MapGen(DecimalGen(nullable=False), DecimalGen(precision=7, scale=3)), MapGen(DecimalGen(precision=7, scale=7, nullable=False), DecimalGen(precision=12, scale=2))]

# GPU does not match CPU to casting these types to string, marked as xfail when testing
not_matched_gens_for_cast_to_string = [FloatGen, DoubleGen]
not_matched_struct_array_gens_for_cast_to_string = [f() for f in not_matched_gens_for_cast_to_string] + [decimal_gen_32bit_neg_scale]
not_matched_map_gens_for_cast_to_string = [MapGen(f(nullable = False), f()) for f in not_matched_gens_for_cast_to_string] + [MapGen(DecimalGen(precision=7, scale=-3, nullable=False), DecimalGen())]

single_level_array_gens_for_cast_to_string = [ArrayGen(sub_gen) for sub_gen in basic_array_struct_gens_for_cast_to_string]
nested_array_gens_for_cast_to_string = [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(null_gen, max_length=10), max_length=10),
    ArrayGen(MapGen(ByteGen(nullable=False), DateGen()), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', date_gen]]))
    ]

all_array_gens_for_cast_to_string = single_level_array_gens_for_cast_to_string + nested_array_gens_for_cast_to_string

def _assert_cast_to_string_equal (data_gen, conf):
    """
    helper function for casting to string of supported type
    """
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast("STRING")),
        conf
    )


@pytest.mark.parametrize('data_gen', all_array_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
def test_cast_array_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled"    : 'true', 
        "spark.sql.legacy.castComplexTypesToString.enabled": legacy})


@pytest.mark.parametrize('data_gen', [ArrayGen(sub) for sub in not_matched_struct_array_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@pytest.mark.xfail(reason='casting this type to string is not exact match')
def test_cast_array_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.rapids.sql.castDecimalToString.enabled"     : 'true',
         "spark.rapids.sql.castFloatToString.enabled"       : "true", 
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


@pytest.mark.parametrize('data_gen', basic_map_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
def test_cast_map_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled"    : 'true',
        "spark.sql.legacy.castComplexTypesToString.enabled": legacy})


@pytest.mark.parametrize('data_gen', not_matched_map_gens_for_cast_to_string, ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@pytest.mark.xfail(reason='casting this type to string is not exact match')
def test_cast_map_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen,
        {"spark.rapids.sql.castDecimalToString.enabled"     : 'true',
         "spark.rapids.sql.castFloatToString.enabled"       : "true",
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


@pytest.mark.parametrize('data_gen', [StructGen([[str(i), gen] for i, gen in enumerate(basic_array_struct_gens_for_cast_to_string)] + [["map", MapGen(ByteGen(nullable=False), null_gen)]])], ids=idfn)
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

@pytest.mark.parametrize('data_gen', [StructGen([["first", element_gen]]) for element_gen in not_matched_struct_array_gens_for_cast_to_string], ids=idfn)
@pytest.mark.parametrize('legacy', ['true', 'false'])
@pytest.mark.xfail(reason='casting this type to string is not an exact match')
def test_cast_struct_with_unmatched_element_to_string(data_gen, legacy):
    _assert_cast_to_string_equal(
        data_gen, 
        {"spark.rapids.sql.castDecimalToString.enabled"     : 'true',
         "spark.rapids.sql.castFloatToString.enabled"       : "true", 
         "spark.sql.legacy.castComplexTypesToString.enabled": legacy}
    )


# The bug SPARK-37451 only affects the following versions
def is_neg_dec_scale_bug_version():
    return ("3.1.1" <= spark_version() < "3.1.3") or ("3.2.0" <= spark_version() < "3.2.1")

@pytest.mark.skipif(is_neg_dec_scale_bug_version(), reason="RAPIDS doesn't support casting string to decimal for negative scale decimal in this version of Spark because of SPARK-37451")
def test_cast_string_to_negative_scale_decimal():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, StringGen("[0-9]{9}")).select(
            f.col('a').cast(DecimalType(8, -3))))

@pytest.mark.skipif(is_before_spark_330(), reason="ansi cast throws exception only in 3.3.0+")
@pytest.mark.parametrize('type', [DoubleType(), FloatType()])
def test_cast_double_to_timestamp(type):
    def fun(spark):
        data=[float("inf"),float("-inf"),float("nan")]
        df = spark.createDataFrame(data, DoubleType())
        return df.select(f.col('value').cast(TimestampType())).collect()
    assert_gpu_and_cpu_error(fun, {"spark.sql.ansi.enabled": True}, "java.time.DateTimeException")

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_cast_day_time_interval_to_string():
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='day', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='hour', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='minute', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='day', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='hour', end_field='hour', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='hour', end_field='minute', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='hour', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='minute', end_field='minute', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='minute', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})
    _assert_cast_to_string_equal(DayTimeIntervalGen(start_field='second', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)]), {})

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_cast_string_to_day_time_interval():
    gen = DayTimeIntervalGen(start_field='day', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)])
    dtType = DayTimeIntervalType(0, 3) # 0 is day; 3 is second
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).select(f.col('a').cast(StringType()).cast(dtType)))

    gen = DayTimeIntervalGen(start_field='hour', end_field='second', special_cases=[MIN_DAY_TIME_INTERVAL, MAX_DAY_TIME_INTERVAL, timedelta(seconds=0)])
    dtType = DayTimeIntervalType(1, 3) # 1 is hour; 3 is second
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).select(f.col('a').cast(StringType()).cast(dtType)))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('invalid_string', [
    "INTERVAL 'xxx' DAY TO SECOND", # invalid format
    "-999999999 04:00:54.775808000" # exceeds min value, min value is "-106751991 04:00:54.775808000"
])
def test_cast_string_to_day_time_interval_exception(invalid_string):
    dtType = DayTimeIntervalType(0, 3)
    def fun(spark):
        data=[invalid_string]
        df = spark.createDataFrame(data, StringType())
        return df.select(f.col('value').cast(dtType)).collect()
    assert_gpu_and_cpu_error(fun, {}, "java.lang.IllegalArgumentException")
