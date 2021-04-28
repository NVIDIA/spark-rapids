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
from datetime import date, datetime, timezone
from marks import incompat
from pyspark.sql.types import *
from spark_session import with_spark_session
import pyspark.sql.functions as f

# We only support literal intervals for TimeSub
vals = [(-584, 1563), (1943, 1101), (2693, 2167), (2729, 0), (44, 1534), (2635, 3319),
            (1885, -2828), (0, 2463), (932, 2286), (0, 0)]
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
def test_timesub(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        # We are starting at year 0015 to make sure we don't go before year 0001 while doing TimeSub
        lambda spark: unary_op_df(spark, TimestampGen(start=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
            .selectExpr("a - (interval {} days {} seconds)".format(days, seconds)))

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
def test_timeadd(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        # We are starting at year 0005 to make sure we don't go before year 0001
        # and beyond year 10000 while doing TimeAdd
        lambda spark: unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
            .selectExpr("a + (interval {} days {} seconds)".format(days, seconds)))

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
def test_dateaddinterval(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days {} seconds)'.format(days, seconds),
            'a - (interval {} days {} seconds)'.format(days, seconds)))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_datediff(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'datediff(a, b)',
            'datediff(\'2016-03-02\', b)',
            'datediff(date(null), b)',
            'datediff(a, date(null))',
            'datediff(a, \'2016-03-02\')'))

def test_hour():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('hour(a)'))

def test_minute():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('minute(a)'))

def test_second():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('second(a)'))

def test_quarter():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('quarter(a)'))

def test_weekday():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('weekday(a)'))

def test_dayofweek():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('dayofweek(a)'))

def test_last_day():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('last_day(a)'))

# We have to set the upper/lower limit on IntegerGen so the date_add doesn't overflow
# Python uses proleptic gregorian date which extends Gregorian calendar as it always existed and
# always exist in future. When performing date_sub('0001-01-01', 1), it will blow up because python
# doesn't recognize dates before Jan 01, 0001. Samething with date_add('9999-01-01', 1), because
# python doesn't recognize dates after Dec 31, 9999. To get around this problem, we have limited the
# Integer value for days to stay within ~ 200 years or ~70000 days to stay within the legal limits
# python date
days_gen = [ByteGen(), ShortGen(), IntegerGen(min_val=-70000, max_val=70000, special_cases=[-70000, 7000,0,1,-1])]
@pytest.mark.parametrize('data_gen', days_gen, ids=idfn)
def test_dateadd(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), data_gen)
                .selectExpr('date_add(a, b)',
               'date_add(date(\'2016-03-02\'), b)',
               'date_add(date(null), b)',
               'date_add(a, cast(null as {}))'.format(string_type),
               'date_add(a, cast(24 as {}))'.format(string_type)))

@pytest.mark.parametrize('data_gen', days_gen, ids=idfn)
def test_datesub(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), data_gen)
                .selectExpr('date_sub(a, b)',
                'date_sub(date(\'2016-03-02\'), b)',
                'date_sub(date(null), b)',
                'date_sub(a, cast(null as {}))'.format(string_type),
                'date_sub(a, cast(24 as {}))'.format(string_type)))

# In order to get a bigger range of values tested for Integer days for date_sub and date_add
# we are casting the output to unix_timestamp. Even that overflows if the integer value is greater
# than 103819094 and less than -109684887 for date('9999-12-31') or greater than 107471152 and less
# than -106032829 for date('0001-01-01') so we have to cap the days values to the lower upper and
# lower ranges.
to_unix_timestamp_days_gen=[ByteGen(), ShortGen(), IntegerGen(min_val=-106032829, max_val=103819094, special_cases=[-106032829, 103819094,0,1,-1])]
@pytest.mark.parametrize('data_gen', to_unix_timestamp_days_gen, ids=idfn)
@incompat
def test_dateadd_with_date_overflow(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, DateGen(),
           data_gen).selectExpr('unix_timestamp(date_add(a, b))',
           'unix_timestamp(date_add( date(\'2016-03-02\'), b))',
           'unix_timestamp(date_add(date(null), b))',
           'unix_timestamp(date_add(a, cast(null as {})))'.format(string_type),
           'unix_timestamp(date_add(a, cast(24 as {})))'.format(string_type)))

to_unix_timestamp_days_gen=[ByteGen(), ShortGen(), IntegerGen(max_val=106032829, min_val=-103819094, special_cases=[106032829, -103819094,0,1,-1])]
@pytest.mark.parametrize('data_gen', to_unix_timestamp_days_gen, ids=idfn)
@incompat
def test_datesub_with_date_overflow(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, DateGen(),
           data_gen).selectExpr('unix_timestamp(date_sub(a, b))',
           'unix_timestamp(date_sub( date(\'2016-03-02\'), b))',
           'unix_timestamp(date_sub(date(null), b))',
           'unix_timestamp(date_sub(a, cast(null as {})))'.format(string_type),
           'unix_timestamp(date_sub(a, cast(24 as {})))'.format(string_type)))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_year(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.year(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_month(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.month(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_dayofmonth(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.dayofmonth(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_dayofyear(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.dayofyear(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_unix_timestamp(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_to_unix_timestamp(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr("to_unix_timestamp(a)"))

@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_unix_timestamp_improved(data_gen):
    conf = {"spark.rapids.sql.improvedTimeOps.enabled": "true",
            "spark.sql.legacy.timeParserPolicy": "CORRECTED"}
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col('a'))), conf)

@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_to_unix_timestamp_improved(data_gen):
    conf = {"spark.rapids.sql.improvedTimeOps.enabled": "true"}
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr("to_unix_timestamp(a)"), conf)

str_date_and_format_gen = [pytest.param(StringGen('[0-9]{4}/[01][0-9]'),'yyyy/MM', marks=pytest.mark.xfail(reason="cudf does no checks")),
        (StringGen('[0-9]{4}/[01][12]/[0-2][1-8]'),'yyyy/MM/dd'),
        (StringGen('[01][12]/[0-2][1-8]'), 'MM/dd'),
        (StringGen('[0-2][1-8]/[01][12]'), 'dd/MM'),
        (ConvertGen(DateGen(nullable=False), lambda d: d.strftime('%Y/%m').zfill(7), data_type=StringType()), 'yyyy/MM')]

@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen, ids=idfn)
def test_string_to_unix_timestamp(data_gen, date_form):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).selectExpr("to_unix_timestamp(a, '{}')".format(date_form)))

@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen, ids=idfn)
def test_string_unix_timestamp(data_gen, date_form):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen, seed=1).select(f.unix_timestamp(f.col('a'), date_form)))

supported_date_formats = ['yyyy-MM-dd', 'yyyy-MM', 'yyyy/MM/dd', 'yyyy/MM', 'dd/MM/yyyy',
                          'MM-dd', 'MM/dd', 'dd-MM', 'dd/MM']
@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_date_format(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))
