# Copyright (c) 2020-2024, NVIDIA CORPORATION.
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
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_gpu_and_cpu_error
from conftest import is_utc, is_supported_time_zone, get_test_tz
from data_gen import *
from datetime import date, datetime, timezone
from marks import allow_non_gpu, datagen_overrides, disable_ansi_mode, ignore_order, incompat, tz_sensitive_test
from pyspark.sql.types import *
from spark_session import with_cpu_session, is_before_spark_330, is_before_spark_350
import pyspark.sql.functions as f

# Some operations only work in UTC specifically
non_utc_tz_allow = ['ProjectExec'] if not is_utc() else []
# Others work in all supported time zones
non_supported_tz_allow = ['ProjectExec'] if not is_supported_time_zone() else []

# We only support literal intervals for TimeSub
vals = [(-584, 1563), (1943, 1101), (2693, 2167), (2729, 0), (44, 1534), (2635, 3319),
            (1885, -2828), (0, 2463), (932, 2286), (0, 0)]
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timesub(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        # We are starting at year 0015 to make sure we don't go before year 0001 while doing TimeSub
        lambda spark: unary_op_df(spark, TimestampGen(start=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
            .selectExpr("a - (interval {} days {} seconds)".format(days, seconds)))

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timeadd(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        # We are starting at year 0005 to make sure we don't go before year 0001
        # and beyond year 10000 while doing TimeAdd
        lambda spark: unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
            .selectExpr("a + (interval {} days {} seconds)".format(days, seconds)))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@allow_non_gpu(*non_utc_allow)
def test_timeadd_daytime_column():
    gen_list = [
        # timestamp column max year is 1000
        ('t', TimestampGen(end=datetime(1000, 1, 1, tzinfo=timezone.utc))),
        # max days is 8000 year, so added result will not be out of range
        ('d', DayTimeIntervalGen(min_value=timedelta(days=0), max_value=timedelta(days=8000 * 365)))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen_list).selectExpr("t + d", "t + INTERVAL '1 02:03:04' DAY TO SECOND"))

@pytest.mark.skipif(is_before_spark_350(), reason='DayTimeInterval overflow check for seconds is not supported before Spark 3.5.0')
def test_interval_seconds_overflow_exception():
    assert_gpu_and_cpu_error(
        lambda spark : spark.sql(""" select cast("interval '10 01:02:69' day to second" as interval day to second) """).collect(),
        conf={},
        error_message="IllegalArgumentException")

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timeadd_from_subquery(data_gen):

    def fun(spark):
        df = unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
        df.createOrReplaceTempView("testTime")
        spark.sql("select a, ((select max(a) from testTime) + interval 1 day) as datePlus from testTime").createOrReplaceTempView("testTime2")
        return spark.sql("select * from testTime2 where datePlus > current_timestamp")

    assert_gpu_and_cpu_are_equal_collect(fun)

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timesub_from_subquery(data_gen):

    def fun(spark):
        df = unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
        df.createOrReplaceTempView("testTime")
        spark.sql("select a, ((select min(a) from testTime) - interval 1 day) as dateMinus from testTime").createOrReplaceTempView("testTime2")
        return spark.sql("select * from testTime2 where dateMinus < current_timestamp")

    assert_gpu_and_cpu_are_equal_collect(fun)


@disable_ansi_mode  # ANSI mode tested separately.
# Should specify `spark.sql.legacy.interval.enabled` to test `DateAddInterval` after Spark 3.2.0,
# refer to https://issues.apache.org/jira/browse/SPARK-34896
# [SPARK-34896][SQL] Return day-time interval from dates subtraction
# 1. Add the SQL config `spark.sql.legacy.interval.enabled` which will control when Spark SQL should use `CalendarIntervalType` instead of ANSI intervals.
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_dateaddinterval(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days {} seconds)'.format(days, seconds),
            'a - (interval {} days {} seconds)'.format(days, seconds)),
        legacy_interval_enabled_conf)

# test add days(not specify hours, minutes, seconds, milliseconds, microseconds) in ANSI mode.
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_dateaddinterval_ansi(data_gen):
    days, _ = data_gen
    # only specify the `days`
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days)'.format(days)),
        conf=copy_and_update(ansi_enabled_conf, legacy_interval_enabled_conf))

# Throws if add hours, minutes or seconds, milliseconds, microseconds to a date in ANSI mode
def test_dateaddinterval_ansi_exception():
    assert_gpu_and_cpu_error(
        # specify the `seconds`
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days {} seconds)'.format(1, 5)).collect(),
        conf=copy_and_update(ansi_enabled_conf, legacy_interval_enabled_conf),
        error_message="IllegalArgumentException")

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_datediff(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'datediff(a, b)',
            'datediff(\'2016-03-02\', b)',
            'datediff(date(null), b)',
            'datediff(a, date(null))',
            'datediff(a, \'2016-03-02\')'))

hms_fallback = ['ProjectExec'] if not is_supported_time_zone() else []

@allow_non_gpu(*hms_fallback)
def test_hour():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('hour(a)'))

@allow_non_gpu(*hms_fallback)
def test_minute():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('minute(a)'))

@allow_non_gpu(*hms_fallback)
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
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10027')
@pytest.mark.parametrize('data_gen', to_unix_timestamp_days_gen, ids=idfn)
@incompat
@allow_non_gpu(*non_utc_allow)
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
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10027')
@pytest.mark.parametrize('data_gen', to_unix_timestamp_days_gen, ids=idfn)
@incompat
@allow_non_gpu(*non_utc_allow)
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
@tz_sensitive_test
@allow_non_gpu(*non_supported_tz_allow)
def test_unix_timestamp(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col('a'))))


@pytest.mark.skipif(is_supported_time_zone(), reason='fallback only happens on unsupported timezones')
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_unsupported_fallback_unix_timestamp(data_gen):
    assert_gpu_fallback_collect(lambda spark: gen_df(
        spark, [("a", data_gen), ("b", string_gen)], length=10).selectExpr(
        "unix_timestamp(a, b)"),
        "UnixTimestamp")

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_to_unix_timestamp(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("to_unix_timestamp(a)"),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.skipif(is_supported_time_zone(), reason='fallback only happens on unsupported timezones')
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_unsupported_fallback_to_unix_timestamp(data_gen):
    assert_gpu_fallback_collect(lambda spark: gen_df(
        spark, [("a", data_gen), ("b", string_gen)], length=10).selectExpr(
        "to_unix_timestamp(a, b)"),
        "ToUnixTimestamp")
    
supported_timezones = ["Asia/Shanghai", "UTC", "UTC+0", "UTC-0", "GMT", "GMT+0", "GMT-0", "EST", "MST", "VST"]
unsupported_timezones = ["PST", "NST", "AST", "America/Los_Angeles", "America/New_York", "America/Chicago"]

@pytest.mark.parametrize('time_zone', supported_timezones, ids=idfn)
def test_from_utc_timestamp(time_zone):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, timestamp_gen).select(f.from_utc_timestamp(f.col('a'), time_zone)))

@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('time_zone', unsupported_timezones, ids=idfn)
def test_from_utc_timestamp_unsupported_timezone_fallback(time_zone):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, timestamp_gen).select(f.from_utc_timestamp(f.col('a'), time_zone)),
    'FromUTCTimestamp')

@allow_non_gpu('ProjectExec')
def test_unsupported_fallback_from_utc_timestamp():
    time_zone_gen = StringGen(pattern="UTC")
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", timestamp_gen), ("tzone", time_zone_gen)]).selectExpr(
            "from_utc_timestamp(a, tzone)"),
        'FromUTCTimestamp')

@pytest.mark.parametrize('time_zone', supported_timezones, ids=idfn)
def test_to_utc_timestamp(time_zone):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, timestamp_gen).select(f.to_utc_timestamp(f.col('a'), time_zone)))

@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('time_zone', unsupported_timezones, ids=idfn)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
def test_to_utc_timestamp_unsupported_timezone_fallback(data_gen, time_zone):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.to_utc_timestamp(f.col('a'), time_zone)),
    'ToUTCTimestamp')

@allow_non_gpu('ProjectExec')
def test_unsupported_fallback_to_utc_timestamp():
    time_zone_gen = StringGen(pattern="UTC")
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", timestamp_gen), ("tzone", time_zone_gen)]).selectExpr(
            "to_utc_timestamp(a, tzone)"),
        'ToUTCTimestamp')

@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('data_gen', [long_gen], ids=idfn)
def test_unsupported_fallback_from_unixtime(data_gen):
    fmt_gen = StringGen(pattern="[M]")
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", data_gen), ("fmt", fmt_gen)]).selectExpr(
            "from_unixtime(a, fmt)"),
        'FromUnixTime')


@pytest.mark.parametrize('invalid,fmt', [
    ('2021-01/01', 'yyyy-MM-dd'),
    ('2021/01-01', 'yyyy/MM/dd'),
    ('2021/01', 'yyyy-MM'),
    ('2021-01', 'yyyy/MM'),
    ('01/02/201', 'dd/MM/yyyy'),
    ('2021-01-01 00:00', 'yyyy-MM-dd HH:mm:ss'),
    ('01#01', 'MM-dd'),
    ('01T01', 'MM/dd'),
    ('29-02', 'dd-MM'),  # 1970-02-29 is invalid
    ('01-01', 'dd/MM'),
    ('2021-01', 'MM/yyyy'),
    ('2021-01', 'MM-yyyy'),
    ('01-02-2022', 'MM/dd/yyyy'),
    ('99-01-2022', 'MM-dd-yyyy'),
], ids=idfn)
@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
@pytest.mark.parametrize('operator', ["to_unix_timestamp", "unix_timestamp", "to_timestamp", "to_date"], ids=idfn)
def test_string_to_timestamp_functions_ansi_invalid(invalid, fmt, parser_policy, operator):
    sql = "{operator}(a, '{fmt}')".format(fmt=fmt, operator=operator)
    parser_policy_dic = {"spark.sql.legacy.timeParserPolicy": "{}".format(parser_policy)}

    def fun(spark):
        df = spark.createDataFrame([(invalid,)], "a string")
        return df.selectExpr(sql).collect()

    assert_gpu_and_cpu_error(fun, conf=copy_and_update(parser_policy_dic, ansi_enabled_conf), error_message="Exception")


@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
# first get expected string via `date_format`
@allow_non_gpu(*non_utc_allow)
def test_string_to_timestamp_functions_ansi_valid(parser_policy):
    expr_format = "{operator}(date_format(a, '{fmt}'), '{fmt}')"
    formats = ['yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM', 'dd/MM/yyyy', 'yyyy-MM-dd HH:mm:ss',
               'MM-dd', 'MM/dd', 'dd-MM', 'dd/MM', 'MM/yyyy', 'MM-yyyy', 'MM/dd/yyyy', 'MM-dd-yyyy']
    operators = ["to_unix_timestamp", "unix_timestamp", "to_timestamp", "to_date"]
    format_operator_pairs = [(fmt, operator) for fmt in formats for operator in operators]
    expr_list = [expr_format.format(operator=operator, fmt=fmt) for (fmt, operator) in format_operator_pairs]
    parser_policy_dic = {"spark.sql.legacy.timeParserPolicy": "{}".format(parser_policy)}

    def fun(spark):
        df = spark.createDataFrame([(datetime(1970, 8, 12, tzinfo=timezone.utc),)], "a timestamp")
        return df.selectExpr(expr_list)

    assert_gpu_and_cpu_are_equal_collect(fun, conf=copy_and_update(parser_policy_dic, ansi_enabled_conf))


@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@tz_sensitive_test
@allow_non_gpu(*non_supported_tz_allow)
def test_unix_timestamp(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col("a"))),
        {'spark.sql.ansi.enabled': ansi_enabled})


str_date_and_format_gen = [pytest.param(StringGen('[0-9]{4}/[01][0-9]'),'yyyy/MM', marks=pytest.mark.xfail(reason="cudf does no checks")),
        (StringGen('[0-9]{4}/[01][12]/[0-2][1-8]'),'yyyy/MM/dd'),
        (StringGen('[01][12]/[0-2][1-8]'), 'MM/dd'),
        (StringGen('[0-2][1-8]/[01][12]'), 'dd/MM'),
        (ConvertGen(DateGen(nullable=False), lambda d: d.strftime('%Y/%m').zfill(7), data_type=StringType()), 'yyyy/MM')]

# get invalid date string df
def invalid_date_string_df(spark):
    return spark.createDataFrame([['invalid_date_string']], "a string")

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen, ids=idfn)
@allow_non_gpu(*non_supported_tz_allow)
def test_string_to_unix_timestamp(data_gen, date_form, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).selectExpr("to_unix_timestamp(a, '{}')".format(date_form)),
        {'spark.sql.ansi.enabled': ansi_enabled})

def test_string_to_unix_timestamp_ansi_exception():
    assert_gpu_and_cpu_error(
        lambda spark : invalid_date_string_df(spark).selectExpr("to_unix_timestamp(a, '{}')".format('yyyy/MM/dd')).collect(),
        error_message="Exception",
        conf=ansi_enabled_conf)

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen, ids=idfn)
@allow_non_gpu(*non_supported_tz_allow)
def test_string_unix_timestamp(data_gen, date_form, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).select(f.unix_timestamp(f.col('a'), date_form)),
        {'spark.sql.ansi.enabled': ansi_enabled})

def test_string_unix_timestamp_ansi_exception():
    assert_gpu_and_cpu_error(
        lambda spark : invalid_date_string_df(spark).select(f.unix_timestamp(f.col('a'), 'yyyy/MM/dd')).collect(),
        error_message="Exception",
        conf=ansi_enabled_conf)


@disable_ansi_mode  # ANSI mode is tested separately.
@tz_sensitive_test
@pytest.mark.skipif(not is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
def test_to_timestamp(parser_policy):
    gen = StringGen("[0-9]{3}[1-9]-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]) ([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]")
    if get_test_tz() == "Asia/Shanghai":
        # ensure some times around transition are tested
        gen = gen.with_special_case("1991-04-14 02:00:00")\
        .with_special_case("1991-04-14 02:30:00")\
        .with_special_case("1991-04-14 03:00:00")\
        .with_special_case("1991-09-15 02:00:00")\
        .with_special_case("1991-09-15 02:30:00")\
        .with_special_case("1991-09-15 03:00:00")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, gen)
            .select(f.col("a"), f.to_timestamp(f.col("a"), "yyyy-MM-dd HH:mm:ss")),
        { "spark.sql.legacy.timeParserPolicy": parser_policy})


@tz_sensitive_test
@pytest.mark.skipif(not is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
@pytest.mark.parametrize("ansi_enabled", [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
def test_to_date(ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen)
            .select(f.to_date(f.col("a").cast('string'), "yyyy-MM-dd")),
        {'spark.sql.ansi.enabled': ansi_enabled})

@tz_sensitive_test
@pytest.mark.skipif(not is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
@pytest.mark.parametrize('data_gen', [StringGen('0[1-9][0-9]{4}')], ids=idfn)
def test_to_date_format_MMyyyy(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.to_date(f.col("a"), "MMyyyy")))

def test_to_date_ansi_exception():
    assert_gpu_and_cpu_error(
        lambda spark : invalid_date_string_df(spark).select(f.to_date(f.col("a"), "yyyy-MM-dd")).collect(),
        error_message="Exception",
        conf=ansi_enabled_conf)

supported_date_formats = ['yyyy-MM-dd', 'yyyy-MM', 'yyyy/MM/dd', 'yyyy/MM', 'dd/MM/yyyy',
                          'MM-dd', 'MM/dd', 'dd-MM', 'dd/MM']
@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', [date_gen], ids=idfn)
@allow_non_gpu('ProjectExec')
def test_date_format_for_date(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))

@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
# use 9999-12-30 instead of 9999-12-31 to avoid the issue: https://github.com/NVIDIA/spark-rapids/issues/10083
@pytest.mark.parametrize('data_gen', [TimestampGen(end=datetime(9999, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc))], ids=idfn)
@pytest.mark.skipif(not is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
def test_date_format_for_time(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))

@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
@pytest.mark.skipif(is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
@allow_non_gpu('ProjectExec')
def test_date_format_for_time_fall_back(data_gen, date_format):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'ProjectExec')

@pytest.mark.parametrize('date_format', supported_date_formats + ['yyyyMMdd'], ids=idfn)
# from 0001-02-01 to 9999-12-30 to avoid 'year 0 is out of range'
@pytest.mark.parametrize('data_gen', [LongGen(min_val=int(datetime(1, 2, 1).timestamp()), max_val=int(datetime(9999, 12, 30).timestamp()))], ids=idfn)
@pytest.mark.skipif(not is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
def test_from_unixtime(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, length=5).selectExpr("from_unixtime(a, '{}')".format(date_format)))

@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
# from 0001-02-01 to 9999-12-30 to avoid 'year 0 is out of range'
@pytest.mark.parametrize('data_gen', [LongGen(min_val=int(datetime(1, 2, 1).timestamp()), max_val=int(datetime(9999, 12, 30).timestamp()))], ids=idfn)
@pytest.mark.skipif(is_supported_time_zone(), reason="not all time zones are supported now, refer to https://github.com/NVIDIA/spark-rapids/issues/6839, please update after all time zones are supported")
def test_from_unixtime_fall_back(data_gen, date_format):
    assert_gpu_fallback_collect(lambda spark : unary_op_df(spark, data_gen, length=5).selectExpr("from_unixtime(a, '{}')".format(date_format)),
        'ProjectExec')

unsupported_date_formats = ['F']
@pytest.mark.parametrize('date_format', unsupported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu('ProjectExec')
def test_date_format_f(data_gen, date_format):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'DateFormatClass')

@pytest.mark.parametrize('date_format', unsupported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu('ProjectExec')
def test_date_format_f_incompat(data_gen, date_format):
    # note that we can't support it even with incompatibleDateFormats enabled
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'DateFormatClass', conf)

maybe_supported_date_formats = ['dd-MM-yyyy', 'yyyy-MM-dd HH:mm:ss.SSS', 'yyyy-MM-dd HH:mm:ss.SSSSSS']
@pytest.mark.parametrize('date_format', maybe_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu('ProjectExec')
def test_date_format_maybe(data_gen, date_format):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'DateFormatClass')

@pytest.mark.parametrize('date_format', maybe_supported_date_formats, ids=idfn)

@pytest.mark.parametrize('data_gen', [date_gen,
                                      # use 9999-12-30 instead of 9999-12-31 to avoid the issue: https://github.com/NVIDIA/spark-rapids/issues/10083
                                      TimestampGen(end=datetime(9999, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc))
                                      ], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_date_format_maybe_incompat(data_gen, date_format):
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)), conf)


@disable_ansi_mode  # ANSI mode tested separately.
# Reproduce conditions for https://github.com/NVIDIA/spark-rapids/issues/5670
# where we had a failure due to GpuCast canonicalization with timezone.
# In this case it was doing filter after project, the way I get that to happen is by adding in the
# input_file_name(), otherwise filter happens before project.
@allow_non_gpu("CollectLimitExec", "FileSourceScanExec" ,"DeserializeToObjectExec", *non_utc_allow)
@ignore_order()
def test_date_format_mmyyyy_cast_canonicalization(spark_tmp_path):
    data_path = spark_tmp_path + '/CSV_DATA'
    gen = StringGen(pattern='[0][0-9][1][8-9][1-9][1-9]', nullable=False)
    schema = gen.data_type
    with_cpu_session(lambda spark : gen_df(spark, gen, length=100).write.csv(data_path))
    def do_join_cast(spark):
        left = spark.read.csv(data_path)\
            .selectExpr("date_format(to_date(_c0, 'MMyyyy'), 'MM/dd/yyyy') as monthly_reporting_period", "substring_index(substring_index(input_file_name(),'/',-1),'.',1) as filename")
        right = spark.read.csv(data_path).withColumnRenamed("_c0", "r_c0")\
            .selectExpr("date_format(to_date(r_c0, 'MMyyyy'), 'MM/dd/yyyy') as monthly_reporting_period", "substring_index(substring_index(input_file_name(),'/',-1),'.',1) as filename")\
            .withColumnRenamed("monthly_reporting_period", "r_monthly_reporting_period")\
            .withColumnRenamed("filename", "r_filename")
        return left.join(right, left.monthly_reporting_period == right.r_monthly_reporting_period, how='inner')
    assert_gpu_and_cpu_are_equal_collect(do_join_cast)


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_unsupported_fallback_date_format(data_gen):
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_fallback_collect(
        lambda spark : gen_df(spark, [("a", data_gen)]).selectExpr(
            "date_format(a, a)"),
        "DateFormatClass",
        conf)


@disable_ansi_mode  # Failure cases for ANSI mode are tested separately.
@allow_non_gpu('ProjectExec')
def test_unsupported_fallback_to_date():
    date_gen = StringGen(pattern="2023-08-01")
    pattern_gen = StringGen(pattern="[M]")
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", date_gen), ("b", pattern_gen)]).selectExpr(
            "to_date(a, b)"),
        'GetTimestamp',
        conf)


# (-62135510400, 253402214400) is the range of seconds that can be represented by timestamp_seconds 
# considering the influence of time zone.
ts_float_gen = SetValuesGen(FloatType(), [0.0, -0.0, 1.0, -1.0, 1.234567, -1.234567, 16777215.0, float('inf'), float('-inf'), float('nan')])
seconds_gens = [LongGen(min_val=-62135510400, max_val=253402214400), IntegerGen(), ShortGen(), ByteGen(),
                DoubleGen(min_exp=0, max_exp=32), ts_float_gen, DecimalGen(16, 6), DecimalGen(13, 3), DecimalGen(10, 0), DecimalGen(7, -3), DecimalGen(6, 6)]
@pytest.mark.parametrize('data_gen', seconds_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_seconds(a)"))

@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds_long_overflow():
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, long_gen).selectExpr("timestamp_seconds(a)").collect(),
        conf={},
        error_message='long overflow')

# For Decimal(20, 7) case, the data is both 'Overflow' and 'Rounding necessary', this case is to verify
# that 'Rounding necessary' check is before 'Overflow' check. So we should make sure that every decimal
# value in test data is 'Rounding necessary' by setting full_precision=True to avoid leading and trailing zeros.
# Otherwise, the test data will bypass the 'Rounding necessary' check and throw an 'Overflow' error.
@pytest.mark.parametrize('data_gen', [DecimalGen(7, 7, full_precision=True), DecimalGen(20, 7, full_precision=True)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds_rounding_necessary(data_gen):
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_seconds(a)").collect(),
        conf={},
        error_message='Rounding necessary')
    
@pytest.mark.parametrize('data_gen', [DecimalGen(19, 6), DecimalGen(20, 6)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds_decimal_overflow(data_gen):
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_seconds(a)").collect(),
        conf={},
        error_message='Overflow')

millis_gens = [LongGen(min_val=-62135410400000, max_val=253402214400000), IntegerGen(), ShortGen(), ByteGen()]
@pytest.mark.parametrize('data_gen', millis_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_millis(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_millis(a)"))
    
@allow_non_gpu(*non_utc_allow)
def test_timestamp_millis_long_overflow():
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, long_gen).selectExpr("timestamp_millis(a)").collect(),
        conf={},
        error_message='long overflow')

micros_gens = [LongGen(min_val=-62135510400000000, max_val=253402214400000000), IntegerGen(), ShortGen(), ByteGen()]
@pytest.mark.parametrize('data_gen', micros_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_micros(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_micros(a)"))
