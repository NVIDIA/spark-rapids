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
from datetime import date, datetime, timedelta, timezone
from marks import incompat
from pyspark.sql.types import *
import pyspark.sql.functions as f

def unary_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([('a', gen)], nullable=False), length=length, seed=seed)

date_gen = [DateGen()]
date_n_time_gen = [DateGen(), TimestampGen()]

@pytest.mark.parametrize('data_gen', date_gen, ids=idfn)
def test_year(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.year(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gen, ids=idfn)
def test_month(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.month(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gen, ids=idfn)
def test_dayofmonth(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.dayofmonth(f.col('a'))))

@incompat #Really only the string is
@pytest.mark.parametrize('data_gen', date_n_time_gen, ids=idfn)
def test_unix_time(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col('a'))))

str_date_and_format_gen = [pytest.param(StringGen('[0-9]{1,4}/[01][0-9]'),'yyyy/MM', marks=pytest.mark.xfail(reason="cudf does no checks")),
        (StringGen('[0-9]{4}/[01][12]/[0-2][1-8]'),'yyyy/MM/dd'),
        (ConvertGen(DateGen(nullable=False), lambda d: d.strftime('%Y/%m').zfill(7), data_type=StringType()), 'yyyy/MM')]

@incompat
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen, ids=idfn)
def test_string_unix_time(data_gen, date_form):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : debug_df(unary_op_df(spark, data_gen, seed=1, length=10)).select(f.unix_timestamp(f.col('a'), date_form)))


