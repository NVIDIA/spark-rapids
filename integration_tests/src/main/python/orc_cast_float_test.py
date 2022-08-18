# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from pyspark.sql.types import *
from spark_session import with_cpu_session


@pytest.mark.parametrize('to_type', ['float', 'double', 'boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_from_float_and_double(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_casting_from_float_and_double'
    data_gen = [('float_column', float_gen), ('double_column', double_gen)]
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen).write.orc(orc_path)
    )
    schema_str = "float_column {}, double_column {}".format(to_type, to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )


@pytest.mark.parametrize('data_gen', [DoubleGen(max_exp=32, special_cases=None),
                                      DoubleGen(max_exp=32, special_cases=[8.88e9, 9.99e10, 1.314e11])])
def test_casting_from_double_to_timestamp(spark_tmp_path, data_gen):
    # ORC will assume the original double value in seconds, we need to convert them to
    # timestamp(INT64 in micro-seconds).
    #
    # The 'datetime' module in python requires 0 <= year < 10000, and UTC timestamp is start from 1970/1/1.
    # That is, the minimum valid negative number is -1970 * 365 * 24 * 3600 = -62125920000 -> 6e10 -> 2^32.
    # So we set max_exp = 32 in DoubleGen.
    #
    # The maximum valid positive number is (10000 - 1970) * 365 * 24 * 3600 = 253234080000 -> 2e11 -> 2^37,
    # so we add some special cases from 2^33 - 2^37 (8e9 ~ 1e11).
    #
    # In DoubleGen, special_case=None will generate some NaN, INF corner cases.

    orc_path = spark_tmp_path + '/orc_casting_from_double_to_timestamp'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).write.orc(orc_path)
    )
    # the name of unique column is 'a', cast it into timestamp type
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(orc_path)
    )


def test_casting_from_overflow_double_to_timestamp(spark_tmp_path):
    orc_path = spark_tmp_path + '/orc_casting_from_overflow_double_to_timestamp'
    with_cpu_session(
        lambda spark: unary_op_df(spark, DoubleGen(min_exp=38)).write.orc(orc_path)
    )
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.read.schema("a timestamp").orc(orc_path).collect(),
        conf={},
        error_message="ArithmeticException"
    )
