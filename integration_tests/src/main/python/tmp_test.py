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


def create_orc(data_gen_list, data_path):
    # generate ORC dataframe, and dump it to local file 'data_path'
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen_list).write.mode('overwrite').orc(data_path)
    )

# TODO: merge test_casting_from_float and test_casting_from_double into one test
# TODO: Need a float_gen with range [a, b], if float/double >= 1e13, then float/double -> timestamp will overflow
'''
We need this test cases:
1. val * 1e3 <= LONG_MAX && val * 1e6 <= LONG_MAX  (no overflow)
2. val * 1e3 <= LONG_MAX && val * 1e6 > LONG_MAX   (caught java.lang.ArithmeticException)
3. val * 1e3 > LONG_MAX  (caught java.lang.ArithmeticException)
'''
@pytest.mark.parametrize('to_type', ['double', 'boolean', 'tinyint', 'smallint', 'int', 'bigint', 'timestamp'])
def test_casting_from_float(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_casting_from_float'
    data_gen = [('float_column', float_gen)]
    create_orc(data_gen, orc_path)
    schema_str = "float_column {}".format(to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )




@pytest.mark.parametrize('to_type', ['float', 'boolean', 'tinyint', 'smallint', 'int', 'bigint', 'timestamp'])
def test_casting_from_double(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_casting_from_double'
    data_gen = [('double_column', float_gen)]
    create_orc(data_gen, orc_path)
    schema_str = "double_column {}".format(to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )

