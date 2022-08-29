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

@pytest.mark.parametrize('to_type', ['boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_string_to_integers(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_cast_string_to_int'
    normal_cases = []
    length = 2048
    for _ in range(0, length):
        normal_cases.append(str(random.randint(INT_MIN, INT_MAX)))

    special_cases = [
        "0000", "00123", "-00123", "+00123",         # leading zeros, valid cases
        "+0", "-0", "+1", "-1", "+000", "-000",      # positive or negative sign, valid cases
        "    ", "  1", "1  ", " 1  2 ",              # leading/trailing spaces, invalid
        "+-0", "1e3", "1-0", "0.1",                  # invalid integer format
        "+", "-", "0xFF",                            # other invalid cases
        "true", "false", "True", "False"
    ]
    # convert each case into a data row
    test_cases = [[x] for x in normal_cases + special_cases]
    with_cpu_session(
        func=lambda spark: spark.createDataFrame(data=test_cases,
                                                 schema=["int_str"]).write.orc(orc_path)
    )
    schema_str = "int_str {}".format(to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )


@pytest.mark.approximate_float
@pytest.mark.parametrize('to_type', ['float', 'double'])
def test_casting_string_to_float(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_cast_string_to_float'
    # INF and NaN are case-sensitive
    normal_cases = [
        'Infinity', '+Infinity', '-Infinity', 'NaN', '+NaN', '-NaN',
        str(FLOAT_MAX), str(FLOAT_MIN), str(DOUBLE_MAX), str(DOUBLE_MIN)
    ]
    length = 2048
    for _ in range(0, length):
        normal_cases.append(str(random.uniform(DOUBLE_MIN, DOUBLE_MAX)))
    special_cases = [
        "123", "00123.00", "123.", "-0123.", ".123", "-.123",    # different positions of decimal point, valid
        "+0", "-0", "+00.0", "-00.0", "-00000.",                 # zero values, valid
        "    000123.00   ", "   -NaN ", " +Infinity ",           # valid cases with whitespaces
        "+01.234e10", "3.14e10", "03.14E+015", ".123e001",       # scientific notation
        "123.e00100", "-.123e10",
        "9.999e99999", "0.00000001e-999999",                     # overflow, convert it to INF, 0.0
        "    ", ".", "inf", "nan", "true", "abc",                # some invalid cases
        "1.2.", "1.2.3", "3.14e3.14"
    ]
    test_cases = [[x] for x in normal_cases + special_cases]
    with_cpu_session(
        func=lambda spark: spark.createDataFrame(data=test_cases,
                                                 schema=["float_str"]).write.orc(orc_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("float_str {}".format(to_type)).orc(orc_path)
    )


'''
FIXME & TODO
1. There is a strange case, if input string is "9808-02-30", intuitively, we should return null.
However, CPU spark output 9808-02-29. Need to figure whether if it's a bug or a feature.
2. For all 'random' callings, add seed for them.
'''
def test_casting_string_to_date(spark_tmp_path):
    orc_path = spark_tmp_path + '/orc_cast_string_to_date'
    length = 2048
    normal_cases = []
    for _ in range(0, length):
        year = str(random.randint(0, 9999)).zfill(4)
        month = str(random.randint(1, 12)).zfill(2)
        day = str(random.randint(1, 31)).zfill(2)
        normal_cases.append('{}-{}-{}'.format(year, month, day))
    test_cases = [[x] for x in normal_cases]
    with_cpu_session(
        func=lambda spark: spark.createDataFrame(data=test_cases,
                                                 schema=["date_str"]).write.orc(orc_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("date_str date").orc(orc_path)
    )
