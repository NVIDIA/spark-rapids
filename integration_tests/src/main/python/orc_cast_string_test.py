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
def test_casting_from_string(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_casting_from_string'
    normal_cases = []
    length = 2048
    while len(normal_cases) < length:
        normal_cases.append(str(random.randint(INT_MIN, INT_MAX)))

    special_cases = [
        "0000", "00123", "-00123", "+00123",     # leading zeros, valid cases
        "+0", "-0", "+1", "-1", "+000", "-000",  # positive or negative sign, valid cases
        "    ", "  1", "1  ", "1  2",            # leading/trailing spaces, invalid
        "+-0", "1e3", "1-0", "0.1",              # invalid integer format
        "true", "false", "True", "False"         # other invalid cases
    ]
    # convert each case into a data row
    test_cases = [[x] for x in normal_cases + special_cases]
    with_cpu_session(
        func=lambda spark: spark.createDataFrame(data=test_cases,
                                                 schema=["string_col"]).write.orc(orc_path)
    )
    schema_str = "string_col {}".format(to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )
