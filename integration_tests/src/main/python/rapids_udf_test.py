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

from asserts import assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from spark_session import with_spark_session

def skip_if_no_hive(spark):
    if spark.conf.get("spark.sql.catalogImplementation") != "hive":
        pytest.skip("The Spark session does not have Hive support")

def test_hive_simple_udf():
    with_spark_session(skip_if_no_hive)
    data_gens = [["i", int_gen], ["s", StringGen('([^%]{0,1}(%[0-9A-F][0-9A-F]){0,1}){0,30}')]]
    def evalfn(spark):
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS urldecode")
        spark.sql("CREATE TEMPORARY FUNCTION urldecode AS 'com.nvidia.spark.rapids.udf.URLDecode'")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_simple_udf_test_table",
        "SELECT i, urldecode(s) FROM hive_simple_udf_test_table")
