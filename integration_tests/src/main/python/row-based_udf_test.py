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

from asserts import assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from spark_session import with_spark_session
from rapids_udf_test import skip_if_no_hive, load_hive_udf_or_skip_test

def test_hive_empty_simple_udf():
    with_spark_session(skip_if_no_hive)
    data_gens = [["i", int_gen], ["s", string_gen]]
    def evalfn(spark):
        load_hive_udf_or_skip_test(spark, "emptysimple", "com.nvidia.spark.rapids.udf.hive.EmptyHiveSimpleUDF")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_simple_udf_test_table",
        "SELECT i, emptysimple(s, 'const_string') FROM hive_simple_udf_test_table",
        conf={'spark.rapids.sql.rowBasedUDF.enabled': 'true'})

def test_hive_empty_generic_udf():
    with_spark_session(skip_if_no_hive)
    def evalfn(spark):
        load_hive_udf_or_skip_test(spark, "emptygeneric", "com.nvidia.spark.rapids.udf.hive.EmptyHiveGenericUDF")
        return gen_df(spark, [["s", string_gen]])
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_generic_udf_test_table",
        "SELECT emptygeneric(s) FROM hive_generic_udf_test_table",
        conf={'spark.rapids.sql.rowBasedUDF.enabled': 'true'})
