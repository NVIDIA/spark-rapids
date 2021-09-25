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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import rapids_udf_example_native
from spark_session import with_spark_session
from pyspark.sql.utils import AnalysisException
from conftest import skip_unless_precommit_tests

encoded_url_gen = StringGen('([^%]{0,1}(%[0-9A-F][0-9A-F]){0,1}){0,30}')

def drop_udf(spark, udfname):
    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS {}".format(udfname))

def skip_if_no_hive(spark):
    if spark.conf.get("spark.sql.catalogImplementation") != "hive":
        skip_unless_precommit_tests('The Spark session does not have Hive support')

def load_hive_udf_or_skip_test(spark, udfname, udfclass):
    drop_udf(spark, udfname)
    try:
        spark.sql("CREATE TEMPORARY FUNCTION {} AS '{}'".format(udfname, udfclass))
    except AnalysisException:
        skip_unless_precommit_tests("UDF {} failed to load, udf-examples jar is probably missing".format(udfname))

def test_hive_simple_udf():
    with_spark_session(skip_if_no_hive)
    data_gens = [["i", int_gen], ["s", encoded_url_gen]]
    def evalfn(spark):
        load_hive_udf_or_skip_test(spark, "urldecode", "com.nvidia.spark.rapids.udf.hive.URLDecode")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_simple_udf_test_table",
        "SELECT i, urldecode(s) FROM hive_simple_udf_test_table")

def test_hive_generic_udf():
    with_spark_session(skip_if_no_hive)
    data_gens = [["s", StringGen('.{0,30}')]]
    def evalfn(spark):
        load_hive_udf_or_skip_test(spark, "urlencode", "com.nvidia.spark.rapids.udf.hive.URLEncode")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_generic_udf_test_table",
        "SELECT urlencode(s) FROM hive_generic_udf_test_table")

@rapids_udf_example_native
def test_hive_simple_udf_native(enable_rapids_udf_example_native):
    with_spark_session(skip_if_no_hive)
    data_gens = [["s", StringGen('.{0,30}')]]
    def evalfn(spark):
        load_hive_udf_or_skip_test(spark, "wordcount", "com.nvidia.spark.rapids.udf.hive.StringWordCount")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_native_udf_test_table",
        "SELECT wordcount(s) FROM hive_native_udf_test_table")

def load_java_udf_or_skip_test(spark, udfname, udfclass):
    drop_udf(spark, udfname)
    try:
        spark.udf.registerJavaFunction(udfname, udfclass)
    except AnalysisException:
        skip_unless_precommit_tests("UDF {} failed to load, udf-examples jar is probably missing".format(udfname))

def test_java_url_decode():
    def evalfn(spark):
        load_java_udf_or_skip_test(spark, 'urldecode', 'com.nvidia.spark.rapids.udf.java.URLDecode')
        return unary_op_df(spark, encoded_url_gen).selectExpr("urldecode(a)")
    assert_gpu_and_cpu_are_equal_collect(evalfn)

def test_java_url_encode():
    def evalfn(spark):
        load_java_udf_or_skip_test(spark, 'urlencode', 'com.nvidia.spark.rapids.udf.java.URLEncode')
        return unary_op_df(spark, StringGen('.{0,30}')).selectExpr("urlencode(a)")
    assert_gpu_and_cpu_are_equal_collect(evalfn)

@rapids_udf_example_native
def test_java_cosine_similarity_reasonable_range(enable_rapids_udf_example_native):
    def evalfn(spark):
        class RangeFloatGen(FloatGen):
            def start(self, rand):
                self._start(rand, lambda: rand.uniform(-1000.0, 1000.0))
        load_java_udf_or_skip_test(spark, "cosine_similarity", "com.nvidia.spark.rapids.udf.java.CosineSimilarity")
        arraygen = ArrayGen(RangeFloatGen(nullable=False, no_nans=True, special_cases=[]), min_length=8, max_length=8)
        df = binary_op_df(spark, arraygen)
        return df.selectExpr("cosine_similarity(a, b)")
    assert_gpu_and_cpu_are_equal_collect(evalfn)

@rapids_udf_example_native
def test_java_cosine_similarity_with_nans(enable_rapids_udf_example_native):
    def evalfn(spark):
        load_java_udf_or_skip_test(spark, "cosine_similarity", "com.nvidia.spark.rapids.udf.java.CosineSimilarity")
        arraygen = ArrayGen(FloatGen(nullable=False), min_length=8, max_length=8)
        return binary_op_df(spark, arraygen).selectExpr("cosine_similarity(a, b)")
    assert_gpu_and_cpu_are_equal_collect(evalfn)
