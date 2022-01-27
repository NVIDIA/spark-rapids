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

#
# This file depends on external udf-examples jar built from
# https://github.com/NVIDIA/spark-rapids-examples/tree/branch-xx.xx/examples/Spark-Rapids/udf-examples.
# By default the cases are disable, are only used by Jenkins jobs to test JNI UDF test cases.
#
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import rapids_udf_example_native
from spark_session import with_spark_session
from pyspark.sql.utils import AnalysisException
from conftest import skip_unless_precommit_tests

def drop_udf(spark, udfname):
    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS {}".format(udfname))

def skip_if_no_hive(spark):
    if spark.conf.get("spark.sql.catalogImplementation") != "hive":
        skip_unless_precommit_tests('The Spark session does not have Hive support')

def load_hive_udf(spark, udfname, udfclass):
    drop_udf(spark, udfname)
    try:
        spark.sql("CREATE TEMPORARY FUNCTION {} AS '{}'".format(udfname, udfclass))
    except AnalysisException as e:
        # rethrow this error, or test case will be skipped silently
        raise RuntimeError("UDF {} failed to load, check if {} is in the class path, error is {}".format(udfname, udfclass, repr(e)))

@rapids_udf_example_native
def test_hive_simple_udf_native(enable_rapids_udf_example_native):
    with_spark_session(skip_if_no_hive)
    data_gens = [["s", StringGen('.{0,30}')]]
    def evalfn(spark):
        load_hive_udf(spark, "wordcount", "com.nvidia.spark.rapids.udf.hive.StringWordCount")
        return gen_df(spark, data_gens)
    assert_gpu_and_cpu_are_equal_sql(
        evalfn,
        "hive_native_udf_test_table",
        "SELECT wordcount(s) FROM hive_native_udf_test_table")

def load_java_udf(spark, udfname, udfclass, udf_return_type=None):
    drop_udf(spark, udfname)
    try:
        spark.udf.registerJavaFunction(udfname, udfclass, udf_return_type)
    except AnalysisException as e:
        # rethrow this error, or test case will be skipped silently
        raise RuntimeError("UDF {} failed to load, check if {} is in the class path, error is {}".format(udfname, udfclass, repr(e)))

@rapids_udf_example_native
def test_java_cosine_similarity_reasonable_range(enable_rapids_udf_example_native):
    def evalfn(spark):
        class RangeFloatGen(FloatGen):
            def start(self, rand):
                self._start(rand, lambda: rand.uniform(-1000.0, 1000.0))
        load_java_udf(spark, "cosine_similarity", "com.nvidia.spark.rapids.udf.java.CosineSimilarity")
        arraygen = ArrayGen(RangeFloatGen(nullable=False, no_nans=True, special_cases=[]), min_length=8, max_length=8)
        df = binary_op_df(spark, arraygen)
        return df.selectExpr("cosine_similarity(a, b)")
    assert_gpu_and_cpu_are_equal_collect(evalfn)

@rapids_udf_example_native
def test_java_cosine_similarity_with_nans(enable_rapids_udf_example_native):
    def evalfn(spark):
        load_java_udf(spark, "cosine_similarity", "com.nvidia.spark.rapids.udf.java.CosineSimilarity")
        arraygen = ArrayGen(FloatGen(nullable=False), min_length=8, max_length=8)
        return binary_op_df(spark, arraygen).selectExpr("cosine_similarity(a, b)")
    assert_gpu_and_cpu_are_equal_collect(evalfn)
