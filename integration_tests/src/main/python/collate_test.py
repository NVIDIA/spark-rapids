# Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, \
    assert_gpu_sql_fallback_collect
from data_gen import *
from marks import allow_non_gpu
from spark_session import is_before_spark_400


####################################################################################################
# Gpu only supports StringType, aka StringType(UTF8_BINARY, NoConstraint)
####################################################################################################

@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@allow_non_gpu("ProjectExec", "Collate")
def test_collate_column_fallback():
    data_gen = [("c1", string_gen), ("c2", string_gen)]
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("contains(collate(c1, 'UTF8_BINARY'), c2)"),
        cpu_fallback_class_name="Collate")


# Test non-UTF8_BINARY string literal
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@allow_non_gpu("ProjectExec", "Concat")
def test_collate_literal_fallback():
    data_gen = [("c1", string_gen)]
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("concat(c1, '_tail' COLLATE UNICODE)"),
        cpu_fallback_class_name="Literal")


# Explicitly specify StringType, aka StringType(UTF8_BINARY, NoConstraint)
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
def test_collate_literal():
    data_gen = [("c1", string_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("concat(c1, '_tail' COLLATE UTF8_BINARY)"))


@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@allow_non_gpu("ProjectExec", "SortAggregateExec", "ShuffleExchangeExec", "SortExec")
def test_collate_sum_fallback():
    data_gen = [("c1", string_gen)]
    assert_gpu_sql_fallback_collect(
        lambda spark: gen_df(spark, data_gen),
        cpu_fallback_class_name="Collate",
        table_name="tab",
        sql="select collate(c1, 'utf8_lcase'), count(*) from tab group by collate(c1, 'utf8_lcase')")


# Test `preserveCharVarcharTypeInfo` is true; char/varchar type
# The CPU plan is: Contains(static_invoke(CharVarcharCodegenUtils.readSidePadding(char_col#8, 5)), a).
# Both scan and project fall back to CPU, because scan and project input do not support CharType/VarcharType
# and more Gpu does not support `static_invoke`.
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 32x, 33x do not support char/varchar type; Spark 34x, 35x throw exception")
@pytest.mark.parametrize('char_type', ["char(5)", "varchar(5)"])
@allow_non_gpu("ProjectExec", "ColumnarToRowExec", "FileSourceScanExec")
def test_char_varchar_fallback_preserve_enabled(spark_tmp_path, char_type):
    preserve_char_conf = {"spark.sql.preserveCharVarcharTypeInfo": True}
    file_path = spark_tmp_path + '/PARQUET_DATA'
    data = [("a",), ("ab",), ("abc",)]
    schema = f"char_col {char_type}"

    # Writing with `preserveCharVarcharTypeInfo` enabled, so reading back keeps char/varchar type.
    # And also, set this config to avoid error: Logical plan should not have output of char/varchar
    # type when spark.sql.preserveCharVarcharTypeInfo is false
    with_cpu_session(
        lambda spark: spark.createDataFrame(data, schema).write.parquet(file_path),
        conf=preserve_char_conf)

    assert_gpu_fallback_collect(
        lambda spark: spark.read.parquet(file_path).selectExpr("contains(char_col, 'a')"),
        cpu_fallback_class_name="Contains",
        conf=preserve_char_conf)


# Test `preserveCharVarcharTypeInfo` is false(default value); char type
# The CPU plan is: Contains(static_invoke(CharVarcharCodegenUtils.readSidePadding(char_col#8, 5)), a)
# Spark scan treats char as StringType.
# Contains falls back because the child `static_invoke` is not supported by GPU.
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 32x, 33x do not support char/varchar type; Spark 34x, 35x throw exception")
@allow_non_gpu("ProjectExec")
def test_char_fallback_preserve_disabled(spark_tmp_path):
    preserve_char_conf = {"spark.sql.preserveCharVarcharTypeInfo": True}
    file_path = spark_tmp_path + '/PARQUET_DATA'
    data = [("a",), ("ab",), ("abc",)]
    schema = f"char_col char(5)"

    # Writing with `preserveCharVarcharTypeInfo` enabled, so reading back keeps char/varchar type.
    # And also, set this config to avoid error: Logical plan should not have output of char/varchar
    # type when spark.sql.preserveCharVarcharTypeInfo is false
    with_cpu_session(
        lambda spark: spark.createDataFrame(data, schema).write.parquet(file_path),
        conf=preserve_char_conf)

    assert_gpu_fallback_collect(
        # when read from the Parquet file with `preserveCharVarcharTypeInfo,
        # the char_col is still char/varchar type.
        lambda spark: spark.read.parquet(file_path).selectExpr("contains(char_col, 'a')"),
        cpu_fallback_class_name="Contains")


# Test `preserveCharVarcharTypeInfo` is false(default value); varchar type
# Spark treats varchar as StringType, it's transparent to GPU, so this case can run on GPU.
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 32x, 33x do not support char/varchar type; Spark 34x, 35x throw exception")
@pytest.mark.parametrize('char_type', ["varchar(5)"])
@allow_non_gpu("ProjectExec", "StaticInvoke")
def test_varchar_preserve_disabled(spark_tmp_path, char_type):
    preserve_char_conf = {"spark.sql.preserveCharVarcharTypeInfo": True}
    file_path = spark_tmp_path + '/PARQUET_DATA'
    data = [("a",), ("ab",), ("abc",)]
    schema = f"char_col {char_type}"

    # Writing with `preserveCharVarcharTypeInfo` enabled, so reading back keeps char/varchar type.
    # And also, set this config to avoid error: Logical plan should not have output of char/varchar
    # type when spark.sql.preserveCharVarcharTypeInfo is false
    with_cpu_session(
        lambda spark: spark.createDataFrame(data, schema).write.parquet(file_path),
        conf=preserve_char_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(file_path).selectExpr("contains(char_col, 'a')"))
