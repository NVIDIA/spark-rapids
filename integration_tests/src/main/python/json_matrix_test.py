# Copyright (c) 2024, NVIDIA CORPORATION.
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

import pyspark.sql.functions as f
import pytest

from asserts import *
from data_gen import *
from conftest import is_not_utc
from datetime import timezone
from conftest import is_databricks_runtime
from marks import approximate_float, allow_non_gpu, ignore_order, datagen_overrides
from spark_session import *

def read_json_df(data_path, schema, spark_tmp_table_factory_ignored, options = {}):
    def read_impl(spark):
        reader = spark.read
        if not schema is None:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return debug_df(reader.json(data_path))
    return read_impl

def read_json_sql(data_path, schema, spark_tmp_table_factory, options = {}):
    opts = options
    if not schema is None:
        opts = copy_and_update(options, {'schema': schema})
    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='json', path=data_path, **opts)
    return read_impl

def read_json_as_text(spark, data_path, column_name):
    return spark.read.text(data_path).withColumnRenamed("value", column_name)

TEXT_INPUT_EXEC='FileSourceScanExec'

_enable_all_types_json_scan_conf = {
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true',
    'spark.rapids.sql.json.read.float.enabled': 'true',
    'spark.rapids.sql.json.read.double.enabled': 'true',
    'spark.rapids.sql.json.read.decimal.enabled': 'true'
}

_enable_json_to_structs_conf = {
    'spark.rapids.sql.expression.JsonToStructs': 'true'
}

_enable_get_json_object_conf = {
    'spark.rapids.sql.expression.GetJsonObject': 'true'
}

_enable_json_tuple_conf = {
    'spark.rapids.sql.expression.JsonTuple': 'true'
}

WITH_COMMENTS_FILE = "withComments.json"
WITH_COMMENTS_SCHEMA = StructType([StructField("str", StringType())])

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_comments_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_COMMENTS_FILE,
        WITH_COMMENTS_SCHEMA,
        spark_tmp_table_factory,
        {"allowComments": "true"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'ProjectExec')
def test_from_json_allow_comments_on(std_input_path):
    schema = WITH_COMMENTS_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowComments': "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_comments_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_COMMENTS_FILE,
        WITH_COMMENTS_SCHEMA,
        spark_tmp_table_factory,
        {"allowComments": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_comments_off(std_input_path):
    schema = WITH_COMMENTS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowComments': "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10194')
def test_get_json_object_allow_comments_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''),
        conf =_enable_get_json_object_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_json_tuple_allow_comments_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_COMMENTS_FILE, "json").selectExpr('''json_tuple(json, "str")'''),
        conf =_enable_json_tuple_conf)

WITH_SQ_FILE = "withSingleQuotes.json"
WITH_SQ_SCHEMA = StructType([StructField("str", StringType())])

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_single_quotes_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_SQ_FILE,
        WITH_SQ_SCHEMA,
        spark_tmp_table_factory,
        {"allowSingleQuotes": "false"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu('ProjectExec', TEXT_INPUT_EXEC)
def test_from_json_allow_single_quotes_off(std_input_path):
    schema = WITH_SQ_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowSingleQuotes': "false"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# On is the default so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_single_quotes_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_SQ_FILE,
        WITH_SQ_SCHEMA,
        spark_tmp_table_factory,
        {"allowSingleQuotes": "true"}),
        conf=_enable_all_types_json_scan_conf)

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_single_quotes_on(std_input_path):
    schema = WITH_SQ_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowSingleQuotes': "true"})),
        conf =_enable_json_to_structs_conf)

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_single_quotes_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''),
        conf =_enable_get_json_object_conf)

# On is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_single_quotes_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_SQ_FILE, "json").selectExpr('''json_tuple(json, "str")'''),
        conf =_enable_json_tuple_conf)

WITH_UNQUOTE_FIELD_NAMES_FILE = "withUnquotedFieldNames.json"
WITH_UNQUOTE_FIELD_NAMES_SCHEMA = StructType([StructField("str", StringType())])

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_field_names_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE,
        WITH_UNQUOTE_FIELD_NAMES_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedFieldNames": "true"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu('ProjectExec', TEXT_INPUT_EXEC)
def test_from_json_allow_unquoted_field_names_on(std_input_path):
    schema = WITH_UNQUOTE_FIELD_NAMES_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowUnquotedFieldNames': "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_field_names_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE,
        WITH_UNQUOTE_FIELD_NAMES_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedFieldNames": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_field_names_on(std_input_path):
    schema = WITH_UNQUOTE_FIELD_NAMES_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {'allowUnquotedFieldNames': "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_get_json_object_allow_unquoted_field_names_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''),
        conf =_enable_get_json_object_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_json_tuple_allow_unquoted_field_names_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTE_FIELD_NAMES_FILE, "json").selectExpr('''json_tuple(json, "str")'''),
        conf =_enable_json_tuple_conf)

WITH_NUMERIC_LEAD_ZEROS_FILE = "withNumericLeadingZeros.json"
WITH_NUMERIC_LEAD_ZEROS_SCHEMA = StructType([StructField("byte", ByteType()),
    StructField("int", IntegerType()),
    StructField("float", FloatType()),
    StructField("decimal", DecimalType(10, 3))])

@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_numeric_leading_zeros_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE,
        WITH_NUMERIC_LEAD_ZEROS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNumericLeadingZeros": "true"}),
        conf=_enable_all_types_json_scan_conf)

@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_numeric_leading_zeros_on(std_input_path):
    schema = WITH_NUMERIC_LEAD_ZEROS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNumericLeadingZeros": "true"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@approximate_float()
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/9588')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_numeric_leading_zeros_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE,
        WITH_NUMERIC_LEAD_ZEROS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNumericLeadingZeros": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default so it really needs to work
@approximate_float()
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/9588')
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_numeric_leading_zeros_off(std_input_path):
    schema = WITH_NUMERIC_LEAD_ZEROS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNumericLeadingZeros": "false"})),
        conf =_enable_json_to_structs_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_get_json_object_allow_numeric_leading_zeros_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").selectExpr('''get_json_object(json, "$.byte")''',
            '''get_json_object(json, "$.int")''', '''get_json_object(json, "$.float")''','''get_json_object(json, "$.decimal")'''),
        conf =_enable_get_json_object_conf)

# Off is the default so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_json_tuple_allow_numeric_leading_zeros_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NUMERIC_LEAD_ZEROS_FILE, "json").selectExpr('''json_tuple(json, "byte", "int", "float", "decimal")'''),
        conf =_enable_json_tuple_conf)

WITH_NONNUMERIC_NUMBERS_FILE = "withNonnumericNumbers.json"
WITH_NONNUMERIC_NUMBERS_SCHEMA = StructType([
    StructField("float", FloatType()),
    StructField("double", DoubleType())])

@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.xfail(condition = is_before_spark_330(), reason = 'https://github.com/NVIDIA/spark-rapids/issues/10493')
def test_scan_json_allow_nonnumeric_numbers_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE,
        WITH_NONNUMERIC_NUMBERS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNonNumericNumbers": "false"}),
        conf=_enable_all_types_json_scan_conf)

@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10456')
@pytest.mark.xfail(condition = is_before_spark_330(), reason = 'https://github.com/NVIDIA/spark-rapids/issues/10493')
def test_from_json_allow_nonnumeric_numbers_off(std_input_path):
    schema = WITH_NONNUMERIC_NUMBERS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNonNumericNumbers": "false"})),
        conf =_enable_json_to_structs_conf)

# On is the default for scan so it really needs to work
@approximate_float()
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.xfail(condition = is_before_spark_330(), reason = 'https://github.com/NVIDIA/spark-rapids/issues/10493')
def test_scan_json_allow_nonnumeric_numbers_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE,
        WITH_NONNUMERIC_NUMBERS_SCHEMA,
        spark_tmp_table_factory,
        {"allowNonNumericNumbers": "true"}),
        conf=_enable_all_types_json_scan_conf)

# On is the default for from_json so it really needs to work
@approximate_float()
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.xfail(condition = is_before_spark_330(), reason = 'https://github.com/NVIDIA/spark-rapids/issues/10493')
@pytest.mark.xfail(condition = is_spark_330_or_later(), reason = 'https://github.com/NVIDIA/spark-rapids/issues/10494')
def test_from_json_allow_nonnumeric_numbers_on(std_input_path):
    schema = WITH_NONNUMERIC_NUMBERS_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowNonNumericNumbers": "true"})),
        conf =_enable_json_to_structs_conf)

# Off is the default for get_json_object so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_get_json_object_allow_nonnumeric_numbers_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").selectExpr('''get_json_object(json, "$.float")''',
            '''get_json_object(json, "$.double")'''),
        conf =_enable_get_json_object_conf)

# Off is the default for json_tuple, so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_json_tuple_allow_nonnumeric_numbers_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_NONNUMERIC_NUMBERS_FILE, "json").selectExpr('''json_tuple(json, "float", "double")'''),
        conf =_enable_json_tuple_conf)

WITH_BS_ESC_FILE = "withBackslashEscapingAnyCharacter.json"
WITH_BS_ESC_SCHEMA = StructType([
    StructField("str", StringType())])

# Off is the default for scan so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_allow_backslash_escape_any_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_BS_ESC_FILE,
        WITH_BS_ESC_SCHEMA,
        spark_tmp_table_factory,
        {"allowBackslashEscapingAnyCharacter": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default for from_json so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_backslash_escape_any_off(std_input_path):
    schema = WITH_BS_ESC_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowBackslashEscapingAnyCharacter": "false"})),
        conf =_enable_json_to_structs_conf)

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_backslash_escape_any_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_fallback_collect(
        read_func(std_input_path + '/' + WITH_BS_ESC_FILE,
        WITH_BS_ESC_SCHEMA,
        spark_tmp_table_factory,
        {"allowBackslashEscapingAnyCharacter": "true"}),
        'FileSourceScanExec',
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, 'ProjectExec')
def test_from_json_allow_backslash_escape_any_on(std_input_path):
    schema = WITH_BS_ESC_SCHEMA
    assert_gpu_fallback_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowBackslashEscapingAnyCharacter": "true"})),
        'JsonToStructs',
        conf =_enable_json_to_structs_conf)

# Off is the default for get_json_object so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_get_json_object_allow_backslash_escape_any_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''),
        conf =_enable_get_json_object_conf)

# Off is the default for json_tuple, so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10454')
def test_json_tuple_allow_backslash_escape_any_off(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_BS_ESC_FILE, "json").selectExpr('''json_tuple(json, "str")'''),
        conf =_enable_json_tuple_conf)

WITH_UNQUOTED_CONTROL_FILE = "withUnquotedControlChars.json"
WITH_UNQUOTED_CONTROL_SCHEMA = StructType([
    StructField("str", StringType())])

# Off is the default for scan so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10457')
def test_scan_json_allow_unquoted_control_chars_off(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE,
        WITH_UNQUOTED_CONTROL_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedControlChars": "false"}),
        conf=_enable_all_types_json_scan_conf)

# Off is the default for from_json so it really needs to work
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10457')
def test_from_json_allow_unquoted_control_chars_off(std_input_path):
    schema = WITH_UNQUOTED_CONTROL_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowUnquotedControlChars": "false"})),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
def test_scan_json_allow_unquoted_control_chars_on(std_input_path, read_func, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE,
        WITH_UNQUOTED_CONTROL_SCHEMA,
        spark_tmp_table_factory,
        {"allowUnquotedControlChars": "true"}),
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_allow_unquoted_control_chars_on(std_input_path):
    schema = WITH_UNQUOTED_CONTROL_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"allowUnquotedControlChars": "true"})),
        conf =_enable_json_to_structs_conf)

# On is the default for get_json_object so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_allow_unquoted_control_chars_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").selectExpr('''get_json_object(json, "$.str")'''),
        conf =_enable_get_json_object_conf)

# On is the default for json_tuple, so we want this to work
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_allow_unquoted_control_chars_on(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_UNQUOTED_CONTROL_FILE, "json").selectExpr('''json_tuple(json, "str")'''),
        conf =_enable_json_tuple_conf)


WITH_DEC_LOCALE_FILE = "decimal_locale_formatted_strings.json"
WITH_DEC_LOCALE_SCHEMA = StructType([
    StructField("data", DecimalType(10, 5))])
DEC_LOCALES=["en-US","it-CH","ko-KR","h-TH-x-lvariant-TH","ru-RU","de-DE","iw-IL","hi-IN","ar-QA","zh-CN","ko-KR"]

# Off is the default for scan so it really needs to work
@pytest.mark.parametrize('read_func', [read_json_df]) # We don't need both they are always the same
@pytest.mark.parametrize('locale', DEC_LOCALES)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10470')
def test_scan_json_dec_locale(std_input_path, read_func, spark_tmp_table_factory,locale):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + WITH_DEC_LOCALE_FILE,
        WITH_DEC_LOCALE_SCHEMA,
        spark_tmp_table_factory,
        {"locale": "locale"}),
        conf=_enable_all_types_json_scan_conf)

@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
@pytest.mark.parametrize('locale', DEC_LOCALES)
@pytest.mark.xfail(reason = 'https://github.com/NVIDIA/spark-rapids/issues/10470')
def test_from_json_dec_locale(std_input_path, locale):
    schema = WITH_DEC_LOCALE_SCHEMA
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").select(f.col('json'), f.from_json(f.col('json'), schema, {"locale": locale})),
        conf =_enable_json_to_structs_conf)

#There is no way to set a locale for these, and it really should not matter
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_dec_locale(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").selectExpr('''get_json_object(json, "$.data")'''),
        conf =_enable_get_json_object_conf)

#There is no way to set a locale for these, and it really should not matter
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_dec_locale(std_input_path):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + WITH_DEC_LOCALE_FILE, "json").selectExpr('''json_tuple(json, "data")'''),
        conf =_enable_json_tuple_conf)


@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_bytes(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", ByteType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    pytest.param("sci_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_bytes(std_input_path, input_file):
    schema = StructType([StructField("data", ByteType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_shorts(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", ShortType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    pytest.param("sci_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_shorts(std_input_path, input_file):
    schema = StructType([StructField("data", ShortType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_ints(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", IntegerType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    pytest.param("sci_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_ints(std_input_path, input_file):
    schema = StructType([StructField("data", IntegerType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_longs(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", LongType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    pytest.param("sci_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10460')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_longs(std_input_path, input_file):
    schema = StructType([StructField("data", LongType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('dt', [DecimalType(38,0), DecimalType(38,10), DecimalType(10,2)], ids=idfn)
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    pytest.param("float_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10469')),
    "sci_formatted_strings.json",
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10470')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df]) # we have done so many tests already that we don't need both read func. They are the same
def test_scan_json_decs(std_input_path, read_func, spark_tmp_table_factory, input_file, dt):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", dt)]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('dt', [DecimalType(38,0), DecimalType(38,10), DecimalType(10,2)], ids=idfn)
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10467')),
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10470')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_decs(std_input_path, input_file, dt):
    schema = StructType([StructField("data", dt)])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)


@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    pytest.param("single_quoted_strings.json", marks=pytest.mark.xfail(condition=is_before_spark_330(),reason='https://github.com/NVIDIA/spark-rapids/issues/10495')),
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10479'))])
@pytest.mark.parametrize('read_func', [read_json_df])
def test_scan_json_strings(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", StringType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10458')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10479'))])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_strings(std_input_path, input_file):
    schema = StructType([StructField("data", StringType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218')),
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218')),
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218'))])
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_get_json_object_formats(std_input_path, input_file):
   assert_gpu_and_cpu_are_equal_collect(
           lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").selectExpr("*", '''get_json_object(json, "$.data")'''),
        conf =_enable_get_json_object_conf)

@pytest.mark.parametrize('input_file', [
    pytest.param("int_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218')),
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218')),
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218'))])
@allow_non_gpu(TEXT_INPUT_EXEC)
def test_json_tuple_formats(std_input_path, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").selectExpr("*", '''json_tuple(json, "data")'''),
        conf =_enable_json_tuple_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    "sci_formatted.json",
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10480'))])
@pytest.mark.parametrize('read_func', [read_json_df])
def test_scan_json_bools(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", BooleanType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    "float_formatted.json",
    pytest.param("sci_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10480')),
    "int_formatted_strings.json",
    "float_formatted_strings.json",
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    pytest.param("boolean_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10480'))])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_bools(std_input_path, input_file):
    schema = StructType([StructField("data", BooleanType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@approximate_float()
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10481')),
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    pytest.param("float_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    pytest.param("sci_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
def test_scan_json_floats(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", FloatType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@approximate_float()
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10481')),
    "sci_formatted.json",
    "int_formatted_strings.json",
    pytest.param("float_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_floats(std_input_path, input_file):
    schema = StructType([StructField("data", FloatType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)

@approximate_float()
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10481')),
    "sci_formatted.json",
    pytest.param("int_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    pytest.param("float_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    pytest.param("sci_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    pytest.param("decimal_locale_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@pytest.mark.parametrize('read_func', [read_json_df])
def test_scan_json_doubles(std_input_path, read_func, spark_tmp_table_factory, input_file):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + input_file,
        StructType([StructField("data", DoubleType())]),
        spark_tmp_table_factory),
        conf=_enable_all_types_json_scan_conf)

@approximate_float()
@pytest.mark.parametrize('input_file', [
    "int_formatted.json",
    pytest.param("float_formatted.json", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10481')),
    "sci_formatted.json",
    "int_formatted_strings.json",
    pytest.param("float_formatted_strings.json",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10468')),
    "sci_formatted_strings.json",
    "decimal_locale_formatted_strings.json",
    "single_quoted_strings.json",
    "boolean_formatted.json"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_from_json_doubles(std_input_path, input_file):
    schema = StructType([StructField("data", DoubleType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : read_json_as_text(spark, std_input_path + '/' + input_file, "json").select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_json_to_structs_conf)


