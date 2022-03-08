# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from conftest import is_databricks_runtime
from marks import approximate_float, allow_non_gpu, ignore_order

from spark_session import with_cpu_session, with_gpu_session, is_before_spark_330, is_spark_330_or_later

json_supported_gens = [
    # Spark does not escape '\r' or '\n' even though it uses it to mark end of record
    # This would require multiLine reads to work correctly, so we avoid these chars
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    StringGen('[nN][aA][nN]'),
    StringGen('[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,
    pytest.param(double_gen),
    pytest.param(FloatGen(no_nans=False)),
    pytest.param(float_gen),
    DoubleGen(no_nans=False)
]

_enable_all_types_conf = {
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true'}

_bool_schema = StructType([
    StructField('number', BooleanType())])

_byte_schema = StructType([
    StructField('number', ByteType())])

_short_schema = StructType([
    StructField('number', ShortType())])

_int_schema = StructType([
    StructField('number', IntegerType())])

_long_schema = StructType([
    StructField('number', LongType())])

_float_schema = StructType([
    StructField('number', FloatType())])

_double_schema = StructType([
    StructField('number', DoubleType())])

_decimal_10_2_schema = StructType([
    StructField('number', DecimalType(10, 2))])

_decimal_10_3_schema = StructType([
    StructField('number', DecimalType(10, 3))])

_date_schema = StructType([
    StructField('number', DateType())])

_string_schema = StructType([
    StructField('a', StringType())])

def read_json_df(data_path, schema, options = {}):
    def read_impl(spark):
        reader = spark.read
        if not schema is None:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return debug_df(reader.json(data_path))
    return read_impl

def read_json_sql(data_path, schema, options = {}):
    opts = options
    if not schema is None:
        opts = copy_and_update(options, {'schema': schema})
    def read_impl(spark):
        spark.sql('DROP TABLE IF EXISTS `TMP_json_TABLE`')
        return spark.catalog.createTable('TMP_json_TABLE', source='json', path=data_path, **opts)
    return read_impl

@approximate_float
@pytest.mark.parametrize('data_gen', [
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@allow_non_gpu('FileSourceScanExec')
def test_json_infer_schema_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.json(data_path),
            conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('data_gen', json_supported_gens, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).json(data_path),
            conf=updated_conf)

@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_input_meta(spark_tmp_path, v1_enabled_list):
    gen = StructGen([('a', long_gen), ('b', long_gen), ('c', long_gen)], nullable=False)
    first_data_path = spark_tmp_path + '/JSON_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(first_data_path))
    second_data_path = spark_tmp_path + '/JSON_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(second_data_path))
    data_path = spark_tmp_path + '/JSON_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type)
                    .json(data_path)
                    .filter(f.col('b') > 0)
                    .selectExpr('b',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=updated_conf)

json_supported_date_formats = ['yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM',
        'MM-yyyy', 'MM/yyyy', 'MM-dd-yyyy', 'MM/dd/yyyy']
@pytest.mark.parametrize('date_format', json_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_date_formats_round_trip(spark_tmp_path, date_format, v1_enabled_list):
    gen = StructGen([('a', DateGen())], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('dateFormat', date_format)\
                    .json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('dateFormat', date_format)\
                    .json(data_path),
            conf=updated_conf)

json_supported_ts_parts = ['', # Just the date
        "'T'HH:mm:ss.SSSXXX",
        "'T'HH:mm:ss[.SSS][XXX]",
        "'T'HH:mm:ss.SSS",
        "'T'HH:mm:ss[.SSS]",
        "'T'HH:mm:ss",
        "'T'HH:mm[:ss]",
        "'T'HH:mm"]

@pytest.mark.parametrize('ts_part', json_supported_ts_parts)
@pytest.mark.parametrize('date_format', json_supported_date_formats)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_ts_formats_round_trip(spark_tmp_path, date_format, ts_part, v1_enabled_list):
    full_format = date_format + ts_part
    data_gen = TimestampGen()
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('timestampFormat', full_format)\
                    .json(data_path))
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('timestampFormat', full_format)\
                    .json(data_path),
            conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'boolean.json',
    pytest.param('boolean_invalid.json', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/4779')),
    'ints.json',
    pytest.param('ints_invalid.json', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/4793')),
    'nan_and_inf.json',
    pytest.param('nan_and_inf_strings.json', marks=pytest.mark.skipif(is_before_spark_330(), reason='https://issues.apache.org/jira/browse/SPARK-38060 fixed in Spark 3.3.0')),
    'nan_and_inf_invalid.json',
    'floats.json',
    'floats_leading_zeros.json',
    'floats_invalid.json',
    pytest.param('floats_edge_cases.json', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/4647')),
    'decimals.json',
    'dates.json',
    'dates_invalid.json',
])
@pytest.mark.parametrize('schema', [_bool_schema, _byte_schema, _short_schema, _int_schema, _long_schema, \
                                    _float_schema, _double_schema, _decimal_10_2_schema, _decimal_10_3_schema, \
                                    _date_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('allow_non_numeric_numbers', ["true", "false"])
@pytest.mark.parametrize('allow_numeric_leading_zeros', ["true"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
def test_basic_json_read(std_input_path, filename, schema, read_func, allow_non_numeric_numbers, allow_numeric_leading_zeros, ansi_enabled):
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.ansi.enabled': ansi_enabled,
         'spark.sql.legacy.timeParserPolicy': 'CORRECTED'})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
        schema,
        { "allowNonNumericNumbers": allow_non_numeric_numbers,
          "allowNumericLeadingZeros": allow_numeric_leading_zeros}),
        conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'dates.json',
])
@pytest.mark.parametrize('schema', [_date_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=[pytest.mark.allow_non_gpu('FileSourceScanExec'), \
        pytest.mark.xfail(is_spark_330_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/4912")]),
    'CORRECTED',
    'EXCEPTION'
])
def test_json_read_valid_dates(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy,
                                    'spark.rapids.sql.incompatibleDateFormats.enabled': True})
    f = read_func(std_input_path + '/' + filename, schema, {})
    if time_parser_policy == 'LEGACY' and ansi_enabled == 'true':
        assert_gpu_fallback_collect(
            f,
            'FileSourceScanExec',
            conf=updated_conf)
    else:
        assert_gpu_and_cpu_are_equal_collect(f, conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'dates_invalid.json',
])
@pytest.mark.parametrize('schema', [_date_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=[pytest.mark.allow_non_gpu('FileSourceScanExec'), \
        pytest.mark.xfail(is_spark_330_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/4912")]),
    'CORRECTED',
    'EXCEPTION'
])
def test_json_read_invalid_dates(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy })
    f = read_func(std_input_path + '/' + filename, schema, {})
    if time_parser_policy == 'EXCEPTION':
        assert_gpu_and_cpu_error(
            df_fun=lambda spark: f(spark).collect(),
            conf=updated_conf,
            error_message='DateTimeException')
    elif time_parser_policy == 'LEGACY' and ansi_enabled == 'true':
        assert_gpu_fallback_collect(
            f,
            'FileSourceScanExec',
            conf=updated_conf)
    else:
        assert_gpu_and_cpu_are_equal_collect(f, conf=updated_conf)

@pytest.mark.parametrize('schema', [_string_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('allow_unquoted_chars', ["true"])
@pytest.mark.parametrize('filename', ['unquotedChars.json'])
def test_json_unquotedCharacters(std_input_path, filename, schema, read_func, allow_unquoted_chars):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
        schema,
        {"allowUnquotedControlChars": allow_unquoted_chars}),
        conf=_enable_all_types_conf)

@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
def test_json_read_with_corrupt_files(spark_tmp_path, v1_enabled_list):
    first_data_path = spark_tmp_path + '/JSON_DATA/first'
    with_cpu_session(lambda spark : spark.range(1).toDF("a").write.json(first_data_path))
    second_data_path = spark_tmp_path + '/JSON_DATA/second'
    with_cpu_session(lambda spark : spark.range(1, 2).toDF("a").write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/JSON_DATA/third'
    with_cpu_session(lambda spark : spark.range(2, 3).toDF("a").write.json(third_data_path))

    all_confs = copy_and_update(_enable_all_types_conf,
                                {'spark.sql.files.ignoreCorruptFiles': "true",
                                 'spark.sql.sources.useV1SourceList': v1_enabled_list})
    schema = StructType([StructField("a", IntegerType())])

    # when ignoreCorruptFiles is enabled, gpu reading should not throw exception, while CPU can successfully
    # read the three files without ignore corrupt files. So we just check if GPU will throw exception.
    with_gpu_session(
            lambda spark : spark.read.schema(schema)
                .json([first_data_path, second_data_path, third_data_path])
                .collect(),
            conf=all_confs)
