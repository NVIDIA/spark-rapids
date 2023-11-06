# Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_and_cpu_row_counts_equal, \
    assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_collect_with_capture
from data_gen import *
from datetime import timezone
from conftest import is_databricks_runtime
from marks import approximate_float, allow_non_gpu, ignore_order
from spark_session import with_cpu_session, with_gpu_session, is_before_spark_330, is_before_spark_340, \
    is_before_spark_341

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
    'spark.rapids.sql.format.json.read.enabled': 'true',
    'spark.rapids.sql.json.read.float.enabled': 'true',
    'spark.rapids.sql.json.read.double.enabled': 'true',
    'spark.rapids.sql.json.read.decimal.enabled': 'true'
}

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

_timestamp_schema = StructType([
    StructField('number', TimestampType())])

_string_schema = StructType([
    StructField('a', StringType())])

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
        'MM-yyyy', 'MM/yyyy', 'MM-dd-yyyy', 'MM/dd/yyyy', 'dd-MM-yyyy', 'dd/MM/yyyy']
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

@allow_non_gpu('FileSourceScanExec', 'ProjectExec')
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('ts_part', json_supported_ts_parts)
@pytest.mark.parametrize('date_format', json_supported_date_formats)
@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_json_ts_formats_round_trip_ntz_v1(spark_tmp_path, date_format, ts_part, timestamp_type):
    json_ts_formats_round_trip_ntz(spark_tmp_path, date_format, ts_part, timestamp_type, 'json', 'FileSourceScanExec')

@allow_non_gpu('BatchScanExec', 'ProjectExec')
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('ts_part', json_supported_ts_parts)
@pytest.mark.parametrize('date_format', json_supported_date_formats)
@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_json_ts_formats_round_trip_ntz_v2(spark_tmp_path, date_format, ts_part, timestamp_type):
    json_ts_formats_round_trip_ntz(spark_tmp_path, date_format, ts_part, timestamp_type, '', 'BatchScanExec')

def json_ts_formats_round_trip_ntz(spark_tmp_path, date_format, ts_part, timestamp_type, v1_enabled_list, cpu_scan_class):
    full_format = date_format + ts_part
    data_gen = TimestampGen(tzinfo=None if timestamp_type == "TIMESTAMP_NTZ" else timezone.utc)
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    with_cpu_session(
        lambda spark : gen_df(spark, gen).write \
            .option('timestampFormat', full_format) \
            .json(data_path))
    updated_conf = copy_and_update(_enable_all_types_conf,
        {
            'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.timestampType': timestamp_type
        })

    def do_read(spark):
        return spark.read \
            .schema(schema) \
            .option('timestampFormat', full_format) \
            .json(data_path)


    if timestamp_type == "TIMESTAMP_LTZ":
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark : do_read(spark),
            exist_classes = 'Gpu' + cpu_scan_class,
            non_exist_classes = cpu_scan_class,
            conf=updated_conf)
    else:
        # we fall back to CPU due to "unsupported data types in output: TimestampNTZType"
        assert_gpu_fallback_collect(
            lambda spark : do_read(spark),
            cpu_fallback_class_name = cpu_scan_class,
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
def test_basic_json_read(std_input_path, filename, schema, read_func, allow_non_numeric_numbers, allow_numeric_leading_zeros, ansi_enabled, spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.ansi.enabled': ansi_enabled,
         'spark.sql.legacy.timeParserPolicy': 'CORRECTED'})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
        schema,
        spark_tmp_table_factory,
        { "allowNonNumericNumbers": allow_non_numeric_numbers,
          "allowNumericLeadingZeros": allow_numeric_leading_zeros}),
        conf=updated_conf)

@ignore_order
@pytest.mark.parametrize('filename', [
    'malformed1.ndjson',
    'malformed2.ndjson',
    'malformed3.ndjson',
    'malformed4.ndjson'
])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('schema', [_int_schema])
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_read_invalid_json(spark_tmp_table_factory, std_input_path, read_func, filename, schema, v1_enabled_list):
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  {}),
        conf=conf)

@pytest.mark.parametrize('filename', [
    'mixed-primitives.ndjson',
    'mixed-primitives-nested.ndjson',
    'simple-nested.ndjson',
    pytest.param('mixed-nested.ndjson', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9353'))
])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('schema', [_int_schema])
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_read_valid_json(spark_tmp_table_factory, std_input_path, read_func, filename, schema, v1_enabled_list):
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  {}),
        conf=conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'dates.json',
])
@pytest.mark.parametrize('schema', [_date_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('FileSourceScanExec')),
    'CORRECTED',
    'EXCEPTION'
])
def test_json_read_valid_dates(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy, spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy})
    f = read_func(std_input_path + '/' + filename, schema, spark_tmp_table_factory, {})
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
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('FileSourceScanExec')),
    'CORRECTED',
    'EXCEPTION'
])
def test_json_read_invalid_dates(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy, spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy })
    f = read_func(std_input_path + '/' + filename, schema, spark_tmp_table_factory, {})
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

@approximate_float
@pytest.mark.parametrize('filename', [
    'timestamps.json',
])
@pytest.mark.parametrize('schema', [_timestamp_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('FileSourceScanExec')),
    'CORRECTED',
    'EXCEPTION'
])
def test_json_read_valid_timestamps(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy, \
        spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy})
    f = read_func(std_input_path + '/' + filename, schema, spark_tmp_table_factory, {})
    assert_gpu_and_cpu_are_equal_collect(f, conf=updated_conf)

@pytest.mark.parametrize('schema', [_string_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('allow_unquoted_chars', ["true"])
@pytest.mark.parametrize('filename', ['unquotedChars.json'])
def test_json_unquotedCharacters(std_input_path, filename, schema, read_func, allow_unquoted_chars, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
        schema,
        spark_tmp_table_factory,
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

@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_read_count(spark_tmp_path, v1_enabled_list):
    gen_list = [byte_gen, short_gen, int_gen, long_gen, boolean_gen]
    gen = StructGen([('_c' + str(i), gen) for i, gen in enumerate(gen_list)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_row_counts_equal(
            lambda spark : spark.read.schema(schema).json(data_path),
            conf=updated_conf)

def test_from_json_map():
    # The test here is working around some inconsistencies in how the keys are parsed for maps
    # on the GPU the keys are dense, but on the CPU they are sparse
    json_string_gen = StringGen(r'{"a": "[0-9]{0,5}"(, "b": "[A-Z]{0,5}")?}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json(f.col('a'), 'MAP<STRING,STRING>')),
        conf={"spark.rapids.sql.expression.JsonToStructs": True})

@allow_non_gpu('ProjectExec', 'JsonToStructs')
def test_from_json_map_fallback():
    # The test here is working around some inconsistencies in how the keys are parsed for maps
    # on the GPU the keys are dense, but on the CPU they are sparse
    json_string_gen = StringGen(r'{"a": \d\d}')
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json(f.col('a'), 'MAP<STRING,INT>')),
        'JsonToStructs',
        conf={"spark.rapids.sql.expression.JsonToStructs": True})

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/8558')
@pytest.mark.parametrize('schema', ['struct<a:string>',
                                    'struct<d:string>',
                                    'struct<a:string,b:string>',
                                    'struct<c:int,a:string>',
                                    'struct<a:string,a:string>',
                                    ])
def test_from_json_struct(schema):
    json_string_gen = StringGen(r'{"a": "[0-9]{0,5}", "b": "[A-Z]{0,5}", "c": 1\d\d\d}').with_special_pattern('', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf={"spark.rapids.sql.expression.JsonToStructs": True})

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/8558')
@pytest.mark.parametrize('schema', ['struct<teacher:string>',
                                    'struct<student:struct<name:string,age:int>>',
                                    'struct<teacher:string,student:struct<name:string,age:int>>'])
def test_from_json_struct_of_struct(schema):
    json_string_gen = StringGen(r'{"teacher": "[A-Z]{1}[a-z]{2,5}",' \
                                r'"student": {"name": "[A-Z]{1}[a-z]{2,5}", "age": 1\d}}').with_special_pattern('', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf={"spark.rapids.sql.expression.JsonToStructs": True})

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/8558')
@pytest.mark.parametrize('schema', ['struct<teacher:string>',
                                    'struct<student:array<struct<name:string,class:string>>>',
                                    'struct<teacher:string,student:array<struct<name:string,class:string>>>'])
def test_from_json_struct_of_list(schema):
    json_string_gen = StringGen(r'{"teacher": "[A-Z]{1}[a-z]{2,5}",' \
                                r'"student": \[{"name": "[A-Z]{1}[a-z]{2,5}", "class": "junior"},' \
                                r'{"name": "[A-Z]{1}[a-z]{2,5}", "class": "freshman"}\]}').with_special_pattern('', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf={"spark.rapids.sql.expression.JsonToStructs": True})

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/8558')
@pytest.mark.parametrize('schema', ['struct<a:string>', 'struct<a:string,b:int>'])
def test_from_json_struct_all_empty_string_input(schema):
    json_string_gen = StringGen('')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf={"spark.rapids.sql.expression.JsonToStructs": True})

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema', [("dates.json", _date_schema),("dates.json", _timestamp_schema),
                                             ("timestamps.json", _timestamp_schema)])
def test_json_datetime_parsing_fallback_cpu_fallback(std_input_path, filename, schema):
    data_path = std_input_path + "/" + filename
    assert_gpu_fallback_collect(
        lambda spark : spark.read.schema(schema).option('enableDateTimeParsingFallback', "true").json(data_path),
        'FileSourceScanExec',
        conf=_enable_all_types_conf)

@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema', [("ints.json", _int_schema)])
def test_json_datetime_parsing_fallback_no_datetime(std_input_path, filename, schema):
    data_path = std_input_path + "/" + filename
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.schema(schema).option('enableDateTimeParsingFallback', "true").json(data_path),
        conf=_enable_all_types_conf)

@pytest.mark.skip(reason=str("https://github.com/NVIDIA/spark-rapids/issues/8403"))
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@pytest.mark.parametrize('col_name', ['K0', 'k0', 'K3', 'k3', 'V0', 'v0'], ids=idfn)
@ignore_order
def test_read_case_col_name(spark_tmp_path, v1_enabled_list, col_name):
    all_confs = {'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.rapids.sql.format.json.read.enabled': True,
            'spark.rapids.sql.format.json.enabled': True}
    gen_list =[('k0', LongGen(nullable=False, min_val=0, max_val=0)), 
            ('k1', LongGen(nullable=False, min_val=1, max_val=1)),
            ('k2', LongGen(nullable=False, min_val=2, max_val=2)),
            ('k3', LongGen(nullable=False, min_val=3, max_val=3)),
            ('v0', LongGen()),
            ('v1', LongGen()),
            ('v2', LongGen()),
            ('v3', LongGen())]
 
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.partitionBy('k0', 'k1', 'k2', 'k3').json(data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type).json(data_path).selectExpr(col_name),
            conf=all_confs)


@pytest.mark.parametrize('data_gen', [byte_gen,
    boolean_gen,
    short_gen,
    int_gen,
    long_gen,
    pytest.param(float_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9350')),
    pytest.param(double_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9350')),
    pytest.param(date_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9515')),
    pytest.param(timestamp_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9515')),
    StringGen('[A-Za-z0-9]{0,10}', nullable=True),
    pytest.param(StringGen(nullable=True), marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9514')),
], ids=idfn)
@pytest.mark.parametrize('ignore_null_fields', [
    True,
    pytest.param(False, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9516'))
])
@pytest.mark.parametrize('pretty', [
    pytest.param(True, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9517')),
    False
])
def test_structs_to_json(spark_tmp_path, data_gen, ignore_null_fields, pretty):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", StructGen([('child', data_gen)], nullable=True)),
        ("c", ArrayGen(StructGen([('child', data_gen)], nullable=True))),
        ("d", MapGen(LongGen(nullable=False), data_gen)),
        ("d", MapGen(StringGen('[A-Za-z0-9]{0,10}', nullable=False), data_gen)),
        ("e", ArrayGen(MapGen(LongGen(nullable=False), data_gen), nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'ignoreNullFields': ignore_null_fields,
                'pretty': pretty }

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options)).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)