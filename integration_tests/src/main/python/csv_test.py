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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_gpu_fallback_write
from conftest import get_non_gpu_allowed
from datetime import datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session

_acq_schema = StructType([
    StructField('loan_id', LongType()),
    StructField('orig_channel', StringType()),
    StructField('seller_name', StringType()),
    StructField('orig_interest_rate', DoubleType()),
    StructField('orig_upb', IntegerType()),
    StructField('orig_loan_term', IntegerType()),
    StructField('orig_date', StringType()),
    StructField('first_pay_date', StringType()),
    StructField('orig_ltv', DoubleType()),
    StructField('orig_cltv', DoubleType()),
    StructField('num_borrowers', DoubleType()),
    StructField('dti', DoubleType()),
    StructField('borrower_credit_score', DoubleType()),
    StructField('first_home_buyer', StringType()),
    StructField('loan_purpose', StringType()),
    StructField('property_type', StringType()),
    StructField('num_units', IntegerType()),
    StructField('occupancy_status', StringType()),
    StructField('property_state', StringType()),
    StructField('zip', IntegerType()),
    StructField('mortgage_insurance_percent', DoubleType()),
    StructField('product_type', StringType()),
    StructField('coborrow_credit_score', DoubleType()),
    StructField('mortgage_insurance_type', DoubleType()),
    StructField('relocation_mortgage_indicator', StringType())])

_perf_schema = StructType([
    StructField('loan_id', LongType()),
    StructField('monthly_reporting_period', StringType()),
    StructField('servicer', StringType()),
    StructField('interest_rate', DoubleType()),
    StructField('current_actual_upb', DoubleType()),
    StructField('loan_age', DoubleType()),
    StructField('remaining_months_to_legal_maturity', DoubleType()),
    StructField('adj_remaining_months_to_maturity', DoubleType()),
    StructField('maturity_date', StringType()),
    StructField('msa', DoubleType()),
    StructField('current_loan_delinquency_status', IntegerType()),
    StructField('mod_flag', StringType()),
    StructField('zero_balance_code', StringType()),
    StructField('zero_balance_effective_date', StringType()),
    StructField('last_paid_installment_date', StringType()),
    StructField('foreclosed_after', StringType()),
    StructField('disposition_date', StringType()),
    StructField('foreclosure_costs', DoubleType()),
    StructField('prop_preservation_and_repair_costs', DoubleType()),
    StructField('asset_recovery_costs', DoubleType()),
    StructField('misc_holding_expenses', DoubleType()),
    StructField('holding_taxes', DoubleType()),
    StructField('net_sale_proceeds', DoubleType()),
    StructField('credit_enhancement_proceeds', DoubleType()),
    StructField('repurchase_make_whole_proceeds', StringType()),
    StructField('other_foreclosure_proceeds', DoubleType()),
    StructField('non_interest_bearing_upb', DoubleType()),
    StructField('principal_forgiveness_upb', StringType()),
    StructField('repurchase_make_whole_proceeds_flag', StringType()),
    StructField('foreclosure_principal_write_off_amount', StringType()),
    StructField('servicing_activity_indicator', StringType())])

_date_schema = StructType([
    StructField('date', DateType())])

_ts_schema = StructType([
    StructField('ts', TimestampType())])

_bad_str_schema = StructType([
    StructField('string', StringType())])

_good_str_schema = StructType([
    StructField('Something', StringType())])

_three_str_schema = StructType([
    StructField('a', StringType()),
    StructField('b', StringType()),
    StructField('c', StringType())])

_trucks_schema = StructType([
    StructField('make', StringType()),
    StructField('model', StringType()),
    StructField('year', IntegerType()),
    StructField('price', StringType()),
    StructField('comment', StringType())])

_bool_schema = StructType([
    StructField('boolean', BooleanType())])

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

_number_as_string_schema = StructType([
    StructField('number', StringType())])

_empty_byte_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', ByteType()),
    StructField('ignored_b', StringType())])

_empty_short_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', ShortType()),
    StructField('ignored_b', StringType())])

_empty_int_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', IntegerType()),
    StructField('ignored_b', StringType())])

_empty_long_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', LongType()),
    StructField('ignored_b', StringType())])

_empty_float_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', FloatType()),
    StructField('ignored_b', StringType())])

_empty_double_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', DoubleType()),
    StructField('ignored_b', StringType())])

_enable_all_types_conf = {'spark.rapids.sql.csvTimestamps.enabled': 'true',
        'spark.rapids.sql.csv.read.bool.enabled': 'true',
        'spark.rapids.sql.csv.read.date.enabled': 'true',
        'spark.rapids.sql.csv.read.byte.enabled': 'true',
        'spark.rapids.sql.csv.read.short.enabled': 'true',
        'spark.rapids.sql.csv.read.integer.enabled': 'true',
        'spark.rapids.sql.csv.read.long.enabled': 'true',
        'spark.rapids.sql.csv.read.float.enabled': 'true',
        'spark.rapids.sql.csv.read.double.enabled': 'true',
        'spark.sql.legacy.timeParserPolicy': 'Corrected'}

def read_csv_df(data_path, schema, options = {}):
    def read_impl(spark):
        reader = spark.read
        if not schema is None:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return debug_df(reader.csv(data_path))
    return read_impl

def read_csv_sql(data_path, schema, options = {}):
    if not schema is None:
        options.update({'schema': schema})
    def read_impl(spark):
        spark.sql('DROP TABLE IF EXISTS `TMP_CSV_TABLE`')
        return spark.catalog.createTable('TMP_CSV_TABLE', source='csv', path=data_path, **options)
    return read_impl

@approximate_float
@pytest.mark.parametrize('name,schema,options', [
    ('Acquisition_2007Q3.txt', _acq_schema, {'sep': '|'}),
    ('Performance_2007Q3.txt_0', _perf_schema, {'sep': '|'}),
    pytest.param('ts.csv', _date_schema, {}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1091')),
    pytest.param('date.csv', _date_schema, {}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1111')),
    ('ts.csv', _ts_schema, {}),
    ('str.csv', _bad_str_schema, {'header': 'true'}),
    ('str.csv', _good_str_schema, {'header': 'true'}),
    ('no-comments.csv', _three_str_schema, {}),
    ('empty.csv', _three_str_schema, {}),
    ('just_comments.csv', _three_str_schema, {'comment': '#'}),
    ('trucks.csv', _trucks_schema, {'header': 'true'}),
    ('trucks.tsv', _trucks_schema, {'sep': '\t', 'header': 'true'}),
    ('trucks-different.csv', _trucks_schema, {'sep': '|', 'header': 'true', 'quote': "'"}),
    ('trucks-blank-names.csv', _trucks_schema, {'header': 'true'}),
    ('trucks-windows.csv', _trucks_schema, {'header': 'true'}),
    ('trucks-empty-values.csv', _trucks_schema, {'header': 'true'}),
    ('trucks-extra-columns.csv', _trucks_schema, {'header': 'true'}),
    pytest.param('trucks-comments.csv', _trucks_schema, {'header': 'true', 'comment': '~'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2066')),
    ('trucks-more-comments.csv', _trucks_schema,  {'header': 'true', 'comment': '#'}),
    pytest.param('trucks-missing-quotes.csv', _trucks_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/130')),
    pytest.param('trucks-null.csv', _trucks_schema, {'header': 'true', 'nullValue': 'null'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2068')),
    pytest.param('trucks-null.csv', _trucks_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('simple_int_values.csv', _byte_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/126')),
    pytest.param('simple_int_values.csv', _short_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/126')),
    pytest.param('simple_int_values.csv', _int_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/126')),
    pytest.param('simple_int_values.csv', _long_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/126')),
    ('simple_int_values.csv', _float_schema, {'header': 'true'}),
    ('simple_int_values.csv', _double_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_byte_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('empty_int_values.csv', _empty_short_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('empty_int_values.csv', _empty_int_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('empty_int_values.csv', _empty_long_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('empty_int_values.csv', _empty_float_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('empty_int_values.csv', _empty_double_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('nan_and_inf.csv', _float_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/125')),
    pytest.param('simple_float_values.csv', _float_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/124, https://github.com/NVIDIA/spark-rapids/issues/125i, https://github.com/NVIDIA/spark-rapids/issues/126')),
    pytest.param('simple_float_values.csv', _double_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/124, https://github.com/NVIDIA/spark-rapids/issues/125, https://github.com/NVIDIA/spark-rapids/issues/126')),
    pytest.param('simple_boolean_values.csv', _bool_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2071')),
    pytest.param('ints_with_whitespace.csv', _number_as_string_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2069')),
    pytest.param('ints_with_whitespace.csv', _byte_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/130'))
    ], ids=idfn)
@pytest.mark.parametrize('read_func', [read_csv_df, read_csv_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_basic_read(std_input_path, name, schema, options, read_func, v1_enabled_list):
    updated_conf=_enable_all_types_conf.copy()
    updated_conf['spark.sql.sources.useV1SourceList']=v1_enabled_list
    assert_gpu_and_cpu_are_equal_collect(read_func(std_input_path + '/' + name, schema, options),
            conf=updated_conf)

csv_supported_gens = [
        # Spark does not escape '\r' or '\n' even though it uses it to mark end of record
        # This would require multiLine reads to work correctly so we avoid these chars
        StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
        StringGen('[aAbB ]{0,10}'),
        byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen,
        DoubleGen(no_nans=True), # NaN, Inf, and -Inf are not supported
        # Once https://github.com/NVIDIA/spark-rapids/issues/125 and https://github.com/NVIDIA/spark-rapids/issues/124
        # are fixed we should not have to special case float values any more.
        pytest.param(double_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/125')),
        pytest.param(FloatGen(no_nans=True), marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/124')),
        pytest.param(float_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/125')),
        TimestampGen()]

@approximate_float
@pytest.mark.parametrize('data_gen', csv_supported_gens, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf=_enable_all_types_conf.copy()
    updated_conf['spark.sql.sources.useV1SourceList']=v1_enabled_list
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).csv(data_path),
            conf=updated_conf)

@allow_non_gpu('org.apache.spark.sql.execution.LeafExecNode')
@pytest.mark.parametrize('read_func', [read_csv_df, read_csv_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.csv.enabled', 'spark.rapids.sql.format.csv.read.enabled'])
def test_csv_fallback(spark_tmp_path, read_func, disable_conf):
    data_gens =[
        StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
        byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen]

    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf=_enable_all_types_conf.copy()
    updated_conf[disable_conf]='false'

    reader = read_func(data_path, schema)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(data_path))
    assert_gpu_fallback_collect(
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2') + f.col('_c3')),
            # TODO add support for lists
            cpu_fallback_class_name=get_non_gpu_allowed()[0],
            conf=updated_conf)

csv_supported_date_formats = ['yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM',
        'MM-yyyy', 'MM/yyyy', 'MM-dd-yyyy', 'MM/dd/yyyy']
@pytest.mark.parametrize('date_format', csv_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_date_formats_round_trip(spark_tmp_path, date_format, v1_enabled_list):
    gen = StructGen([('a', DateGen())], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf=_enable_all_types_conf.copy()
    updated_conf['spark.sql.sources.useV1SourceList']=v1_enabled_list
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('dateFormat', date_format)\
                    .csv(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('dateFormat', date_format)\
                    .csv(data_path),
            conf=updated_conf)

csv_supported_ts_parts = ['', # Just the date
        "'T'HH:mm:ss.SSSXXX",
        "'T'HH:mm:ss[.SSS][XXX]",
        "'T'HH:mm:ss.SSS",
        "'T'HH:mm:ss[.SSS]",
        "'T'HH:mm:ss",
        "'T'HH:mm[:ss]",
        "'T'HH:mm"]

@pytest.mark.parametrize('ts_part', csv_supported_ts_parts)
@pytest.mark.parametrize('date_format', csv_supported_date_formats)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_ts_formats_round_trip(spark_tmp_path, date_format, ts_part, v1_enabled_list):
    full_format = date_format + ts_part
    data_gen = TimestampGen()
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('timestampFormat', full_format)\
                    .csv(data_path))
    updated_conf=_enable_all_types_conf.copy()
    updated_conf['spark.sql.sources.useV1SourceList']=v1_enabled_list
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('timestampFormat', full_format)\
                    .csv(data_path),
            conf=updated_conf)

@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_input_meta(spark_tmp_path, v1_enabled_list):
    gen = StructGen([('a', long_gen), ('b', long_gen)], nullable=False)
    first_data_path = spark_tmp_path + '/CSV_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(first_data_path))
    second_data_path = spark_tmp_path + '/CSV_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(second_data_path))
    data_path = spark_tmp_path + '/CSV_DATA'
    updated_conf=_enable_all_types_conf.copy()
    updated_conf['spark.sql.sources.useV1SourceList']=v1_enabled_list
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type)\
                    .csv(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=updated_conf)

@allow_non_gpu('DataWritingCommandExec')
def test_csv_save_as_table_fallback(spark_tmp_path, spark_tmp_table_factory):
    gen = TimestampGen()
    data_path = spark_tmp_path + '/CSV_DATA'
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("csv").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.csv(path),
            data_path,
            'DataWritingCommandExec')
