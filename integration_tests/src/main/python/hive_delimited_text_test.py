# Copyright (c) 2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_and_cpu_row_counts_equal, assert_gpu_fallback_write, \
    assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_fallback_collect
from conftest import get_non_gpu_allowed
from datetime import datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, is_before_spark_330

acq_schema = StructType([
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

perf_schema = StructType([
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

timestamp_schema = StructType([
    StructField('ts', TimestampType())])

date_schema = StructType([
    StructField('date', DateType())])

trucks_schema = StructType([
    StructField('make', StringType()),
    StructField('model', StringType()),
    StructField('year', IntegerType()),
    StructField('price', StringType()),
    StructField('comment', StringType())])


def make_schema(column_type):
    return StructType([StructField('number', column_type)])


byte_schema = StructType([
    StructField('number', ByteType())])

short_schema = StructType([
    StructField('number', ShortType())])

int_schema = StructType([
    StructField('number', IntegerType())])

long_schema = StructType([
    StructField('number', LongType())])

float_schema = StructType([
    StructField('number', FloatType())])

double_schema = StructType([
    StructField('number', DoubleType())])

decimal_10_2_schema = StructType([
    StructField('number', DecimalType(10, 2))])

decimal_10_3_schema = StructType([
    StructField('number', DecimalType(10, 3))])

number_as_string_schema = StructType([
    StructField('number', StringType())])


def read_hive_text_sql(data_path, schema, spark_tmp_table_factory, options=None):
    if options is None:
        options = {}
    opts = options
    if schema is not None:
        opts = copy_and_update(options, {'schema': schema})

    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='hive', path=data_path, **opts)

    return read_impl


@approximate_float
@pytest.mark.parametrize('name,schema,options', [

    # Numeric Reads.
    ('hive-delim-text/simple-boolean-values', make_schema(BooleanType()),      {}),
    ('hive-delim-text/simple-int-values',     make_schema(ByteType()),         {}),
    ('hive-delim-text/simple-int-values',     make_schema(ShortType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(IntegerType()),      {}),
    ('hive-delim-text/simple-int-values',     make_schema(LongType()),         {}),
    ('hive-delim-text/simple-int-values',     make_schema(FloatType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(DoubleType()),       {}),
    ('hive-delim-text/simple-int-values',     make_schema(DecimalType(10, 2)), {}),
    ('hive-delim-text/simple-int-values',     make_schema(DecimalType(10, 3)), {}),
    ('hive-delim-text/simple-int-values',     make_schema(StringType()),       {}),

    # Custom datasets
    ('hive-delim-text/Acquisition_2007Q3', acq_schema, {}),
    ('hive-delim-text/Performance_2007Q3', perf_schema, {'serialization.null.format': ''}),
    pytest.param('hive-delim-text/Performance_2007Q3', perf_schema, {},
                 marks=pytest.mark.xfail(reason="GPU treats empty strings as nulls."
                                                "See https://github.com/NVIDIA/spark-rapids/issues/7069.")),
    ('hive-delim-text/trucks-1', trucks_schema, {}),
    pytest.param('hive-delim-text/trucks-err', trucks_schema, {},
                 marks=pytest.mark.xfail(reason="GPU skips empty lines, and removes quotes. "
                                                "See https://github.com/NVIDIA/spark-rapids/issues/7068.")),

    # Date/Time
    ('hive-delim-text/timestamp', timestamp_schema, {}),
    ('hive-delim-text/date', date_schema, {}),

    # TODO: GPU reads all input. CPU return null on all but 1 row
    #       (formatted exactly right for timestamp).
    #  1. Document round trip works.
    #  2. Document failure cases on Github issues
    pytest.param('hive-delim-text/timestamp-err', timestamp_schema, {},
                 marks=pytest.mark.xfail(reason="GPU timestamp reads are more permissive than CPU.")),

    # TODO: GPU forgives spaces, but throws on month=50. CPU nulls spaces, nulls month=50.
    #  1. Document round trip works.
    #  2. Document failure cases on Github issues
    pytest.param('hive-delim-text/date-err', date_schema, {},
                 marks=pytest.mark.xfail(reason="GPU read trims date string whitespace, "
                                                "and errors out on invalid dates.")),

    # Test that lines beginning with comments ('#') aren't skipped.
    ('hive-delim-text/comments', StructType([StructField("str", StringType()),
                                             StructField("num", IntegerType()),
                                             StructField("another_str", StringType())]), {}),

    # Test that carriage returns ('\r'/'^M') are treated similarly to newlines ('\n')
    ('hive-delim-text/carriage-return', StructType([StructField("str", StringType())]), {}),
    pytest.param('hive-delim-text/carriage-return-err', StructType([StructField("str", StringType())]), {},
                 marks=pytest.mark.xfail(reason="GPU skips empty lines. Consecutive \r is treated as empty line, "
                                                "and skipped. This produces fewer rows than expected. "
                                                "See https://github.com/NVIDIA/spark-rapids/issues/7068.")),
], ids=idfn)
def test_basic_hive_text_read(std_input_path, name, schema, spark_tmp_table_factory, options):
    assert_gpu_and_cpu_are_equal_collect(read_hive_text_sql(std_input_path + '/' + name,
                                                            schema, spark_tmp_table_factory, options),
                                         conf={})


hive_text_supported_gens = [
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    StringGen('[nN][aA][nN]'),
    StringGen('[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen,
    float_gen,
    FloatGen(no_nans=False),
    double_gen,
    DoubleGen(no_nans=False),
    TimestampGen(),
]


@approximate_float
@pytest.mark.parametrize('data_gen', hive_text_supported_gens, ids=idfn)
def test_hive_text_round_trip(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    def create_hive_text_table(spark, column_gen, text_table_name):
        gen_df(spark, column_gen).repartition(1).createOrReplaceTempView("input_view")
        spark.sql("DROP TABLE IF EXISTS " + text_table_name)
        spark.sql("CREATE TABLE " + text_table_name + " STORED AS TEXTFILE " +
                  "LOCATION '" + data_path + "' " +
                  "AS SELECT my_field FROM input_view")

    def read_hive_text_table(spark, text_table_name):
        return spark.sql("SELECT my_field FROM " + text_table_name)

    with_cpu_session(lambda spark: create_hive_text_table(spark, gen, table_name))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: read_hive_text_table(spark, table_name),
            conf={})
