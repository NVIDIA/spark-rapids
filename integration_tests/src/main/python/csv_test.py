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

from asserts import assert_gpu_and_cpu_are_equal_collect
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

@approximate_float
@pytest.mark.parametrize('name,schema,sep,header', [
    ('Acquisition_2007Q3.txt', _acq_schema, '|', False),
    ('Performance_2007Q3.txt_0', _perf_schema, '|', False)])
def test_basic_read(std_input_path, name, schema, sep, header):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('header', header)\
                    .option('sep', sep)\
                    .csv(std_input_path + '/' + name))


# tiemstamps are not supported because of time zones
csv_supported_gens = [byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen]

#TODO need to work on string_gen
#TODO need to work on double_gen and float_gen (NaN, Infinity and -Infinity don't parse properly).

@pytest.mark.parametrize('data_gen', csv_supported_gens, ids=idfn)
def test_round_trip(spark_tmp_path, data_gen):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).csv(data_path))

# It looks like cudf supports
# TODO add more tests and document incompat
# DATE:
# yyyy-MM?-dd?
# yyyy/MM?/dd?
# yyyy-MM?
# yyyy/MM?
# If day first is false (default)
# MM?/yyyy*
# MM?/dd?/yyyy*
# If day first is true
# dd?-MM?-yyyy* # No bounds checking
# dd?/MM?/yyyy*

# TIMESTAMP:
# $DATE[T ]$TIME
# hh?:mm?:ss?.SSS? aa
# HH?:mm?:ss?.SSS?
# Default date format is "yyyy-MM-dd"
# Default TS format is "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"

# FLOAT:
#-?\d*.\d*[eE][+-]?
#-?\d*[eE][+-]?
# Anything else is null

# INT:
# -?\d*
