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

_simple_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("score", FloatType()),
])


def read_hive_text_sql(data_path, schema, spark_tmp_table_factory, options={}):
    opts = options
    if schema is not None:
        opts = copy_and_update(options, {'schema': schema})

    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='hive', path=data_path, **opts)

    return read_impl


@approximate_float
@pytest.mark.parametrize('name,schema', [
    ('hive-delim-text/Acquisition_2007Q3', _acq_schema)
], ids=idfn)
def test_basic_hive_text_read(std_input_path, name, schema, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(read_hive_text_sql(std_input_path + '/' + name,
                                                            schema, spark_tmp_table_factory, {}),
                                         conf={})
