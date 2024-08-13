# Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal
from data_gen import non_utc_allow
from marks import *

columnarClass = 'com.nvidia.spark.rapids.tests.datasourcev2.parquet.ArrowColumnarDataSourceV2'

def readTable(types, classToUse):
    return lambda spark: spark.read\
        .option("arrowTypes", types)\
        .format(classToUse).load()\
        .orderBy("col1")

@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_int():
    assert_gpu_and_cpu_are_equal_collect(readTable("int", columnarClass))

@validate_execs_in_gpu_plan('HostColumnarToGpu')
@allow_non_gpu(*non_utc_allow)
def test_read_strings():
    assert_gpu_and_cpu_are_equal_collect(readTable("string", columnarClass))

@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_all_types():
    assert_gpu_and_cpu_are_equal_collect(
       readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf={'spark.rapids.sql.castFloatToString.enabled': 'true'})


@disable_ansi_mode  # Cannot run in ANSI mode until COUNT aggregation is supported.
                    # See https://github.com/NVIDIA/spark-rapids/issues/5114
@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_all_types_count():
    assert_gpu_and_cpu_row_counts_equal(
       readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf={'spark.rapids.sql.castFloatToString.enabled': 'true'})


@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_arrow_off():
    assert_gpu_and_cpu_are_equal_collect(
        readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf={'spark.rapids.arrowCopyOptimizationEnabled': 'false',
                  'spark.rapids.sql.castFloatToString.enabled': 'true'})
