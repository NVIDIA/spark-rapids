# Copyright (c) 2021, NVIDIA CORPORATION.
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
from marks import validate_execs_in_gpu_plan

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
def test_read_strings():
    assert_gpu_and_cpu_are_equal_collect(readTable("string", columnarClass))

@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_all_types():
    assert_gpu_and_cpu_are_equal_collect(
       readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf={'spark.rapids.sql.castFloatToString.enabled': 'true'})

@validate_execs_in_gpu_plan('HostColumnarToGpu')
def test_read_arrow_off():
    assert_gpu_and_cpu_are_equal_collect(
        readTable("int,bool,byte,short,long,string,float,double,date,timestamp", columnarClass),
            conf={'spark.rapids.arrowCopyOptmizationEnabled': 'false',
                  'spark.rapids.sql.castFloatToString.enabled': 'true'})
