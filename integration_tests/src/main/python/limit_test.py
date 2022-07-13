# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu
from spark_session import is_before_spark_340


@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens + array_gens_sample + map_gens_sample + struct_gens_sample, ids=idfn)
def test_simple_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # We need some processing after the limit to avoid a CollectLimitExec
            lambda spark : unary_op_df(spark, data_gen, num_slices=1).limit(10).repartition(1),
            conf = {'spark.sql.execution.sortBeforeRepartition': 'false'})


@allow_non_gpu('CollectLimitExec', 'GlobalLimitExec', 'ShuffleExchangeExec')
@pytest.mark.skipif(is_before_spark_340(), reason='offset is introduced from Spark 3.4.0')
def test_non_zero_offset_on_gpu():
    conf = {
        'spark.rapids.sql.exec.CollectLimitExec': 'true',
        'spark.rapids.sql.exec.GlobalLimitExec': 'true'
    }

    # num_slices is the number of partitions of data frame
    # The number of rows in the dataframe is 2048
    def test_runner(sql, num_slices):
        def test_instance(spark):
            df = unary_op_df(spark, int_gen, num_slices=num_slices)
            df.createOrReplaceTempView("tmp_table")
            return spark.sql(sql)
        return test_instance

    sql = "select * from tmp_table limit {} offset {}"

    # Only one partition
    # Corner case: both limit and offset are 0
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(0, 0), 1), conf=conf)
    # offset < limit && limit < numRows
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(1024, 500), 1), conf=conf)
    # offset < limit && limit = numRows
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(2048, 456), 1), conf=conf)
    # offset < limit && limit > numRows
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(3000, 111), 1), conf=conf)
    # offset = limit
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(100, 100), 1), conf=conf)
    # offset > limit
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(100, 600), 1), conf=conf)



    # More than one partition
    # limit = offset = 0
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(0, 0), 20), conf=conf)
    # offset < limit && limit < numRows
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(1111, 500), 23), conf=conf)
    # offset < limit && limit = numRows
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(2048, 789), 11), conf=conf)
    # offset < limit && limit > numRows
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(3000, 789), 101), conf=conf)
    # offset = limit
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(100, 100), 67), conf=conf)
    # offset > limit
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(100, 600), 18), conf=conf)


    # With "sort by"
    sql = "select * from tmp_table sort by a limit {} offset {}"
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(1123, 50), 2), conf=conf)
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(200, 150), 3), conf=conf)

    sql = "select * from tmp_table sort by a desc limit {} offset {}"
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(200, 50), 5), conf=conf)
    assert_gpu_and_cpu_are_equal_collect(test_runner(sql.format(1230, 50), 5), conf=conf)

