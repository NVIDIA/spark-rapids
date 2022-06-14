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
def test_non_zero_offset_fallback():
    def test_fn(spark):
        unary_op_df(spark, int_gen, num_slices=1).createOrReplaceTempView("offset_tmp")
        # `offset` is not exposed to Pyspark, so use the sql string.
        return spark.sql("select * from offset_tmp limit 10 offset 1")

    assert_gpu_fallback_collect(
        # We need some processing after the limit to avoid a CollectLimitExec
        lambda spark: test_fn(spark).repartition(1),
        'GlobalLimitExec',
        conf = {'spark.sql.execution.sortBeforeRepartition': 'false'})

    assert_gpu_fallback_collect(
        test_fn,
        'CollectLimitExec',
        conf = {'spark.sql.execution.sortBeforeRepartition': 'false'})
