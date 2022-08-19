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
from _pytest.mark.structures import ParameterSet
from pyspark.sql.functions import when, col
from pyspark.sql.types import *
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import is_databricks_runtime, is_emr_runtime
from data_gen import *
from marks import ignore_order, allow_non_gpu, incompat, validate_execs_in_gpu_plan, approximate_float
from spark_session import with_cpu_session, with_spark_session

_adaptive_conf = { "spark.sql.adaptive.enabled": "true",
                   "spark.rapids.sql.castFloatToString.enabled": "true",
                   "spark.rapids.sql.castDecimalToString.enabled": "true" }

# Dynamic switching of join strategies

# Dynamic coalescing of shuffle partitions
_adaptive_coalese_conf = copy_and_update(_adaptive_conf, {
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
})


# Dynamically Handle Skew Joins

def create_skew_df(spark, data_gen, length):
    rand = random.Random(0)
    data_gen.start(rand)
    next_val = None
    while next_val is None:
        next_val = data_gen.gen()
    root = two_col_df(spark, data_gen, SetValuesGen(data_gen.data_type, [next_val]), length)
    left = root.select(
        when(col('a') < col('b') / 2, col('b')).
            otherwise('a').alias("key1"),
        col('a').alias("value1")
    )
    right = root.select(
        when(col('a') < col('b'), col('b')).
            otherwise('a').alias("key2"),
        col('a').alias("value2")
    )
    return left, right


@ignore_order(local=True)
@approximate_float(abs=1e-6)
@allow_non_gpu("ShuffleExchangeExec")
@pytest.mark.parametrize("data_gen", numeric_gens + decimal_gens, ids=idfn)
def test_skew_join(data_gen):
    def do_join(spark):
        left, right = create_skew_df(spark, data_gen, length=512)
        left.createOrReplaceTempView("skewData1")
        right.createOrReplaceTempView("skewData2")
        return spark.sql("SELECT * FROM skewData1 join skewData2 ON key1 = key2")

    assert_gpu_and_cpu_are_equal_collect(do_join, _adaptive_conf)

