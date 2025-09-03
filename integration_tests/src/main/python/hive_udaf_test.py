# Copyright (c) 2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_sql
from data_gen import gen_df, IntegerGen, int_gen, long_gen, idfn
from spark_session import with_spark_session
from hive_udf_utils import *
from marks import ignore_order

projected_aggs_list = [
    "average_agg(i), average_agg(l)",
    "average_agg(i), max(i), average_agg(l), min(l)",
    "max(i), average_agg(i), min(l), average_agg(l)",
    "max(i), min(l), average_agg(i), average_agg(l)",
    "average_agg(i), average_agg(l), max(i), min(l)"
]

def hive_udaf_eval_fn(spark, data_gens):
    load_hive_udf(spark, "average_agg",
                  "com.nvidia.spark.rapids.tests.udf.hive.IntLongAverageHiveUDAF")
    return gen_df(spark, data_gens)


@ignore_order(local=True)
@pytest.mark.skip()
@pytest.mark.parametrize("aggs", projected_aggs_list, ids=idfn)
def test_groupby_with_hive_average_udaf(aggs):
    with_spark_session(skip_if_no_hive)
    # 'g' is the group key column, so at most 52 groups (include nulls)
    data_gens = [["g", IntegerGen(min_val=0, max_val=50)], ["i", int_gen], ["l", long_gen]]
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: hive_udaf_eval_fn(spark, data_gens),
        "groupby_hive_udaf_table",
        "SELECT g, {} FROM groupby_hive_udaf_table GROUP BY g".format(aggs),
        conf={"spark.sql.catalogImplementation": "hive"},
        debug=True)


@ignore_order(local=True)
@pytest.mark.skip()
@pytest.mark.parametrize("aggs", projected_aggs_list, ids=idfn)
def test_reduction_with_hive_average_udaf(aggs):
    with_spark_session(skip_if_no_hive)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: hive_udaf_eval_fn(spark, [["i", int_gen], ["l", long_gen]]),
        "reduction_hive_udaf_table",
        "SELECT {} FROM reduction_hive_udaf_table".format(aggs),
        conf={"spark.sql.catalogImplementation": "hive"})
