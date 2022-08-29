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
from marks import ignore_order, allow_non_gpu, incompat, validate_execs_in_gpu_plan
from spark_session import with_cpu_session, with_spark_session

_adaptive_conf = { "spark.sql.adaptive.enabled": "true" }

# Dynamic switching of join strategies
# Dynamic coalescing of shuffle partitions
# Dynamically Handle Skew Joins

_adaptive_coalese_conf = copy_and_update(_adaptive_conf, {
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
})


def create_skew_df(spark, length):
    root = spark.range(0, length)
    mid = length / 2
    left = root.select(
        when(col('id') < mid / 2, mid).
            otherwise('id').alias("key1"),
        col('id').alias("value1")
    )
    right = root.select(
        when(col('id') < mid, mid).
            otherwise('id').alias("key2"),
        col('id').alias("value2")
    )
    return left, right


@ignore_order(local=True)
def test_aqe_skew_join():
    def do_join(spark):
        left, right = create_skew_df(spark, 500)
        left.createOrReplaceTempView("skewData1")
        right.createOrReplaceTempView("skewData2")
        return spark.sql("SELECT * FROM skewData1 join skewData2 ON key1 = key2")

    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_adaptive_conf)

@ignore_order(local=True)
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_join_parquet(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).orderBy('a').write.parquet(data_path)
    )

    def do_it(spark):
        spark.read.parquet(data_path).createOrReplaceTempView('df1')
        spark.read.parquet(data_path).createOrReplaceTempView('df2')
        return spark.sql("select count(*) from df1,df2 where df1.a = df2.a")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=_adaptive_conf)


@ignore_order(local=True)
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_join_parquet_batch(spark_tmp_path, data_gen):
    # force v2 source for parquet to use BatchScanExec
    conf = copy_and_update(_adaptive_conf, {
        "spark.sql.sources.useV1SourceList": ""
    })

    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'

    def do_it(spark):
        spark.read.parquet(data_path).createOrReplaceTempView('df1')
        spark.read.parquet(data_path).createOrReplaceTempView('df2')
        return spark.sql("select count(*) from df1,df2 where df1.a = df2.a")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)


@ignore_order
@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['true', 'false'], ids=idfn)
@allow_non_gpu('ShuffleExchangeExec')
def test_aqe_join_sum_window(data_gen, aqe_enabled):
    conf = {'spark.sql.adaptive.sql.enabled': aqe_enabled,
            'spark.rapids.sql.batchSizeBytes': '100',
            'spark.rapids.sql.explain': 'NONE'}

    def do_it(spark):
        agg_table = gen_df(spark, StructGen([('a_1', LongRangeGen()), ('c', data_gen)], nullable=False))
        part_table = gen_df(spark, StructGen([('a_2', LongRangeGen()), ('b', byte_gen)], nullable=False))
        agg_table.createOrReplaceTempView("agg")
        part_table.createOrReplaceTempView("part")
        return spark.sql("select b, sum(c) as sum_c, sum(c)/sum(sum(c)) over (partition by b) as r_c from agg, part where a_1 = a_2 group by b order by b, r_c")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf = conf)
