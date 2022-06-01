# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import spark_tmp_table_factory, is_databricks_runtime
from data_gen import *
from marks import ignore_order, allow_non_gpu
from spark_session import is_before_spark_320, with_cpu_session


def create_dim_table(table_name, table_format, length=500):
    def fn(spark):
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('skey', IntegerGen(nullable=False, min_val=0, max_val=4, special_cases=[])),
            ('ex_key', IntegerGen(nullable=False, min_val=0, max_val=3, special_cases=[])),
            ('value', int_gen),
            ('filter', RepeatSeqGen(
                IntegerGen(min_val=0, max_val=length, special_cases=[]), length=length // 20))
        ], length)
        df.cache()
        df.write.format(table_format) \
            .mode("overwrite") \
            .saveAsTable(table_name)
        return df.select('filter').first()[0]

    return with_cpu_session(fn)


def create_fact_table(table_name, table_format, length=2000):
    def fn(spark):
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('skey', IntegerGen(nullable=False, min_val=0, max_val=4, special_cases=[])),
            # ex_key is not a partition column
            ('ex_key', IntegerGen(nullable=False, min_val=0, max_val=3, special_cases=[])),
            ('value', int_gen)], length)
        df.write.format(table_format) \
            .mode("overwrite") \
            .partitionBy('key', 'skey') \
            .saveAsTable(table_name)
    with_cpu_session(fn)


_dpp_conf = [('spark.sql.optimizer.dynamicPartitionPruning.enabled', 'true')]
_exchange_reuse_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'true'),
    ('spark.sql.exchange.reuse', 'true')
]
_bypass_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'true'),
    ('spark.sql.exchange.reuse', 'false')
]
_no_exchange_reuse_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'false'),
    ('spark.sql.exchange.reuse', 'false')
]
_dpp_fallback_conf = _dpp_conf + [
    ('spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly', 'false'),
    ('spark.sql.exchange.reuse', 'false'),
    ('spark.sql.optimizer.dynamicPartitionPruning.useStats', 'false'),
    ('spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio', '0'),
]

_statements = [
    '''
    SELECT fact.key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key
    WHERE dim.filter = {2} AND fact.value > 0
    GROUP BY fact.key
    ''',
    '''
    SELECT f.key, sum(f.value)
    FROM (SELECT *, struct(key, skey) AS keys FROM {0} fact) f
    JOIN (SELECT *, struct(key, skey) AS keys FROM {1} dim) d
    ON f.keys = d.keys
    WHERE d.filter = {2}
    GROUP BY f.key
    ''',
    '''
    SELECT fact.key, fact.skey, fact.ex_key, sum(fact.value)
    FROM {0} fact
    JOIN {1} dim
    ON fact.key = dim.key AND fact.skey = dim.skey AND fact.ex_key = dim.ex_key
    WHERE dim.filter = {2}
    GROUP BY fact.key, fact.skey, fact.ex_key
    ''',
    # This query checks the pattern of reused broadcast subquery: ReusedSubquery(SubqueryBroadcast(...))
    # https://github.com/NVIDIA/spark-rapids/issues/4625
    """
    SELECT key, max(value)
    FROM (
        SELECT fact.key as key, fact.value as value
        FROM {0} fact
        JOIN {1} dim
        ON fact.key = dim.key
        WHERE dim.filter = {2}
    UNION ALL
        SELECT fact.key as key, fact.value as value
        FROM {0} fact
        JOIN {1} dim
        ON fact.key = dim.key
        WHERE dim.filter = {2}
    )
    GROUP BY key
    """
]


def __dpp_reuse_broadcast_exchange(store_format, s_index, spark_tmp_table_factory, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format, length=10000)
    filter_val = create_dim_table(dim_table, store_format, length=2000)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # The existence of GpuSubqueryBroadcastExec indicates the reuse works on the GPU
        exist_classes='DynamicPruningExpression,GpuSubqueryBroadcastExec,ReusedExchangeExec',
        conf=dict(_exchange_reuse_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


# When BroadcastExchangeExec is available on filtering side, and it can be reused:
# DynamicPruningExpression(InSubqueryExec(value, GpuSubqueryBroadcastExec)))
@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
def test_dpp_reuse_broadcast_exchange_aqe_off(store_format, s_index, spark_tmp_table_factory):
    __dpp_reuse_broadcast_exchange(store_format, s_index, spark_tmp_table_factory, 'false')


# Only in Spark 3.2.0+ AQE and DPP can be both enabled
@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ AQE and DPP can be both enabled")
def test_dpp_reuse_broadcast_exchange_aqe_on(store_format, s_index, spark_tmp_table_factory):
    __dpp_reuse_broadcast_exchange(store_format, s_index, spark_tmp_table_factory, 'true')


# The SubqueryBroadcast can work on GPU even if the scan who holds it fallbacks into CPU.
@ignore_order
@pytest.mark.allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
def test_dpp_reuse_broadcast_exchange_cpu_scan(spark_tmp_table_factory):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, 'parquet', length=10000)
    filter_val = create_dim_table(dim_table, 'parquet', length=2000)
    statement = _statements[0].format(fact_table, dim_table, filter_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # The existence of GpuSubqueryBroadcastExec indicates the reuse works on the GPU
        exist_classes='FileSourceScanExec,GpuSubqueryBroadcastExec,ReusedExchangeExec',
        conf=dict(_exchange_reuse_conf + [
            ('spark.sql.adaptive.enabled', 'false'),
            ('spark.rapids.sql.format.parquet.read.enabled', 'false')]))


# When BroadcastExchange is not available and non-broadcast DPPs are forbidden, Spark will bypass it:
# DynamicPruningExpression(Literal.TrueLiteral)
def __dpp_bypass(store_format, s_index, spark_tmp_table_factory, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)
    filter_val = create_dim_table(dim_table, store_format)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # Bypass with a true literal, if we can not reuse broadcast exchange.
        exist_classes='DynamicPruningExpression',
        non_exist_classes='SubqueryExec,SubqueryBroadcastExec',
        conf=dict(_bypass_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
def test_dpp_bypass_aqe_off(store_format, s_index, spark_tmp_table_factory):
    __dpp_bypass(store_format, s_index, spark_tmp_table_factory, 'false')


@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ AQE and DPP can be both enabled")
def test_dpp_bypass_aqe_on(store_format, s_index, spark_tmp_table_factory):
    __dpp_bypass(store_format, s_index, spark_tmp_table_factory, 'true')


# When BroadcastExchange is not available, but it is still worthwhile to run DPP,
# then Spark will plan an extra Aggregate to collect filtering values:
# DynamicPruningExpression(InSubqueryExec(value, SubqueryExec(Aggregate(...))))
def __dpp_via_aggregate_subquery(store_format, s_index, spark_tmp_table_factory, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)
    filter_val = create_dim_table(dim_table, store_format)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # SubqueryExec appears if we plan extra subquery for DPP
        exist_classes='DynamicPruningExpression,SubqueryExec',
        conf=dict(_no_exchange_reuse_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
def test_dpp_via_aggregate_subquery_aqe_off(store_format, s_index, spark_tmp_table_factory):
    __dpp_via_aggregate_subquery(store_format, s_index, spark_tmp_table_factory, 'false')


@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ AQE and DPP can be both enabled")
def test_dpp_via_aggregate_subquery_aqe_on(store_format, s_index, spark_tmp_table_factory):
    __dpp_via_aggregate_subquery(store_format, s_index, spark_tmp_table_factory, 'true')


# When BroadcastExchange is not available, Spark will skip DPP if there is no potential benefit
def __dpp_skip(store_format, s_index, spark_tmp_table_factory, aqe_enabled):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)
    filter_val = create_dim_table(dim_table, store_format)
    statement = _statements[s_index].format(fact_table, dim_table, filter_val)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        # SubqueryExec appears if we plan extra subquery for DPP
        non_exist_classes='DynamicPruningExpression',
        conf=dict(_dpp_fallback_conf + [('spark.sql.adaptive.enabled', aqe_enabled)]))


@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
def test_dpp_skip_aqe_off(store_format, s_index, spark_tmp_table_factory):
    __dpp_skip(store_format, s_index, spark_tmp_table_factory, 'false')


@ignore_order
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.parametrize('s_index', list(range(len(_statements))), ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ AQE and DPP can be both enabled")
def test_dpp_skip_aqe_on(store_format, s_index, spark_tmp_table_factory):
    __dpp_skip(store_format, s_index, spark_tmp_table_factory, 'true')


# GPU verification on https://issues.apache.org/jira/browse/SPARK-34436
def _test_dpp_like_any(store_format, is_aqe_on, spark_tmp_table_factory):
    fact_table, dim_table = spark_tmp_table_factory.get(), spark_tmp_table_factory.get()
    create_fact_table(fact_table, store_format)

    def create_dim_table_for_like(spark):
        df = gen_df(spark, [
            ('key', IntegerGen(nullable=False, min_val=0, max_val=9, special_cases=[])),
            ('filter', StringGen(pattern='[0-9]{2,10}')),
        ], 100)
        df.write.format(store_format).mode("overwrite").saveAsTable(dim_table)
    with_cpu_session(create_dim_table_for_like)

    statement = """
    SELECT f.key, f.skey, f.value
    FROM {0} f JOIN {1} s
    ON f.key = s.key
    WHERE s.filter LIKE ANY ('%00%', '%01%', '%10%', '%11%')
    """.format(fact_table, dim_table)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(statement),
        exist_classes='DynamicPruningExpression,GpuSubqueryBroadcastExec,ReusedExchangeExec',
        conf=dict(_exchange_reuse_conf + [('spark.sql.adaptive.enabled', is_aqe_on)]))


@ignore_order
@allow_non_gpu('FilterExec')
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
def test_dpp_like_any_aqe_off(store_format, spark_tmp_table_factory):
    _test_dpp_like_any(store_format, False, spark_tmp_table_factory)


@ignore_order
@allow_non_gpu('FilterExec')
@pytest.mark.parametrize('store_format', ['parquet', 'orc'], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="DPP can not cooperate with rapids plugin on Databricks runtime")
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2.0+ AQE and DPP can be both enabled")
def test_dpp_like_any_aqe_on(store_format, spark_tmp_table_factory):
    _test_dpp_like_any(store_format, True, spark_tmp_table_factory)
