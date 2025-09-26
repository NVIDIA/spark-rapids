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
from typing import Callable, Any

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update
from iceberg import create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name, iceberg_full_gens_list, iceberg_write_enabled_conf
from marks import iceberg, ignore_order, allow_non_gpu
from spark_session import with_gpu_session, with_cpu_session, is_spark_35x

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                       reason="Current spark-rapids only support spark 3.5.x")


def do_test_insert_into_table_sql(spark_tmp_table_factory,
                                  create_table_func: Callable[[str], Any]):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf = iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf = iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_unpartitioned_table(spark_tmp_table_factory, format_version, write_distribution_mode):
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_unpartitioned_table_all_cols_fallback(spark_tmp_table_factory, format_version, write_distribution_mode):
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    def this_gen_df(spark):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    def insert_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop, df_gen=this_gen_df)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("fanout", [True, False], ids=lambda x: f"fanout={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_partitioned_table(spark_tmp_table_factory, format_version, fanout, write_distribution_mode):
    table_prop = {"format-version": format_version,
                  "write.spark.fanout.enabled": str(fanout).lower(),
                  "write.distribution-mode": write_distribution_mode}

    def create_table_and_set_write_order(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
            table_prop=table_prop)

        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY _c2, _c3, _c4"
        with_cpu_session(lambda spark: spark.sql(sql).collect())

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        create_table_and_set_write_order)

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_partitioned_table_all_cols_fallback(spark_tmp_table_factory, format_version, write_distribution_mode):
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    def this_gen_df(spark):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    def insert_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name,
                          partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
                          table_prop=table_prop,
                          df_gen=this_gen_df)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_table_unsupported_file_format_fallback(
        spark_tmp_table_factory, format_version, file_format, write_distribution_mode):
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode,
                  "write.format.default": file_format}

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_into_iceberg_table_fallback_when_conf_disabled(
        spark_tmp_table_factory, format_version, write_distribution_mode, conf_key):
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    updated_conf = copy_and_update(iceberg_write_enabled_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = updated_conf)