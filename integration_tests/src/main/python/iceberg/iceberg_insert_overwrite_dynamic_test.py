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
from data_gen import gen_df, copy_and_update
from iceberg import create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name, iceberg_full_gens_list, iceberg_write_enabled_conf
from marks import iceberg, ignore_order, allow_non_gpu
from spark_session import with_gpu_session, with_cpu_session, is_spark_35x

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                       reason="Current spark-rapids only support spark 3.5.x")


# Seed for initial data to ensure different data from overwrite operations
INITIAL_DATA_SEED = 42

# Configuration for dynamic partition overwrite mode
dynamic_overwrite_conf = copy_and_update(iceberg_write_enabled_conf, {
    "spark.sql.sources.partitionOverwriteMode": "dynamic"
})


def do_test_insert_overwrite_dynamic(spark_tmp_table_factory,
                                      create_table_func: Callable[[str], Any]):
    """Test INSERT OVERWRITE with dynamic partition overwrite mode."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def initial_insert(spark, table_name: str):
        """Insert initial data to have something to overwrite."""
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    # Insert initial data on both CPU and GPU
    with_gpu_session(lambda spark: initial_insert(spark, gpu_table_name),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: initial_insert(spark, cpu_table_name),
                     conf=iceberg_write_enabled_conf)

    def overwrite_data(spark, table_name: str):
        """Perform INSERT OVERWRITE with dynamic mode - only overwrites touched partitions."""
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    # Perform overwrite on both tables with dynamic mode
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf=dynamic_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf=dynamic_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_overwrite_dynamic_unpartitioned_table(spark_tmp_table_factory, format_version, write_distribution_mode):
    """Test dynamic overwrite on unpartitioned tables - should run on GPU."""
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    do_test_insert_overwrite_dynamic(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("fanout", [True, False], ids=lambda x: f"fanout={x}")
def test_insert_overwrite_dynamic_bucket_partitioned(spark_tmp_table_factory, format_version, write_distribution_mode, fanout):
    """Test dynamic overwrite with bucket partitioning - should run on GPU."""
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode,
                  "write.spark.fanout.enabled": str(fanout).lower()}

    def create_table_with_partition(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
            table_prop=table_prop)

    do_test_insert_overwrite_dynamic(
        spark_tmp_table_factory,
        create_table_with_partition)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwritePartitionsDynamicExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_overwrite_dynamic_unsupported_data_types_fallback(spark_tmp_table_factory, format_version, write_distribution_mode):
    """Test that INSERT OVERWRITE falls back to CPU with unsupported data types."""
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    def this_gen_df(spark):
        cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    table_name = get_full_table_name(spark_tmp_table_factory)

    # Create table
    create_iceberg_table(table_name, table_prop=table_prop, df_gen=this_gen_df)

    # Initial insert (CPU only to set up the table)
    def initial_insert(spark, table_name: str):
        # Use a specific seed for initial data to ensure different data from overwrite
        cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        df = gen_df(spark, list(zip(cols, iceberg_full_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: initial_insert(spark, table_name),
                     conf=iceberg_write_enabled_conf)

    # Overwrite with dynamic mode (assert GPU falls back to CPU)
    def overwrite_data(spark, table_name: str):
        # Use default seed for overwrite data - different from initial
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwritePartitionsDynamicExec",
                                conf=dynamic_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwritePartitionsDynamicExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("_c2", id="identity"),
    pytest.param("truncate(5, _c6)", id="truncate"),
    pytest.param("year(_c9)", id="year"),
    pytest.param("month(_c9)", id="month"),
    pytest.param("day(_c9)", id="day"),
    pytest.param("hour(_c9)", id="hour"),
    pytest.param("bucket(8, _c6)", id="bucket_unsupported_type"),
])
def test_insert_overwrite_dynamic_unsupported_partition_fallback(
        spark_tmp_table_factory, format_version, write_distribution_mode, partition_col_sql):
    """Test that INSERT OVERWRITE falls back to CPU with unsupported partition functions."""
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    table_name = get_full_table_name(spark_tmp_table_factory)

    # Create table
    create_iceberg_table(table_name,
                         partition_col_sql=partition_col_sql,
                         table_prop=table_prop)

    # Initial insert (CPU only to set up the table)
    def initial_insert(spark, table_name: str):
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: initial_insert(spark, table_name),
                     conf=iceberg_write_enabled_conf)

    # Overwrite with dynamic mode (assert GPU falls back to CPU)
    def overwrite_data(spark, table_name: str):
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwritePartitionsDynamicExec",
                                conf=dynamic_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwritePartitionsDynamicExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_overwrite_dynamic_unsupported_file_format_fallback(
        spark_tmp_table_factory, format_version, file_format, write_distribution_mode):
    """Test that INSERT OVERWRITE falls back to CPU with unsupported file formats."""
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode,
                  "write.format.default": file_format}

    table_name = get_full_table_name(spark_tmp_table_factory)

    # Create table
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (CPU only to set up the table)
    def initial_insert(spark, table_name: str):
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: initial_insert(spark, table_name),
                     conf=iceberg_write_enabled_conf)

    # Overwrite with dynamic mode (assert GPU falls back to CPU)
    def overwrite_data(spark, table_name: str):
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwritePartitionsDynamicExec",
                                conf=dynamic_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwritePartitionsDynamicExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_overwrite_dynamic_fallback_when_conf_disabled(
        spark_tmp_table_factory, format_version, write_distribution_mode, conf_key):
    """Test that INSERT OVERWRITE falls back to CPU when Iceberg write is disabled."""
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    table_name = get_full_table_name(spark_tmp_table_factory)

    # Create table
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (CPU only to set up the table)
    def initial_insert(spark, table_name: str):
        # Use a specific seed for initial data to ensure different data from overwrite
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_DATA_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_cpu_session(lambda spark: initial_insert(spark, table_name),
                     conf=iceberg_write_enabled_conf)

    # Overwrite with dynamic mode (assert GPU falls back to CPU)
    def overwrite_data(spark, table_name: str):
        # Use default seed for overwrite data - different from initial
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    updated_conf = copy_and_update(dynamic_overwrite_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwritePartitionsDynamicExec",
                                conf=updated_conf)
