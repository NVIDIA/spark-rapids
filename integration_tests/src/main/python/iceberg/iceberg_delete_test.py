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

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from data_gen import *
from iceberg import (create_iceberg_table, get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_base_table_cols, iceberg_gens_list)
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_spark_35x, with_cpu_session, with_gpu_session

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Current spark-rapids only support spark 3.5.x")

# Configuration for copy-on-write DELETE operations
iceberg_delete_cow_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Fixed seed for reproducible test data. Iceberg's delete test plan will be different with different data and filter. For example, 
# if deleted data exactly match some data files, we could remove all files using delete metadata only operation, then the physical plan 
# would be DeleteFromTableExec.
DELETE_TEST_SEED = 42
DELETE_TEST_SEED_OVERRIDE_REASON = "Ensure reproducible test data for DELETE operations"

def create_iceberg_table_with_data(table_name: str, 
                                   partition_col_sql=None,
                                   data_gen_func=None,
                                   table_properties=None):
    """Helper function to create and populate an Iceberg table for DELETE tests."""
    # Always use copy-on-write mode for these tests
    base_props = {
        'format-version': '2',
        'write.delete.mode': 'copy-on-write'
    }
    if table_properties:
        base_props.update(table_properties)
    
    # Use standard iceberg table definition if no custom generator provided
    if data_gen_func is None:
        data_gen_func = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    create_iceberg_table(table_name, 
                        partition_col_sql=partition_col_sql,
                        table_prop=base_props,
                        df_gen=data_gen_func)
    
    # Insert data
    def insert_data(spark):
        df = data_gen_func(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)

def do_delete_test(spark_tmp_table_factory, delete_sql_func, data_gen_func=None, 
                  partition_col_sql=None, table_properties=None):
    """
    Helper function to test DELETE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        delete_sql_func: Function that takes (spark, table_name) and executes DELETE
        data_gen_func: Function to generate test data
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Create identical tables for CPU and GPU
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties)
    
    # Execute DELETE on GPU
    def do_gpu_delete(spark):
        delete_sql_func(spark, gpu_table_name)
        
    with_gpu_session(do_gpu_delete, conf=iceberg_delete_cow_enabled_conf)
    
    # Execute DELETE on CPU
    def do_cpu_delete(spark):
        delete_sql_func(spark, cpu_table_name)
        
    with_cpu_session(do_cpu_delete, conf=iceberg_delete_cow_enabled_conf)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
def test_iceberg_delete_unpartitioned_table(spark_tmp_table_factory):
    """Test DELETE on unpartitioned table with fixed seed"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 > 50")
    )

@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
def test_iceberg_delete_partitioned_table(spark_tmp_table_factory):
    """Test DELETE on bucket-partitioned table with fixed seed"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 > 50"),
        partition_col_sql="bucket(16, _c2)"
    )


@allow_non_gpu("BatchScanExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
def test_iceberg_delete_fallback_read_disabled(spark_tmp_table_factory):
    """Test DELETE falls back when Iceberg read is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    create_iceberg_table_with_data(table_name)
    
    def do_delete(spark):
        return spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.read.enabled": "false"
        })
    )

@allow_non_gpu("ReplaceDataExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
def test_iceberg_delete_fallback_write_disabled(spark_tmp_table_factory):
    """Test DELETE falls back when Iceberg write is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    create_iceberg_table_with_data(table_name)
    
    def do_delete(spark):
        return spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false"
        })
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("_c2", id="identity"),
    pytest.param("truncate(5, _c6)", id="truncate"),
    pytest.param("year(_c9)", id="year"),
    pytest.param("month(_c9)", id="month"),
    pytest.param("day(_c9)", id="day"),
    pytest.param("hour(_c9)", id="hour"),
    pytest.param("bucket(8, _c6)", id="bucket_unsupported_type"),
])
def test_iceberg_delete_fallback_unsupported_partition_transform(spark_tmp_table_factory, partition_col_sql):
    """Test DELETE falls back with unsupported partition transforms"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    table_props = {
        'format-version': '2',
        'write.delete.mode': 'copy-on-write'
    }
    
    create_iceberg_table(table_name,
                        partition_col_sql=partition_col_sql,
                        table_prop=table_props,
                        df_gen=data_gen)
    
    def insert_data(spark):
        df = data_gen(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)
    
    def do_delete(spark):
        return spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec", "ShuffleExchangeExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_iceberg_delete_fallback_unsupported_file_format(spark_tmp_table_factory, file_format):
    """Test DELETE falls back with unsupported file formats (ORC, Avro)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    table_props = {
        'format-version': '2',
        'write.delete.mode': 'copy-on-write',
        'write.format.default': file_format
    }
    
    create_iceberg_table(table_name,
                        table_prop=table_props,
                        df_gen=data_gen)
    
    def insert_data(spark):
        df = data_gen(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)
    
    def do_delete(spark):
        return spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
def test_iceberg_delete_fallback_nested_types(spark_tmp_table_factory):
    """Test DELETE falls back with nested types (arrays, structs) - currently unsupported"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    # Use table with nested types (arrays and structs)
    def data_gen(spark):
        return spark.createDataFrame([
            (1, [1, 2, 3], {'field1': 'a', 'field2': 10}),
            (2, [4, 5, 6], {'field1': 'b', 'field2': 20}),
            (3, [7, 8, 9], {'field1': 'c', 'field2': 30})
        ], ['id', 'arr_col', 'struct_col'])
    
    table_props = {
        'format-version': '2',
        'write.delete.mode': 'copy-on-write'
    }
    
    create_iceberg_table(table_name,
                        table_prop=table_props,
                        df_gen=data_gen)
    
    def insert_data(spark):
        df = data_gen(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)
    
    def do_delete(spark):
        return spark.sql(f"DELETE FROM {table_name} WHERE id > 1")
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
def test_iceberg_delete_fallback_iceberg_disabled(spark_tmp_table_factory):
    """Test DELETE falls back when Iceberg is completely disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    create_iceberg_table_with_data(table_name)
    
    def do_delete(spark):
        return spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false"
        })
    )
