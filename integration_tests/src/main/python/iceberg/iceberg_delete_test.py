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
def test_iceberg_delete_simple(spark_tmp_table_factory):
    """Test simple DELETE with WHERE clause on unpartitioned table"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 > 50")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_with_and_condition(spark_tmp_table_factory):
    """Test DELETE with AND condition"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 > 0 AND _c3 < 100")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_with_or_condition(spark_tmp_table_factory):
    """Test DELETE with OR condition"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c0 < 0 OR _c1 < 0")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_with_in_condition(spark_tmp_table_factory):
    """Test DELETE with IN condition"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 IN (1, 2, 3)")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_bucket_partitioned_table(spark_tmp_table_factory):
    """Test DELETE on bucket-partitioned table"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 > 50"),
        partition_col_sql="bucket(16, _c2)"
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_with_string_comparison(spark_tmp_table_factory):
    """Test DELETE with string comparison"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c6 IS NOT NULL AND _c6 > 'abc'")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_multiple_types(spark_tmp_table_factory):
    """Test DELETE on table with multiple data types (int, long, double, boolean)"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c5 < 10.0 OR _c7 = false")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_all_rows(spark_tmp_table_factory):
    """Test DELETE that removes all rows"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 IS NOT NULL OR _c2 IS NULL")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_no_rows(spark_tmp_table_factory):
    """Test DELETE that matches no rows"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 > 1000000")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_with_date_condition(spark_tmp_table_factory):
    """Test DELETE with date comparison"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c8 > DATE '2020-01-01'")
    )

@iceberg
@ignore_order(local=True)
def test_iceberg_delete_with_decimal_condition(spark_tmp_table_factory):
    """Test DELETE with decimal comparison"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c10 > 100.0")
    )


@allow_non_gpu("BatchScanExec")
@iceberg
@ignore_order(local=True)
def test_iceberg_delete_fallback_read_disabled(spark_tmp_table_factory):
    """Test DELETE falls back when Iceberg read is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    create_iceberg_table_with_data(table_name)
    
    def do_delete(spark):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
        return spark.sql(f"SELECT * FROM {table_name} ORDER BY _c2").collect()
    
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
def test_iceberg_delete_fallback_write_disabled(spark_tmp_table_factory):
    """Test DELETE falls back when Iceberg write is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    create_iceberg_table_with_data(table_name)
    
    def do_delete(spark):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
        return spark.sql(f"SELECT * FROM {table_name} ORDER BY _c2").collect()
    
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
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
        return spark.sql(f"SELECT * FROM {table_name} ORDER BY _c2").collect()
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec", "ShuffleExchangeExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
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
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
        return spark.sql(f"SELECT * FROM {table_name} ORDER BY _c2").collect()
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
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
        spark.sql(f"DELETE FROM {table_name} WHERE id > 1")
        return spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
def test_iceberg_delete_fallback_iceberg_disabled(spark_tmp_table_factory):
    """Test DELETE falls back when Iceberg is completely disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    table_name = f"{base_table_name}_test"
    
    create_iceberg_table_with_data(table_name)
    
    def do_delete(spark):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 > 50")
        return spark.sql(f"SELECT * FROM {table_name} ORDER BY _c2").collect()
    
    assert_gpu_fallback_collect(
        do_delete,
        "ReplaceDataExec",
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false"
        })
    )
