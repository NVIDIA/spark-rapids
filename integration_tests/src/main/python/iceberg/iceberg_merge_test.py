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

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_write_sql
from conftest import is_iceberg_remote_catalog
from data_gen import *
from iceberg import (create_iceberg_table, get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_base_table_cols, iceberg_gens_list, iceberg_full_gens_list,
                     rapids_reader_types)
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_spark_35x, with_gpu_session, with_cpu_session

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Current spark-rapids only support spark 3.5.x")

# Base configuration for Iceberg MERGE tests
iceberg_merge_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})


def create_iceberg_table_with_merge_data(
        table_name: str,
        partition_col_sql=None,
        table_properties=None):
    """Helper function to create and populate an Iceberg table for MERGE tests."""
    # Always use copy-on-write mode for these tests
    base_props = {
        'format-version': '2',
        'write.merge.mode': 'copy-on-write'
    }
    if table_properties:
        base_props.update(table_properties)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    create_iceberg_table(table_name,
                        partition_col_sql=partition_col_sql,
                        table_prop=base_props,
                        df_gen=data_gen)
    
    # Insert data
    def insert_data(spark):
        df = data_gen(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)


def do_merge_test(
        spark_tmp_table_factory,
        merge_sql_func,
        partition_col_sql=None,
        table_properties=None,
        reader_type='COALESCING'):
    """
    Helper function to test MERGE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        merge_sql_func: Function that takes (spark, target_table, source_table) and executes MERGE
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        reader_type: Rapids reader type for parquet reading
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_target_table = f"{base_table_name}_cpu_target"
    gpu_target_table = f"{base_table_name}_gpu_target"
    source_table = f"{base_table_name}_source"
    
    # Create identical target tables for CPU and GPU
    create_iceberg_table_with_merge_data(cpu_target_table, partition_col_sql, table_properties)
    create_iceberg_table_with_merge_data(gpu_target_table, partition_col_sql, table_properties)
    
    # Create source table (shared between CPU and GPU tests)
    create_iceberg_table_with_merge_data(source_table, partition_col_sql, table_properties)
    
    # Merge reader_type into configuration
    test_conf = copy_and_update(iceberg_merge_enabled_conf, {
        "spark.rapids.sql.format.parquet.reader.type": reader_type
    })
    
    # Execute MERGE on GPU
    def do_gpu_merge(spark):
        merge_sql_func(spark, gpu_target_table, source_table)
    
    with_gpu_session(do_gpu_merge, conf=test_conf)
    
    # Execute MERGE on CPU
    def do_cpu_merge(spark):
        merge_sql_func(spark, cpu_target_table, source_table)
    
    with_cpu_session(do_cpu_merge)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_target_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_target_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu("Keep", "Discard", "Split")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('partition_col_sql', [None, "bucket(16, _c2)"])
@pytest.mark.parametrize('merge_sql', [
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="basic_update_insert"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED AND t._c2 % 2 = 0 THEN UPDATE SET t._c1 = s._c1
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="conditional_update"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED THEN DELETE
        """,
        id="delete_when_matched"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN MATCHED AND t._c2 < 0 THEN DELETE
        WHEN MATCHED AND t._c2 >= 0 THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="multiple_matched_clauses"),
    pytest.param(
        """
        MERGE INTO {target} t USING {source} s ON t._c0 = s._c0
        WHEN NOT MATCHED THEN INSERT *
        """,
        id="insert_only"),
])
def test_iceberg_merge_copy_on_write(spark_tmp_table_factory, reader_type, partition_col_sql, merge_sql):
    """Test various MERGE operations on Iceberg tables (partitioned and unpartitioned)."""
    do_merge_test(
        spark_tmp_table_factory,
        lambda spark, target, source: spark.sql(merge_sql.format(target=target, source=source)),
        partition_col_sql=partition_col_sql,
        reader_type=reader_type
    )


@allow_non_gpu("ReplaceDataExec", "MergeRowsExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_merge_fallback_write_disabled(spark_tmp_table_factory, reader_type):
    """Test MERGE falls back when Iceberg write is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    create_iceberg_table_with_merge_data(cpu_target_table)
    create_iceberg_table_with_merge_data(gpu_target_table)
    create_iceberg_table_with_merge_data(source_table)
    
    # Phase 2: MERGE operation (to be tested with fallback)
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        ["ReplaceDataExec"],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false",
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )


@allow_non_gpu("ReplaceDataExec", "MergeRowsExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("_c2", id="identity"),
    pytest.param("truncate(5, _c6)", id="truncate"),
    pytest.param("year(_c9)", id="year"),
    pytest.param("month(_c9)", id="month"),
    pytest.param("day(_c9)", id="day"),
    pytest.param("hour(_c9)", id="hour"),
    pytest.param("bucket(8, _c6)", id="bucket_unsupported_type"),
])
def test_iceberg_merge_fallback_unsupported_partition_transform(
        spark_tmp_table_factory, reader_type, partition_col_sql):
    """Test MERGE falls back with unsupported partition transforms"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data
    def init_table(table_name):
        table_props = {
            'format-version': '2',
            'write.merge.mode': 'copy-on-write'
        }
        
        def data_gen(spark):
            return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        
        create_iceberg_table(table_name,
                            partition_col_sql=partition_col_sql,
                            table_prop=table_props,
                            df_gen=data_gen)
        
        def insert_data(spark):
            df = data_gen(spark)
            df.writeTo(table_name).append()
        
        with_cpu_session(insert_data)
    
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    init_table(cpu_target_table)
    init_table(gpu_target_table)
    init_table(source_table)
    
    # Phase 2: MERGE operation (to be tested with fallback)
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        ["ReplaceDataExec"],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )


@allow_non_gpu("ReplaceDataExec", "MergeRowsExec", "BatchScanExec", "ShuffleExchangeExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_iceberg_merge_fallback_unsupported_file_format(spark_tmp_table_factory, reader_type, file_format):
    """Test MERGE falls back with unsupported file formats (ORC, Avro)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables
    def init_table(table_name):
        # Create table with parquet, insert data, then change format
        table_props = {
            'format-version': '2',
            'write.merge.mode': 'copy-on-write',
            'write.format.default': 'parquet'
        }
        
        create_iceberg_table(table_name, table_prop=table_props, df_gen=data_gen)
        
        def insert_data(spark):
            df = data_gen(spark)
            df.writeTo(table_name).append()
        
        with_cpu_session(insert_data)
        
        # Change format to unsupported
        def change_format(spark):
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.format.default' = '{file_format}')")
        
        with_cpu_session(change_format)
    
    cpu_target_table = f'{base_table_name}_cpu_target'
    gpu_target_table = f'{base_table_name}_gpu_target'
    source_table = f'{base_table_name}_source'
    
    init_table(cpu_target_table)
    init_table(gpu_target_table)
    init_table(source_table)
    
    # Phase 2: MERGE operation
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    def read_func(spark, table_name):
        # Select 1 to avoid unsupported file format read error
        return spark.sql(f"SELECT 1")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        ["ReplaceDataExec"],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )


@allow_non_gpu("ReplaceDataExec", "MergeRowsExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13649")
def test_iceberg_merge_fallback_iceberg_disabled(spark_tmp_table_factory, reader_type):
    """Test MERGE falls back when Iceberg is completely disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    cpu_target_table = f'{base_table_name}_target_cpu'
    gpu_target_table = f'{base_table_name}_target_gpu'
    source_table = f'{base_table_name}_source'
    
    create_iceberg_table_with_merge_data(cpu_target_table)
    create_iceberg_table_with_merge_data(gpu_target_table)
    create_iceberg_table_with_merge_data(source_table)
    
    def write_func(spark, target_table_name):
        spark.sql(f"""
            MERGE INTO {target_table_name} t
            USING {source_table} s
            ON t._c0 = s._c0
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name + "_target",
        ["ReplaceDataExec"],
        conf=copy_and_update(iceberg_merge_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false",
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )

