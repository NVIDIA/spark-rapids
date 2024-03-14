# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

from asserts import assert_equal, assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write
from data_gen import *
from delta_lake_utils import *
from marks import *
from spark_session import is_before_spark_320, is_databricks_runtime, supports_delta_lake_deletion_vectors, \
    with_cpu_session, with_gpu_session

delta_delete_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                            {"spark.rapids.sql.command.DeleteCommand": "true",
                                             "spark.rapids.sql.command.DeleteCommandEdge": "true"})

def delta_sql_delete_test(spark_tmp_path, use_cdf, dest_table_func, delete_sql,
                          check_func, partition_columns=None):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, partition_columns)
    def do_delete(spark, path):
        return spark.sql(delete_sql.format(path=path))
    with_cpu_session(setup_tables)
    check_func(data_path, do_delete)

def assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, dest_table_func, delete_sql,
                                    partition_columns=None,
                                    conf=delta_delete_enabled_conf,
                                    skip_sql_result_check=False):
    def read_data(spark, path):
        read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
        df = read_func(spark, path)
        return df.sort(df.columns)
    def checker(data_path, do_delete):
        cpu_path = data_path + "/CPU"
        gpu_path = data_path + "/GPU"
        if not skip_sql_result_check:
            # compare resulting dataframe from the delete operation (some older Spark versions return empty here)
            cpu_result = with_cpu_session(lambda spark: do_delete(spark, cpu_path).collect(), conf=conf)
            gpu_result = with_gpu_session(lambda spark: do_delete(spark, gpu_path).collect(), conf=conf)
            assert_equal(cpu_result, gpu_result)
        # compare table data results, read both via CPU to make sure GPU write can be read by CPU
        cpu_result = with_cpu_session(lambda spark: read_data(spark, cpu_path).collect(), conf=conf)
        gpu_result = with_cpu_session(lambda spark: read_data(spark, gpu_path).collect(), conf=conf)
        assert_equal(cpu_result, gpu_result)
        # Using partition columns involves sorting, and there's no guarantees on the task
        # partitioning due to random sampling.
        if not partition_columns:
            with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    delta_sql_delete_test(spark_tmp_path, use_cdf, dest_table_func, delete_sql, checker,
                          partition_columns)

@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf",
                          [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                           {"spark.rapids.sql.format.parquet.enabled": "false"},
                           {"spark.rapids.sql.format.parquet.write.enabled": "false"},
                           {"spark.rapids.sql.command.DeleteCommand": "false"},
                           delta_writes_enabled_conf  # Test disabled by default
                           ], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_delete_disabled_fallback(spark_tmp_path, disable_conf):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=False)
    def write_func(spark, path):
        delete_sql="DELETE FROM delta.`{}`".format(path)
        spark.sql(delete_sql)
    with_cpu_session(setup_tables)
    assert_gpu_fallback_write(write_func, read_delta_path, data_path,
                              "ExecutedCommandExec", disable_conf)

@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), \
    reason="Deletion vectors new in Delta Lake 2.4 / Apache Spark 3.4")
def test_delta_deletion_vector_fallback(spark_tmp_path, use_cdf):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=use_cdf, enable_deletion_vectors=True)
    def write_func(spark, path):
        delete_sql="DELETE FROM delta.`{}`".format(path)
        spark.sql(delete_sql)
    with_cpu_session(setup_tables)
    disable_conf = copy_and_update(delta_delete_enabled_conf,
        {"spark.databricks.delta.delete.deletionVectors.persistent": "true"})

    assert_gpu_fallback_write(write_func, read_delta_path, data_path,
                              "ExecutedCommandExec", disable_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_delete_entire_table(spark_tmp_path, use_cdf, partition_columns):
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen)
    delete_sql = "DELETE FROM delta.`{path}`"
    # Databricks recently changed how the num_affected_rows is computed
    # on deletes of entire files, RAPIDS Accelerator has yet to be updated.
    # https://github.com/NVIDIA/spark-rapids/issues/8123
    skip_sql_result = is_databricks_runtime()
    assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, generate_dest_data,
                                    delete_sql, partition_columns,
                                    skip_sql_result_check=skip_sql_result)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [["a"], ["a", "b"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_delete_partitions(spark_tmp_path, use_cdf, partition_columns):
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen)
    delete_sql = "DELETE FROM delta.`{path}` WHERE a = 3"
    # Databricks recently changed how the num_affected_rows is computed
    # on deletes of entire files, RAPIDS Accelerator has yet to be updated.
    # https://github.com/NVIDIA/spark-rapids/issues/8123
    skip_sql_result = is_databricks_runtime()
    assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, generate_dest_data,
                                    delete_sql, partition_columns,
                                    skip_sql_result_check=skip_sql_result)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@datagen_overrides(seed=0, permanent=True, reason='https://github.com/NVIDIA/spark-rapids/issues/9884')
def test_delta_delete_rows(spark_tmp_path, use_cdf, partition_columns):
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    delete_sql = "DELETE FROM delta.`{path}` WHERE b < 'd'"
    assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, generate_dest_data,
                                    delete_sql, partition_columns)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@datagen_overrides(seed=0, permanent=True, reason='https://github.com/NVIDIA/spark-rapids/issues/9884')
def test_delta_delete_dataframe_api(spark_tmp_path, use_cdf, partition_columns):
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/DELTA_DATA"
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    with_cpu_session(lambda spark: setup_delta_dest_tables(spark, data_path, generate_dest_data, use_cdf, partition_columns))
    def do_delete(spark, path):
        dest_table = DeltaTable.forPath(spark, path)
        dest_table.delete("b > 'c'")
    read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
    assert_gpu_and_cpu_writes_are_equal_collect(do_delete, read_func, data_path,
                                                conf=delta_delete_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
