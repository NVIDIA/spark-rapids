# Copyright (c) 2024, NVIDIA CORPORATION.
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

import pyspark.sql.functions as f
import string
from pyspark.sql.types import *

from asserts import *
from data_gen import *
from delta_lake_utils import *
from spark_session import is_databricks_runtime

# Databricks changes the number of files being written, so we cannot compare logs
num_slices_to_test = [10] if is_databricks_runtime() else [1, 10]


def make_df(spark, gen, num_slices):
    return three_col_df(spark, gen, SetValuesGen(StringType(), string.ascii_lowercase),
                        SetValuesGen(StringType(), string.ascii_uppercase), num_slices=num_slices)


def delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                         src_table_func, dest_table_func, merge_sql, check_func,
                         partition_columns=None):
    data_path = spark_tmp_path + "/DELTA_DATA"
    src_table = spark_tmp_table_factory.get()

    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, partition_columns)
        src_table_func(spark).createOrReplaceTempView(src_table)

    def do_merge(spark, path):
        dest_table = spark_tmp_table_factory.get()
        read_delta_path(spark, path).createOrReplaceTempView(dest_table)
        return spark.sql(merge_sql.format(src_table=src_table, dest_table=dest_table)).collect()
    with_cpu_session(setup_tables)
    check_func(data_path, do_merge)


def assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql,
                                   compare_logs, partition_columns=None, conf=None):
    assert conf is not None, "conf must be set"
        
    def read_data(spark, path):
        read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
        df = read_func(spark, path)
        return df.sort(df.columns)

    def checker(data_path, do_merge):
        cpu_path = data_path + "/CPU"
        gpu_path = data_path + "/GPU"
        # compare resulting dataframe from the merge operation (some older Spark versions return empty here)
        cpu_result = with_cpu_session(lambda spark: do_merge(spark, cpu_path), conf=conf)
        gpu_result = with_gpu_session(lambda spark: do_merge(spark, gpu_path), conf=conf)
        assert_equal(cpu_result, gpu_result)
        # compare merged table data results, read both via CPU to make sure GPU write can be read by CPU
        cpu_result = with_cpu_session(lambda spark: read_data(spark, cpu_path).collect(), conf=conf)
        gpu_result = with_cpu_session(lambda spark: read_data(spark, gpu_path).collect(), conf=conf)
        assert_equal(cpu_result, gpu_result)
        # Using partition columns involves sorting, and there's no guarantees on the task
        # partitioning due to random sampling.
        if compare_logs and not partition_columns:
            with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                         src_table_func, dest_table_func, merge_sql, checker, partition_columns)


def do_test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                              use_cdf, partition_columns, num_slices, compare_logs,
                                              conf):
    src_range, dest_range = table_ranges
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), src_range), num_slices)
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), dest_range), num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN NOT MATCHED THEN INSERT *"
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   partition_columns, conf=conf)


def do_test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                          use_cdf, partition_columns, num_slices, compare_logs,
                                          conf):
    src_range, dest_range = table_ranges
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), src_range), num_slices)
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), dest_range), num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED THEN DELETE"
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   partition_columns, conf=conf)


def do_test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                        num_slices, compare_logs, conf):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, seed=1, num_slices=num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   conf=conf)


def do_test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                              merge_sql, num_slices, compare_logs, conf):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, seed=1, num_slices=num_slices)
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   conf=conf)


def do_test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path,
                                                                spark_tmp_table_factory, use_cdf,
                                                                num_slices, compare_logs, conf):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, SetValuesGen(IntegerType(), range(100)), string_gen, seed=1, num_slices=num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED AND {dest_table}.a > 100 THEN UPDATE SET *"
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   conf=conf)


def do_test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                                conf):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: spark.range(10).withColumn("x", f.col("id") + 1) \
        .select(f.col("id"), (f.col("x") + 1).alias("x")) \
        .drop_duplicates(["id"]) \
        .limit(10)
    dest_table_func = lambda spark: spark.range(5).withColumn("x", f.col("id") + 1)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.id == {src_table}.id" \
                " WHEN MATCHED THEN UPDATE SET {dest_table}.x = {src_table}.x + 2" \
                " WHEN NOT MATCHED AND {src_table}.x < 7 THEN INSERT *"

    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql,
                                   compare_logs=False, conf=conf)
