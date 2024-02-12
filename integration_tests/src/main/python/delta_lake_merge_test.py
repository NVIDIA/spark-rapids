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

import pyspark.sql.functions as f
import pytest
import string

from asserts import *
from data_gen import *
from delta_lake_utils import *
from marks import *
from pyspark.sql.types import *
from spark_session import is_before_spark_320, is_databricks_runtime, spark_version

# Databricks changes the number of files being written, so we cannot compare logs
num_slices_to_test = [10] if is_databricks_runtime() else [1, 10]

delta_merge_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                           {"spark.rapids.sql.command.MergeIntoCommand": "true",
                                            "spark.rapids.sql.command.MergeIntoCommandEdge": "true"})

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
                                   compare_logs, partition_columns=None,
                                   conf=delta_merge_enabled_conf):
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

@allow_non_gpu(delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf",
                         [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                          {"spark.rapids.sql.format.parquet.enabled": "false"},
                          {"spark.rapids.sql.format.parquet.write.enabled": "false"},
                          {"spark.rapids.sql.command.MergeIntoCommand": "false"},
                          delta_writes_enabled_conf  # Test disabled by default
                         ], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_merge_disabled_fallback(spark_tmp_path, spark_tmp_table_factory, disable_conf):
    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path,
                                  delta_write_fallback_check, conf=disable_conf)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN NOT MATCHED THEN INSERT *"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False,
                         src_table_func=lambda spark: unary_op_df(spark, SetValuesGen(IntegerType(), range(100))),
                         dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                         merge_sql=merge_sql,
                         check_func=checker)

@allow_non_gpu("ExecutedCommandExec,BroadcastHashJoinExec,ColumnarToRowExec,BroadcastExchangeExec,DataWritingCommandExec", delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and spark_version() < "3.3.2", reason="NOT MATCHED BY SOURCE added in DBR 12.2")
@pytest.mark.skipif((not is_databricks_runtime()) and is_before_spark_340(), reason="NOT MATCHED BY SOURCE added in Delta Lake 2.4")
def test_delta_merge_not_matched_by_source_fallback(spark_tmp_path, spark_tmp_table_factory):
    def checker(data_path, do_merge):
        assert_gpu_fallback_write(do_merge, read_delta_path, data_path, "ExecutedCommandExec", conf = delta_merge_enabled_conf)
    merge_sql = "MERGE INTO {dest_table} " \
                "USING {src_table} " \
                "ON {src_table}.a == {dest_table}.a " \
                "WHEN MATCHED THEN " \
                "  UPDATE SET {dest_table}.b = {src_table}.b " \
                "WHEN NOT MATCHED THEN " \
                "  INSERT (a, b) VALUES ({src_table}.a, {src_table}.b) " \
                "WHEN NOT MATCHED BY SOURCE AND {dest_table}.b > 0 THEN " \
                "  UPDATE SET {dest_table}.b = 0"
    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory,
                         use_cdf=False,
                         src_table_func=lambda spark: binary_op_df(spark, SetValuesGen(IntegerType(), range(10))),
                         dest_table_func=lambda spark: binary_op_df(spark, SetValuesGen(IntegerType(), range(20, 30))),
                         merge_sql=merge_sql,
                         check_func=checker)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
@pytest.mark.parametrize("disable_conf", [
    "spark.rapids.sql.exec.RapidsProcessDeltaMergeJoinExec",
    "spark.rapids.sql.expression.Add"], ids=idfn)
def test_delta_merge_partial_fallback_via_conf(spark_tmp_path, spark_tmp_table_factory,
                                               use_cdf, partition_columns, num_slices, disable_conf):
    src_range, dest_range = range(20), range(10, 30)
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), src_range), num_slices) \
        .groupBy("a").agg(f.max("b").alias("b"),f.min("c").alias("c"))
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), dest_range), num_slices)
    merge_sql = "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
                " WHEN MATCHED THEN UPDATE SET d.a = s.a + 4 WHEN NOT MATCHED THEN INSERT *"
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    conf = copy_and_update(delta_merge_enabled_conf, { disable_conf: "false" })
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   partition_columns, conf=conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("table_ranges", [(range(20), range(10)),  # partial insert of source
                                          (range(5), range(5)),  # no-op insert
                                          (range(10), range(20, 30))  # full insert of source
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                           use_cdf, partition_columns, num_slices):
    src_range, dest_range = table_ranges
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), src_range), num_slices)
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), dest_range), num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN NOT MATCHED THEN INSERT *"
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   partition_columns)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("table_ranges", [(range(10), range(20)),  # partial delete of target
                                          (range(5), range(5)),  # full delete of target
                                          (range(10), range(20, 30))  # no-op delete
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                       use_cdf, partition_columns, num_slices):
    src_range, dest_range = table_ranges
    src_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), src_range), num_slices)
    dest_table_func = lambda spark: make_df(spark, SetValuesGen(IntegerType(), dest_range), num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED THEN DELETE"
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs,
                                   partition_columns)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, seed=1, num_slices=num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("merge_sql", [
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'q' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN NOT MATCHED THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN NOT MATCHED AND s.b > 'q' THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'a' AND s.b < 'g' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN MATCHED AND s.b > 'g' AND s.b < 'z' THEN UPDATE SET d.a = s.a / 4, d.b = concat('extra', s.b)" \
    " WHEN NOT MATCHED AND s.b > 'b' AND s.b < 'f' THEN INSERT *" \
    " WHEN NOT MATCHED AND s.b > 'f' AND s.b < 'z' THEN INSERT (b) VALUES ('not here')" ], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, merge_sql, num_slices):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, seed=1, num_slices=num_slices)
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, SetValuesGen(IntegerType(), range(100)), string_gen, seed=1, num_slices=num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED AND {dest_table}.a > 100 THEN UPDATE SET *"
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    compare_logs = num_slices == 1
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
def test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: spark.range(10).withColumn("x", f.col("id") + 1)\
        .select(f.col("id"), (f.col("x") + 1).alias("x"))\
        .drop_duplicates(["id"])\
        .limit(10)
    dest_table_func = lambda spark: spark.range(5).withColumn("x", f.col("id") + 1)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.id == {src_table}.id" \
                " WHEN MATCHED THEN UPDATE SET {dest_table}.x = {src_table}.x + 2" \
                " WHEN NOT MATCHED AND {src_table}.x < 7 THEN INSERT *"

    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf,
                                   src_table_func, dest_table_func, merge_sql, compare_logs=False)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.xfail(not is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/7573")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_dataframe_api(spark_tmp_path, use_cdf, num_slices):
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/DELTA_DATA"
    dest_table_func = lambda spark: two_col_df(spark, SetValuesGen(IntegerType(), [None] + list(range(100))), string_gen, seed=1, num_slices=num_slices)
    with_cpu_session(lambda spark: setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf))
    def do_merge(spark, path):
        # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
        src_df = two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
        dest_table = DeltaTable.forPath(spark, path)
        dest_table.alias("dest").merge(src_df.alias("src"), "dest.a == src.a") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
    assert_gpu_and_cpu_writes_are_equal_collect(do_merge, read_func, data_path, conf=delta_merge_enabled_conf)
    # Non-deterministic input for each task means we can only reliably compare record counts when using only one task
    if num_slices == 1:
        with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
