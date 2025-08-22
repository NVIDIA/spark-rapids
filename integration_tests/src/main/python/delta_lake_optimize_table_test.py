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

from asserts import assert_gpu_fallback_write
from data_gen import *
from delta_lake_utils import *
from marks import *
from spark_session import with_cpu_session, with_gpu_session, is_before_spark_353, \
    supports_delta_lake_deletion_vectors, is_databricks_runtime
from pyspark.sql.types import IntegerType, StringType


@delta_lake
@allow_non_gpu('ExecutedCommandExec', *delta_meta_allow)
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime(), reason="OPTIMIZE table command is not supported for Databricks")
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), reason="Deletion vectors aren't supported")
def test_delta_optimize_fallback_with_deletion_vectors(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_OPT_DV"

    def write_then_optimize(spark, path):
        # Create the table at the given path with DVs enabled, then run OPTIMIZE on it.
        # Use a fixed seed so CPU and GPU paths get identical data
        df = unary_op_df(spark, int_gen, seed=1234)
        writer = df.write.format("delta").mode("append")
        writer = writer.option("delta.enableDeletionVectors", "true")
        writer.save(path)
        spark.sql(f"OPTIMIZE delta.`{path}`")

    # Read back sorted to avoid nondeterministic row order differences
    assert_gpu_fallback_write(write_then_optimize, lambda s, p: _read_sorted(s, p), data_path,
                              "ExecutedCommandExec")


def _write_many_small_files(spark, enable_deletion_vectors, path, partition_columns=None):
    num_slices = 64 if partition_columns is None else 4
    df = three_col_df(
        spark,
        SetValuesGen(IntegerType(), range(1000)),
        SetValuesGen(StringType(), list("abcdefghij")),
        string_gen,
        num_slices=num_slices)
    writer = df.write.format("delta").mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(partition_columns)
    if supports_delta_lake_deletion_vectors():
        writer = writer.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
    writer.save(path)
    # count the number of files written to ensure we have many small files
    num_files = spark.read.format("delta").load(path).inputFiles()
    assert len(num_files) > 63, f"Expected more than 63 files, but got {num_files}"


def _read_sorted(spark, path):
    df = spark.read.format("delta").load(path)
    return df.sort(df.columns)


def _optimize_sql(path):
    return f"OPTIMIZE delta.`{path}`"


def _setup_tables(enable_deletion_vectors, cpu_path, gpu_path, partition_columns=None):
    def setup_cpu(spark):
        _write_many_small_files(spark, enable_deletion_vectors, cpu_path, partition_columns)
    def setup_gpu(spark):
        _write_many_small_files(spark, enable_deletion_vectors, gpu_path, partition_columns)
    with_cpu_session(setup_cpu)
    with_cpu_session(setup_gpu)


def _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=None):
    data_path = spark_tmp_path + "/DELTA_OPTIMIZE"
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"

    _setup_tables(enable_deletion_vectors, cpu_path, gpu_path, partition_columns)

    # Run OPTIMIZE on each table and verify the returned path matches the target
    cpu_result = with_cpu_session(lambda s: s.sql(_optimize_sql(cpu_path)).collect())
    gpu_result = with_gpu_session(lambda s: s.sql(_optimize_sql(gpu_path)).collect())
    # Validate the returned path for each run; metrics object is not stable across JVMs
    # Compare only the suffix to avoid scheme differences like file: vs absolute path
    assert str(cpu_result[0][0]).rstrip('/').endswith('/CPU')
    assert str(gpu_result[0][0]).rstrip('/').endswith('/GPU')

    # Compare table data after optimize (read via CPU)
    cpu_data = with_cpu_session(lambda s: _read_sorted(s, cpu_path).collect())
    gpu_data = with_cpu_session(lambda s: _read_sorted(s, gpu_path).collect())
    assert_equal(cpu_data, gpu_data)

    with_cpu_session(lambda s: assert_gpu_and_cpu_delta_logs_equivalent(s, data_path))


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime(), reason="OPTIMIZE table command is not supported for Databricks")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
    enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_optimize_unpartitioned_table(spark_tmp_path, enable_deletion_vectors):
    _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=None)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_353(), reason="OPTIMIZE table command is supported in Spark 3.5.3+")
@pytest.mark.skipif(is_databricks_runtime(), reason="OPTIMIZE table command is not supported for Databricks")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
    enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_optimize_partitioned_table(spark_tmp_path, enable_deletion_vectors):
    _assert_optimize_parity(enable_deletion_vectors, spark_tmp_path, partition_columns=["a"])


@delta_lake
@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "AtomicCreateTableAsSelectExec", "AppendDataExecV1")
@pytest.mark.skipif(is_before_spark_353(), reason="Liquid clustering requires Delta 3.3+")
@pytest.mark.skipif(is_databricks_runtime(), reason="OPTIMIZE table command is not supported for Databricks")
def test_delta_optimize_fallback_on_clustered_table(spark_tmp_path, spark_tmp_table_factory):
    view_name = spark_tmp_table_factory.get()
    data_path = spark_tmp_path + "/DELTA_OPT_CLUSTERED"

    def write_base_data(spark):
        df = unary_op_df(spark, int_gen).coalesce(1)
        df.createOrReplaceTempView(view_name)

    def write_clustered_then_optimize(spark, path):
        spark.sql(f"""
            CREATE TABLE delta.`{path}`
            USING DELTA
            CLUSTER BY (a)
            AS SELECT * FROM {view_name}
        """)
        spark.sql(_optimize_sql(path))

    # Enable OptimizeTableCommand explicitly to make sure that the fallback will happen
    # when the command is enabled.
    conf = copy_and_update(delta_writes_enabled_conf, {
        "spark.rapids.sql.command.OptimizeTableCommand": "true",
    })

    with_cpu_session(write_base_data, conf=conf)

    assert_gpu_fallback_write(
        write_clustered_then_optimize,
        lambda spark, path: _read_sorted(spark, path),
        data_path,
        "ExecutedCommandExec",
        conf=conf
    )
