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
from typing import Callable, Dict
from pyspark.sql.types import StringType, IntegerType

from asserts import assert_gpu_fallback_write, assert_gpu_and_cpu_writes_are_equal_collect
from conftest import is_databricks_runtime
from data_gen import unary_op_df, int_gen, copy_and_update, SetValuesGen, string_gen, long_gen, \
    gen_df
from delta_lake_delete_test import delta_delete_enabled_conf
from delta_lake_merge_test import delta_merge_enabled_conf
from delta_lake_update_test import delta_update_enabled_conf
from delta_lake_utils import delta_meta_allow, \
    delta_writes_enabled_conf, delta_write_fallback_allow, assert_gpu_and_cpu_delta_logs_equivalent
from marks import allow_non_gpu, delta_lake, ignore_order
from spark_session import is_databricks133_or_later, is_spark_353_or_later, is_spark_356_or_later, with_cpu_session


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "AtomicCreateTableAsSelectExec", "AppendDataExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="CTAS with cluster by is only supported on delta 3.3+")
def test_delta_ctas_sql_liquid_clustering_fallback(spark_tmp_path, spark_tmp_table_factory):
    view_name = spark_tmp_table_factory.get()
    """
    Test to ensure that creating a Delta table with liquid clustering (CLUSTER BY)
    falls back to the CPU, as this feature is not supported on the GPU.
    """
    def write_func(spark, path):
        # Create a temp view to select from for the CTAS operation
        unary_op_df(spark, int_gen).coalesce(1).createOrReplaceTempView(view_name)
        spark.sql(f"""
            CREATE TABLE delta.`{path}`
            USING DELTA
            CLUSTER BY (a)
            AS SELECT * FROM {view_name}
        """)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "AtomicCreateTableAsSelectExec",
        conf=delta_writes_enabled_conf)



def create_clustered_table_sql(spark, table_name, path):
    spark.sql(f"""
            CREATE TABLE {table_name}
            (a long, 
             b string, 
             c string, 
             d int, 
             e long)
            USING DELTA
            LOCATION '{path}'
            CLUSTER BY (a, b, d)
        """)


def gen_df_and_replace_view(spark, view_name):
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    df = gen_df(spark, gen_list)
    df.coalesce(1).createOrReplaceTempView(view_name)


def setup_clustered_table_sql(spark, path, table_name, view_name):
    create_clustered_table_sql(spark, table_name, path)
    gen_df_and_replace_view(spark, view_name)
    spark.sql(f"""
            INSERT INTO {table_name}
            SELECT * FROM {view_name}
        """)




@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec",
               "AtomicReplaceTableAsSelectExec",
               "AppendDataExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="RTAS with cluster by is only supported on delta 3.3+")
@pytest.mark.skipif(is_spark_356_or_later(),
                    reason="https://github.com/delta-io/delta/issues/4671")
def test_delta_rtas_sql_liquid_clustering_fallback(spark_tmp_path, spark_tmp_table_factory):
    """
    Test to ensure that creating a Delta table with liquid clustering (CLUSTER BY)
    falls back to the CPU, as this feature is not supported on the GPU.
    """
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()

        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            REPLACE TABLE {table_name}
            USING DELTA
            LOCATION '{path}'
            CLUSTER BY (a)
            AS SELECT * FROM {view_name}
        """)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "AtomicReplaceTableAsSelectExec",
        conf=delta_writes_enabled_conf)



@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_append_sql_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):
    """
    Test to ensure that creating a Delta table with liquid clustering (CLUSTER BY)
    falls back to the CPU, as this feature is not supported on the GPU.
    """
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT INTO {table_name}
            SELECT * FROM {view_name}
        """)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf
    )
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec",
               "OverwriteByExpressionExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_static_sql_liquid_clustering_fallback(spark_tmp_path,
                                                               spark_tmp_table_factory):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT OVERWRITE TABLE {table_name}
            SELECT * FROM {view_name}
        """)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    conf = copy_and_update(delta_writes_enabled_conf,
                           {"spark.sql.sources.partitionOverwriteMode": "STATIC"})

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "OverwriteByExpressionExecV1",
        conf=conf)


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_dynamic_sql_liquid_clustering_fallback(spark_tmp_path,
                                                                      spark_tmp_table_factory):
    """
    Test to ensure that creating a Delta table with liquid clustering (CLUSTER BY)
    falls back to the CPU, as this feature is not supported on the GPU.
    """
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT OVERWRITE TABLE {table_name}
            SELECT * FROM {view_name}
        """)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    conf = copy_and_update(delta_writes_enabled_conf,
                           {"spark.sql.sources.partitionOverwriteMode": "DYNAMIC"})

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=conf)


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec",
               "OverwriteByExpressionExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_replace_where_sql_liquid_clustering_fallback(spark_tmp_path,
                                                                             spark_tmp_table_factory):

    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        view_name = spark_tmp_table_factory.get()
        create_clustered_table_sql(spark, table_name, path)
        gen_df_and_replace_view(spark, view_name)
        spark.sql(f"""
            INSERT INTO TABLE {table_name}
            REPLACE WHERE b = 'x'
            SELECT * FROM {view_name}
        """)

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "OverwriteByExpressionExecV1",
        conf=delta_writes_enabled_conf)


def do_test_delta_dml_sql_liquid_clustering_fallback(spark_tmp_path,
                                                     spark_tmp_table_factory,
                                                     conf: Dict[str, str],
                                                     sql_func: Callable[[str], str]):

    base_data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"
    cpu_data_path = f"{base_data_path}/CPU"
    cpu_table_name = spark_tmp_table_factory.get()
    gpu_data_path = f"{base_data_path}/GPU"
    gpu_table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, cpu_data_path,
                                                             cpu_table_name,
                                                             spark_tmp_table_factory.get()))
    with_cpu_session(lambda spark: setup_clustered_table_sql(spark, gpu_data_path,
                                                             gpu_table_name,
                                                             spark_tmp_table_factory.get()))

    def modify_table(spark, path):
        table_name = cpu_table_name if path == cpu_data_path else gpu_table_name
        spark.sql(sql_func(table_name))

    assert_gpu_fallback_write(modify_table,
                              lambda spark, path: spark.read.format("delta").load(path),
                              base_data_path,
                              "ExecutedCommandExec",
                              conf=conf)


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec",
               "AppendDataExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_delete_sql_liquid_clustering_fallback(spark_tmp_path,
                                                     spark_tmp_table_factory):

    do_test_delta_dml_sql_liquid_clustering_fallback(
        spark_tmp_path, spark_tmp_table_factory, delta_delete_enabled_conf,
        lambda table_name: f"DELETE FROM {table_name} WHERE a > 0")

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec",
               "AppendDataExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_update_sql_liquid_clustering_fallback(spark_tmp_path,
                                                     spark_tmp_table_factory):

    do_test_delta_dml_sql_liquid_clustering_fallback(
        spark_tmp_path, spark_tmp_table_factory, delta_update_enabled_conf,
        lambda table_name: f"UPDATE {table_name} SET e = e+1 WHERE a > 0")

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec",
               "AppendDataExecV1")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_merge_sql_liquid_clustering_fallback(spark_tmp_path,
                                                     spark_tmp_table_factory):

    do_test_delta_dml_sql_liquid_clustering_fallback(
        spark_tmp_path, spark_tmp_table_factory, delta_merge_enabled_conf,
        lambda table_name: f"MERGE INTO {table_name} "
                           f"USING {table_name} as src_table "
                           f"ON {table_name}.a == src_table.a "
                           f"WHEN NOT MATCHED THEN INSERT *")



def create_clustered_delta_table_df(table_name, table_path):
    from delta.tables import DeltaTable
    (DeltaTable.create()
     .tableName(table_name)
     .addColumn("a", dataType="INT")
     .addColumn("b", dataType="STRING")
     .addColumn("c", dataType="STRING")
     .addColumn("d", dataType="INT")
     .addColumn("e", dataType="LONG")
     .clusterBy("a", "b", "d")
     .location(table_path)
     .execute())

def write_to_delta_table_df(spark, path, mode, opts= None):
    if opts is None:
        opts = {}
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    df_writer = (gen_df(spark, gen_list)
                 .coalesce(1)
                 .write
                 .format("delta")
                 .mode(mode))

    for k, v in opts.items():
        df_writer = df_writer.option(k, v)

    df_writer.save(path)


@allow_non_gpu(*delta_meta_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_append_df_liquid_clustering(spark_tmp_path, spark_tmp_table_factory):
    """
    Test to ensure that creating a Delta table with liquid clustering (CLUSTER BY)
    falls back to the CPU, as this feature is not supported on the GPU.
    """
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        create_clustered_delta_table_df(table_name, path)
        # Create a temp view to select from for the CTAS operation
        write_to_delta_table_df(spark, path, "append")

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("overwrite_mode", ["STATIC", "DYNAMIC"],
                         ids = lambda val: f"overwrite_mode={val}")
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_df_liquid_clustering_fallback(spark_tmp_path,
                                                              spark_tmp_table_factory,
                                                              overwrite_mode):
    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        create_clustered_delta_table_df(table_name, path)
        write_to_delta_table_df(spark, path, "overwrite",
                                opts = {'partitionOverwriteMode': overwrite_mode})

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=delta_writes_enabled_conf)


@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow, "CreateTableExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Delta Lake liquid clustering is only supported on Databricks 13.3+")
@pytest.mark.skipif(not is_spark_353_or_later(),
                    reason="Create table with cluster by is only supported on delta 3.1+")
def test_delta_insert_overwrite_replace_where_df_liquid_clustering_fallback(
        spark_tmp_path, spark_tmp_table_factory):

    def write_func(spark, path):
        table_name = spark_tmp_table_factory.get()
        create_clustered_delta_table_df(table_name, path)
        write_to_delta_table_df(spark, path, "overwrite",
                                opts = {'replaceWhere': " b='x' "})

    data_path = spark_tmp_path + "/DELTA_LIQUID_CLUSTER"

    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=delta_writes_enabled_conf)