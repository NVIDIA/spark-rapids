# Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import json
import os
import pyspark.sql.functions as f
import pytest

from asserts import *
from data_gen import *
from conftest import is_databricks_runtime
from delta_lake_utils import *
from marks import *
from parquet_write_test import parquet_write_gens_list, writer_confs
from pyspark.sql.types import *
from spark_session import *

delta_write_gens = [x for sublist in parquet_write_gens_list for x in sublist]

delta_part_write_gens = [
    byte_gen,
    short_gen,
    int_gen,
    long_gen,
    # Some file systems have issues with UTF8 strings so to help the test pass even there
    StringGen('(\\w| ){0,50}'),
    boolean_gen,
    date_gen,
    timestamp_gen
]

_delta_confs = copy_and_update(writer_confs, delta_writes_enabled_conf,
                               {"spark.rapids.sql.hasExtendedYearValues": "false",
                                "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
                                "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED"})

def get_writer_with_deletion_vector_property_set(writer, enable_deletion_vectors):
    if supports_delta_lake_deletion_vectors():
        return writer.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
    return writer

def get_last_operation_metrics(path):
    from delta.tables import DeltaTable
    return with_cpu_session(lambda spark: DeltaTable.forPath(spark, path)\
                            .history(1)\
                            .selectExpr("operationMetrics")\
                            .head()[0])

def _create_table(spark, path, schema, partitioned_by=None, enable_deletion_vectors=False):
    q = f"CREATE TABLE delta.`{path}` ({schema}) USING DELTA"
    if partitioned_by:
        q += f" PARTITIONED BY ({partitioned_by})"

    if supports_delta_lake_deletion_vectors():
        q += " TBLPROPERTIES ('delta.enableDeletionVectors' = {})".format(str(enable_deletion_vectors).lower())
    spark.sql(q)

def _create_cpu_gpu_tables(spark, path, schema, partitioned_by=None, enable_deletion_vectors=False):
    _create_table(spark, path + "/CPU", schema, partitioned_by, enable_deletion_vectors)
    _create_table(spark, path + "/GPU", schema, partitioned_by, enable_deletion_vectors)

def _assert_sql(data_path, confs, query):
    def do_sql(spark, q): spark.sql(q)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: do_sql(spark, query.format(path=path)),
        read_delta_path,
        data_path,
        confs)


@allow_non_gpu(delta_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf",
                         [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                          {"spark.rapids.sql.format.parquet.enabled": "false"},
                          {"spark.rapids.sql.format.parquet.write.enabled": "false"}], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_write_disabled_fallback(spark_tmp_path, disable_conf, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            unary_op_df(spark, int_gen).coalesce(1).write.format("delta"), enable_deletion_vectors).save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        delta_write_fallback_check,
        conf=copy_and_update(writer_confs, disable_conf))


@allow_non_gpu("AppendDataExecV1", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
    enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12027"), ids=idfn)
def test_delta_write_round_trip_managed(spark_tmp_table_factory, enable_deletion_vectors):
    gen_list = [("c" + str(i), gen) for i, gen in enumerate(delta_write_gens)]
    conf = copy_and_update(writer_confs, delta_writes_enabled_conf)
    (cpu_table, gpu_table) = assert_gpu_and_cpu_save_as_table_are_equal_collect(
        spark_tmp_table_factory,
        lambda spark, table: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors)
            .saveAsTable(table),
        conf=conf
    )
    assert_delta_history_equal(conf, cpu_table, gpu_table)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12027"), ids=idfn)
def test_delta_write_round_trip_unmanaged(spark_tmp_path, enable_deletion_vectors):
    gen_list = [("c" + str(i), gen) for i, gen in enumerate(delta_write_gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors).save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("gens", delta_part_write_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_part_write_round_trip_unmanaged(spark_tmp_path, gens, enable_deletion_vectors):
    gen_list = [("a", RepeatSeqGen(gens, 10)), ("b", gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors)
            .partitionBy("a")
            .save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    # Avoid checking delta log equivalence here. Using partition columns involves sorting, and
    # there's no guarantees on the task partitioning due to random sampling.

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("gens", delta_part_write_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_multi_part_write_round_trip_unmanaged(spark_tmp_path, gens, enable_deletion_vectors):
    gen_list = [("a", RepeatSeqGen(gens, 10)), ("b", gens), ("c", SetValuesGen(StringType(), ["x", "y", "z"]))]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors)
            .partitionBy("a", "c")
            .save(path),
        lambda spark, path: spark.read.format("delta").load(path).filter("c='x'"),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    # Avoid checking delta log equivalence here. Using partition columns involves sorting, and
    # there's no guarantees on the task partitioning due to random sampling.

def do_update_round_trip_managed(spark_tmp_path, mode, enable_deletion_vectors):
    gen_list = [("x", int_gen), ("y", binary_gen), ("z", string_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
        gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors).save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
        gen_df(spark, gen_list).coalesce(1).write.mode(mode).format("delta"), enable_deletion_vectors).save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)

    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    # Verify time travel still works
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("delta").option("versionAsOf", "0").load(data_path + "/GPU"),
        conf=confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12027"), ids=idfn)
def test_delta_overwrite_round_trip_unmanaged(spark_tmp_path, enable_deletion_vectors):
    do_update_round_trip_managed(spark_tmp_path, "overwrite", enable_deletion_vectors)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12027"), ids=idfn)
def test_delta_append_round_trip_unmanaged(spark_tmp_path, enable_deletion_vectors):
    do_update_round_trip_managed(spark_tmp_path, "append", enable_deletion_vectors)

def _atomic_write_table_as_select(gens, spark_tmp_table_factory, spark_tmp_path, overwrite, enable_deletion_vectors):
    gen_list = [("c" + str(i), gen) for i, gen in enumerate(gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    path_to_table = {}
    def do_write(spark, path):
        table = spark_tmp_table_factory.get()
        path_to_table[path] = table
        writer = get_writer_with_deletion_vector_property_set(gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors)
        if overwrite:
            writer = writer.mode("overwrite")
        writer.saveAsTable(table)
    assert_gpu_and_cpu_writes_are_equal_collect(
        do_write,
        lambda spark, path: spark.read.format("delta").table(path_to_table[path]),
        data_path,
        conf=confs)

@allow_non_gpu('DataWritingCommandExec', 'WriteFilesExec', *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12041"), ids=idfn)
def test_delta_atomic_create_table_as_select(spark_tmp_table_factory, spark_tmp_path, enable_deletion_vectors):
    _atomic_write_table_as_select(delta_write_gens, spark_tmp_table_factory, spark_tmp_path,
                                  overwrite=False,
                                  enable_deletion_vectors=enable_deletion_vectors)

@allow_non_gpu('DataWritingCommandExec', 'WriteFilesExec', *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12041"), ids=idfn)
@pytest.mark.xfail(is_spark_356(), reason="https://github.com/delta-io/delta/issues/4671")
def test_delta_atomic_replace_table_as_select(spark_tmp_table_factory, spark_tmp_path, enable_deletion_vectors):
    _atomic_write_table_as_select(delta_write_gens, spark_tmp_table_factory, spark_tmp_path,
                                  overwrite=True, enable_deletion_vectors=enable_deletion_vectors)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_append_data_exec_v1(spark_tmp_path, use_cdf, enable_deletion_vectors):
    gen_list = [("c" + str(i), gen) for i, gen in enumerate(delta_write_gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                lambda spark: gen_df(spark, gen_list).coalesce(1), use_cdf, enable_deletion_vectors)
    with_cpu_session(setup_tables, writer_confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1)\
            .write.format("delta").mode("append"), enable_deletion_vectors).saveAsTable(f"delta.`{path}`"),
        read_delta_path,
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_overwrite_by_expression_exec_v1(spark_tmp_table_factory, spark_tmp_path, use_cdf, enable_deletion_vectors):
    gen_list = [("c" + str(i), gen) for i, gen in enumerate(delta_write_gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    src_path = spark_tmp_path + "/PARQUET_DATA"
    src_table = spark_tmp_table_factory.get()
    def setup_src_table(spark):
        df = gen_df(spark, gen_list).coalesce(1)
        df.write.parquet(src_path)
        spark.read.parquet(src_path).createOrReplaceTempView(src_table)
    with_cpu_session(setup_src_table, conf=writer_confs)
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                lambda spark: spark.read.parquet(src_path).limit(1), use_cdf, enable_deletion_vectors)
    with_cpu_session(setup_tables, writer_confs)
    def overwrite_table(spark, path):
        spark.sql(f"INSERT OVERWRITE delta.`{path}` SELECT * FROM {src_table}")
    assert_gpu_and_cpu_writes_are_equal_collect(
        overwrite_table,
        read_delta_path,
        data_path,
        conf=_delta_confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_overwrite_dynamic_by_name(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    schema = "id bigint, data string, data2 string"
    with_cpu_session(lambda spark: _create_cpu_gpu_tables(spark, data_path, schema), conf=writer_confs)
    confs = _delta_confs
    _assert_sql(data_path, confs, "INSERT OVERWRITE delta.`{path}`(id, data, data2) VALUES(1L, 'a', 'b')")
    _assert_sql(data_path, confs, "INSERT OVERWRITE delta.`{path}`(data, data2, id) VALUES('b', 'd', 2L)")
    _assert_sql(data_path, confs, "INSERT OVERWRITE delta.`{path}`(data, data2, id) VALUES('c', 'e', 1)")
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_340() and not is_databricks_runtime(), reason="Schema evolution fixed in later releases")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_overwrite_schema_evolution_arrays(spark_tmp_path, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_DATA"
    src_path = data_path + "/SRC"
    def setup_tables(spark):
        src_schema = "id INT, col2 STRING, " +\
                     "col ARRAY<STRUCT<f1: INT, f2: STRUCT<f21: STRING, f22: DATE>, f3: STRUCT<f31: STRING>>>"
        dst_schema = "id INT, col2 DATE, col ARRAY<STRUCT<f1: INT, f2: STRUCT<f21: STRING>>>"
        _create_table(spark, src_path, src_schema, enable_deletion_vectors=enable_deletion_vectors)
        spark.sql(f"INSERT INTO delta.`{src_path}` VALUES (1, '2022-11-01', " +
                  "array(struct(1, struct('s1', DATE'2022-11-01'), struct('s1'))))")
        _create_cpu_gpu_tables(spark, data_path, dst_schema, enable_deletion_vectors=enable_deletion_vectors)
    with_cpu_session(setup_tables, conf=writer_confs)
    confs = copy_and_update(_delta_confs, {"spark.databricks.delta.schema.autoMerge.enabled": "true"})
    _assert_sql(data_path, confs,
                "INSERT INTO delta.`{path}` VALUES(2, DATE'2022-11-02', array(struct(2, struct('s2'))))")
    _assert_sql(data_path, confs, "INSERT OVERWRITE delta.`{path}` " +
                f"SELECT * FROM delta.`{src_path}`")
    _assert_sql(data_path, confs, "INSERT INTO delta.`{path}` VALUES(2, DATE'2022-11-02'," +
               "array(struct(2, struct('s2', DATE'2022-11-02'), struct('s2'))))")
    _assert_sql(data_path, confs, "INSERT INTO delta.`{path}` VALUES (3, DATE'2022-11-03', " +
               "array(struct(3, struct('s3', NULL), struct(NULL))))")
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("mode", [
    "STATIC",
    pytest.param("DYNAMIC", marks=pytest.mark.xfail(is_databricks_runtime(),
                                                    reason="https://github.com/NVIDIA/spark-rapids/issues/9543"))
], ids=idfn)
@pytest.mark.parametrize("clause", ["", "PARTITION (id)"], ids=idfn)
def test_delta_overwrite_dynamic_missing_clauses(spark_tmp_table_factory, spark_tmp_path, mode, clause):
    data_path = spark_tmp_path + "/DELTA_DATA"
    view = spark_tmp_table_factory.get()
    confs = copy_and_update(_delta_confs,
                            {"spark.sql.sources.partitionOverwriteMode" : mode})
    def setup(spark):
        _create_cpu_gpu_tables(spark, data_path, "id bigint, data string", "id")
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ("id", "data")).createOrReplaceTempView(view)
    with_cpu_session(setup, conf=writer_confs)
    _assert_sql(data_path, confs, "INSERT INTO delta.`{path}` VALUES (2L, 'dummy'), (4L, 'value')")
    _assert_sql(data_path, confs, "INSERT OVERWRITE TABLE delta.`{path}` " +
                f"{clause} SELECT * FROM {view}")


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("mode", [
    "STATIC",
    pytest.param("DYNAMIC", marks=pytest.mark.xfail(is_databricks_runtime(),
                                                    reason="https://github.com/NVIDIA/spark-rapids/issues/9543"))
], ids=idfn)
@pytest.mark.parametrize("clause", ["PARTITION (id, p = 2)", "PARTITION (p = 2, id)", "PARTITION (p = 2)"])
def test_delta_overwrite_mixed_clause(spark_tmp_table_factory, spark_tmp_path, mode, clause):
    data_path = spark_tmp_path + "/DELTA_DATA"
    view = spark_tmp_table_factory.get()
    confs = copy_and_update(_delta_confs,
                            {"spark.sql.sources.partitionOverwriteMode" : mode})
    def setup(spark):
        _create_cpu_gpu_tables(spark, data_path, "id bigint, data string, p int", "id, p")
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ("id", "data")).createOrReplaceTempView(view)
    with_cpu_session(setup, conf=writer_confs)
    _assert_sql(data_path, confs, "INSERT INTO delta.`{path}` VALUES (2L, 'dummy', 23), (4L, 'value', 2)")
    _assert_sql(data_path, confs, "INSERT OVERWRITE TABLE delta.`{path}` " +
                f"{clause} SELECT * FROM {view}")
    # Avoid checking delta log equivalence here. Using partition columns involves sorting, and
    # there's no guarantees on the task partitioning due to random sampling.

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(is_databricks_runtime() and is_before_spark_330(),
                    reason="Databricks 10.4 does not properly handle options passed during DataFrame API write")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12027"), ids=idfn)
def test_delta_write_round_trip_cdf_write_opt(spark_tmp_path, enable_deletion_vectors):
    gen_list = [("ints", int_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    # drop the _commit_timestamp column when comparing since it will always be different
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors)
            .option("delta.enableChangeDataFeed", "true")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: get_writer_with_deletion_vector_property_set(
            gen_df(spark, gen_list).coalesce(1).write.format("delta"), enable_deletion_vectors)
            .mode("overwrite")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)

    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_round_trip_cdf_table_prop(spark_tmp_path):
    gen_list = [("ints", int_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    def setup_tables(spark):
        for name in ["CPU", "GPU"]:
            spark.sql("CREATE TABLE delta.`{}/{}` (ints INT) ".format(data_path, name) +
                      "USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    with_cpu_session(setup_tables)
    # drop the _commit_timestamp column when comparing since it will always be different
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .mode("append")
            .option("delta.enableChangeDataFeed", "true")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .mode("overwrite")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("ts_write", ["INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_legacy_timestamp(spark_tmp_path, ts_write):
    gen = TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc),
                       end=datetime(2000, 1, 1, tzinfo=timezone.utc)).with_special_case(
        datetime(1000, 1, 1, tzinfo=timezone.utc), weight=10.0)
    data_path = spark_tmp_path + "/DELTA_DATA"
    all_confs = copy_and_update(delta_writes_enabled_conf, {
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",
        "spark.sql.legacy.parquet.outputTimestampType": ts_write
    })
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=all_confs)

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_encryption_option_fallback(spark_tmp_path, write_options):
    def write_func(spark, path):
        writer = unary_op_df(spark, int_gen).coalesce(1).write.format("delta")
        for key, value in write_options.items():
            writer.option(key , value)
        writer.save(path)
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        delta_write_fallback_check,
        conf=delta_writes_enabled_conf)

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_encryption_runtimeconfig_fallback(spark_tmp_path, write_options):
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        delta_write_fallback_check,
        conf=copy_and_update(write_options, delta_writes_enabled_conf))

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_encryption_hadoopconfig_fallback(spark_tmp_path, write_options):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_hadoop_confs(spark):
        for k, v in write_options.items():
            spark.sparkContext._jsc.hadoopConfiguration().set(k, v)
    def reset_hadoop_confs(spark):
        for k in write_options.keys():
            spark.sparkContext._jsc.hadoopConfiguration().unset(k)
    try:
        with_cpu_session(setup_hadoop_confs)
        assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            delta_write_fallback_check,
            conf=delta_writes_enabled_conf)
    finally:
        with_cpu_session(reset_hadoop_confs)

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize('codec', ['gzip'])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_compression_fallback(spark_tmp_path, codec):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {"spark.sql.parquet.compression.codec": codec})
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        delta_write_fallback_check,
        conf=confs)

@allow_non_gpu(*delta_meta_allow, delta_write_fallback_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_legacy_format_fallback(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {"spark.sql.parquet.writeLegacyFormat": "true"})
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        delta_write_fallback_check,
        conf=confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_append_only(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    gen = int_gen
    # setup initial table
    with_gpu_session(lambda spark: unary_op_df(spark, gen).coalesce(1).write.format("delta")
                     .option("delta.appendOnly", "true")
                     .save(data_path),
                     conf=delta_writes_enabled_conf)
    # verify overwrite fails
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: unary_op_df(spark, gen).write.format("delta").mode("overwrite").save(data_path),
            conf=delta_writes_enabled_conf),
        "This table is configured to only allow appends")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_constraint_not_null(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    not_null_gen = StringGen(nullable=False)
    null_gen = SetValuesGen(StringType(), [None])

    # create table with not null constraint
    def setup_table(spark):
        spark.sql("CREATE TABLE delta.`{}` (a string NOT NULL) USING DELTA".format(data_path))

    with_cpu_session(setup_table)

    # verify write of non-null values does not throw
    with_gpu_session(lambda spark: unary_op_df(spark, not_null_gen).write.format("delta").mode("append").save(data_path),
                     conf=delta_writes_enabled_conf)

    # verify write of null value throws
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: unary_op_df(spark, null_gen).write.format("delta").mode("append").save(data_path),
            conf=delta_writes_enabled_conf),
        "NOT NULL constraint violated for column: a")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_constraint_check(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"

    # create table with check constraint
    def setup_table(spark):
        spark.sql("CREATE TABLE delta.`{}` (id long, x long) USING DELTA".format(data_path))
        spark.sql("ALTER TABLE delta.`{}` ADD CONSTRAINT customcheck CHECK (id < x)".format(data_path))

    with_cpu_session(setup_table)

    # verify write of dataframe that passes constraint check does not fail
    def gen_good_data(spark):
        return spark.range(1024).withColumn("x", f.col("id") + 1)

    with_gpu_session(lambda spark: gen_good_data(spark).write.format("delta").mode("append").save(data_path),
                     conf=delta_writes_enabled_conf)

    # verify write of values that violate the constraint throws
    def gen_bad_data(spark):
        return gen_good_data(spark).union(spark.range(1).withColumn("x", f.col("id")))

    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: gen_bad_data(spark).write.format("delta").mode("append").save(data_path),
            conf=delta_writes_enabled_conf),
        "CHECK constraint customcheck (id < x) violated")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_constraint_check_fallback(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    # create table with check constraint
    def setup_table(spark):
        spark.sql("CREATE TABLE delta.`{}` (id long, x long) USING DELTA".format(data_path))
        spark.sql("ALTER TABLE delta.`{}` ADD CONSTRAINT mycheck CHECK (id + x < 1000)".format(data_path))
    with_cpu_session(setup_table)
    # create a conf that will force constraint check to fallback to CPU
    add_disable_conf = copy_and_update(delta_writes_enabled_conf, {"spark.rapids.sql.expression.Add": "false"})
    # verify write of dataframe that passes constraint check does not fail
    def gen_good_data(spark):
        return spark.range(100).withColumn("x", f.col("id") + 1)
    # TODO: Find a way to capture plan with DeltaInvariantCheckerExec
    with_gpu_session(lambda spark: gen_good_data(spark).write.format("delta").mode("append").save(data_path),
                     conf=add_disable_conf)
    # verify write of values that violate the constraint throws
    def gen_bad_data(spark):
        return spark.range(1000).withColumn("x", f.col("id") + 1)
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: gen_bad_data(spark).write.format("delta").mode("append").save(data_path),
            conf=add_disable_conf),
        "CHECK constraint mycheck ((id + x) < 1000) violated",)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_stat_column_limits(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(delta_writes_enabled_conf, {"spark.databricks.io.skipping.stringPrefixLength": 8})
    # maximum unicode codepoint and maximum ascii character
    umax, amax = chr(1114111), chr(0x7f)
    expected_min = {"a": "abcdefgh", "b": "abcdefg�", "c": "abcdefgh",
                    "d": "abcdefgh", "e": umax * 4, "f": umax * 4}
    # no max expected for column f since it cannot be truncated to 8 characters and remain
    # larger than the original value
    expected_max = {"a": "bcdefghi", "b": "bcdefgh�", "c": "bcdefghi" + amax,
                    "d": "bcdefghi" + umax, "e": umax * 8}
    def write_table(spark, path):
        df = spark.createDataFrame([
            ("bcdefghi", "abcdefg�", "bcdefghijk", "abcdefgh�", umax * 4, umax * 9),
            ("abcdefgh", "bcdefgh�", "abcdefghij", "bcdefghi�", umax * 8, umax * 9)],
            "a string, b string, c string, d string, e string, f string")
        df.repartition(1).write.format("delta").save(path)
    def verify_stat_limits(spark):
        log_data = read_delta_logs(spark, data_path + "/GPU/_delta_log/*.json")
        assert len(log_data) == 1, "GPU should generate exactly one Delta log"
        json_objs = list(log_data.values())[0]
        json_adds = [x["add"] for x in json_objs if "add" in x]
        assert len(json_adds) == 1, "GPU should only generate a single add in Delta log"
        stats = json.loads(json_adds[0]["stats"])
        actual_min = stats["minValues"]
        assert expected_min == actual_min, \
            f"minValues mismatch, expected: {expected_min} actual: {actual_min}"
        actual_max = stats["maxValues"]
        assert expected_max == actual_max, \
                f"maxValues stats mismatch, expected: {expected_max} actual: {actual_max}"
    assert_gpu_and_cpu_writes_are_equal_collect(
        write_table,
        read_delta_path,
        data_path,
        conf=confs)
    # Many Delta Lake versions are missing the fix from https://github.com/delta-io/delta/pull/3430
    # so instead of a full delta log compare with the CPU, focus on the reported statistics on GPU.
    with_cpu_session(verify_stat_limits)

@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_generated_columns(spark_tmp_table_factory, spark_tmp_path):
    from delta.tables import DeltaTable
    def write_data(spark, path):
        DeltaTable.create(spark) \
            .tableName(spark_tmp_table_factory.get()) \
            .location(path) \
            .addColumn("id", "LONG", comment="IDs") \
            .addColumn("x", "LONG", comment="some other column") \
            .addColumn("z", "STRING", comment="a generated column",
                       generatedAlwaysAs="CONCAT('sum(id,x)=', CAST((id + x) AS STRING))") \
            .execute()
        df = spark.range(2048).withColumn("x", f.col("id") * 2)
        df.write.format("delta").mode("append").save(path)

    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        write_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)

    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


def do_test_delta_write_identity_columns(data_path, create_data):
    assert_gpu_and_cpu_writes_are_equal_collect(
        create_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    def append_data(spark, path):
        spark.range(2048).selectExpr("id + 10 as x").write.format("delta").mode("append").save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        append_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime(), reason="IdentityGenerator is missing in Databricks python API")
@pytest.mark.skipif(not is_databricks_runtime() and is_before_spark_350(),
                    reason="Delta Lake identity columns for Dataframe are currently only supported on Apache Spark 3.5+")
def test_delta_write_identity_columns_df(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    # clean up redundant slashes in the path
    data_path = data_path.replace("//", "/")
    def create_data(spark, path):
        from delta.tables import DeltaTable, IdentityGenerator
        from pyspark.sql.types import LongType
        DeltaTable.create() \
            .location("file:{}".format(path)) \
            .addColumn("x", dataType=LongType()) \
            .addColumn("id", dataType=LongType(), generatedAlwaysAs=IdentityGenerator()) \
            .execute()
        spark.range(2048).selectExpr("id * id AS x").write.format("delta").mode("append").save(path)
    do_test_delta_write_identity_columns(data_path, create_data)


@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_runtime(),
                    reason="Delta Lake identity columns for SQL are currently only supported on Databricks")
def test_delta_write_identity_columns_sql(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def create_data(spark, path):
        spark.sql("CREATE TABLE delta.`{}` (x BIGINT, id BIGINT GENERATED ALWAYS AS IDENTITY) USING DELTA".format(path))
        spark.range(2048).selectExpr("id * id AS x").write.format("delta").mode("append").save(path)
    do_test_delta_write_identity_columns(data_path, create_data)


@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_databricks_runtime(), reason="IdentityGenerator is missing in Databricks python API")
@pytest.mark.skipif(not is_databricks_runtime() and is_before_spark_350(),
                    reason="Delta Lake identity columns for Dataframe are currently only supported on Apache Spark 3.5+")
def test_delta_write_multiple_identity_columns_df(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    # clean up redundant slashes in the path
    data_path = data_path.replace("//", "/")
    def create_data(spark, path):
        from delta.tables import DeltaTable, IdentityGenerator
        from pyspark.sql.types import LongType
        DeltaTable.create() \
            .location("file:{}".format(path)) \
            .addColumn("id1", dataType=LongType(), generatedAlwaysAs=IdentityGenerator()) \
            .addColumn("x", dataType=LongType()) \
            .addColumn("id2", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(start=100)) \
            .addColumn("id3", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(step=11)) \
            .addColumn("id4", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(start=-200,step=3)) \
            .addColumn("id5", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(start=12,step=-3)) \
            .execute()
        spark.range(2048).selectExpr("id * id AS x").write.format("delta").mode("append").save(path)
    do_test_delta_write_identity_columns(data_path, create_data)


@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_runtime(),
                    reason="Delta Lake identity columns for SQL are currently only supported on Databricks")
def test_delta_write_multiple_identity_columns_sql(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def create_data(spark, path):
        spark.sql("CREATE TABLE delta.`{}` (".format(path) +
                  "id1 BIGINT GENERATED ALWAYS AS IDENTITY, "
                  "x BIGINT, "
                  "id2 BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 100 ), "
                  "id3 BIGINT GENERATED ALWAYS AS IDENTITY ( INCREMENT BY 11 ), "
                  "id4 BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH -200 INCREMENT BY 3 ), "
                  "id5 BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 12 INCREMENT BY -3 )"
                  ") USING DELTA")
        spark.range(2048).selectExpr("id * id AS x").write.format("delta").mode("append").save(path)
    do_test_delta_write_identity_columns(data_path, create_data)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12027"), ids=idfn)
def test_delta_write_aqe_join(spark_tmp_path, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {"spark.sql.adaptive.enabled": "true"})
    def do_join(spark, path):
        df = unary_op_df(spark, int_gen)
        get_writer_with_deletion_vector_property_set(
        df.join(df, ["a"], "inner").write.format("delta"), enable_deletion_vectors).save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        do_join,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)

    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

def do_test_optimize_write(spark_tmp_path, aqe_enabled, do_write, num_chunks):
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.sql.adaptive.enabled" : str(aqe_enabled)
    })
    data_path = os.path.join(spark_tmp_path, "DELTA_DATA1")
    do_write(confs, data_path, is_optimize_write=False)
    opmetrics = get_last_operation_metrics(os.path.join(data_path, "GPU"))
    assert int(opmetrics["numFiles"]) == num_chunks
    data_path = os.path.join(spark_tmp_path, "DELTA_DATA2")
    do_write(confs, data_path, is_optimize_write=True)
    opmetrics = get_last_operation_metrics(os.path.join(data_path, "GPU"))
    assert int(opmetrics["numFiles"]) == 1

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(), reason="Delta Lake optimized writes are not supported before Spark 3.5.3 on Apache Spark")
@pytest.mark.parametrize("enable_conf_key", [
    "spark.databricks.delta.optimizeWrite.enabled",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite"], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", [True, False], ids=idfn)
def test_delta_write_optimized_sql_conf_aqe(spark_tmp_path, enable_conf_key, aqe_enabled):
    num_chunks = 20

    def do_write(conf, data_path, is_optimize_write):
        confs=copy_and_update(conf, {
            enable_conf_key : str(is_optimize_write)
        })
        assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, int_gen)
                .repartition(num_chunks).write.format("delta").save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            conf=confs)
    do_test_optimize_write(spark_tmp_path, aqe_enabled, do_write, num_chunks)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("confkey", ["optimizeWrite"], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", [True, False], ids=idfn)
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(), reason="Delta Lake optimized writes are not supported before Spark 3.5.3 on Apache Spark")
def test_delta_write_optimized_write_opts_aqe(spark_tmp_path, confkey, aqe_enabled):
    num_chunks = 20

    def do_write(conf, data_path, is_optimize_write):
        assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, int_gen)
                .repartition(num_chunks).write.format("delta")
                .option(confkey, str(is_optimize_write)).save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            conf=conf)
    do_test_optimize_write(spark_tmp_path, aqe_enabled, do_write, num_chunks)

@allow_non_gpu(*delta_meta_allow, )
@delta_lake
@ignore_order
@pytest.mark.parametrize("confkey", ["delta.autoOptimize.optimizeWrite"], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", [True, False], ids=idfn)
@pytest.mark.skipif(not is_databricks_runtime() and not is_spark_353_or_later(), reason="Delta Lake optimized writes are not supported before Spark 3.5.3 on Apache Spark")
def test_delta_write_optimized_table_props_aqe(spark_tmp_path, confkey, aqe_enabled):
    num_chunks = 20

    def do_write(conf, data_path, is_optimize_write):
        def setup_tables(spark):
            spark.sql("CREATE TABLE delta.`{}/CPU` (a INT) USING DELTA TBLPROPERTIES ({} = {})".format(data_path, confkey, is_optimize_write))
            spark.sql("CREATE TABLE delta.`{}/GPU` (a INT) USING DELTA TBLPROPERTIES ({} = {})".format(data_path, confkey, is_optimize_write))
        with_cpu_session(setup_tables)
        assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, int_gen).repartition(num_chunks)
                .write.format("delta").mode("append").save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            conf=conf)
    do_test_optimize_write(spark_tmp_path, aqe_enabled, do_write, num_chunks)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(not is_databricks_runtime(), reason="Delta Lake optimized writes are only supported on Databricks")
def test_delta_write_optimized_supported_types(spark_tmp_path):
    num_chunks = 20
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(writer_confs, delta_writes_enabled_conf, {
        "spark.sql.execution.sortBeforeRepartition": "true",
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true"
    })
    simple_gens = [ byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                    string_gen, boolean_gen, date_gen, TimestampGen() ]
    genlist = simple_gens + \
        [ StructGen([("child" + str(i), gen) for i, gen in enumerate(simple_gens)]) ] + \
        [ StructGen([("x", StructGen([("y", int_gen)]))]) ]
    gens = [("c" + str(i), gen) for i, gen in enumerate(genlist)]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gens) \
            .repartition(num_chunks).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)
    opmetrics = get_last_operation_metrics(data_path + "/GPU")
    assert int(opmetrics["numFiles"]) < 20

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(not is_databricks_runtime(), reason="Delta Lake optimized writes are only supported on Databricks")
def test_delta_write_optimized_supported_types_partitioned(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(writer_confs, delta_writes_enabled_conf, {
        "spark.sql.execution.sortBeforeRepartition": "true",
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true"
    })
    genlist = [ SetValuesGen(StringType(), ["a", "b", "c"]) ] + delta_write_gens
    gens = [("c" + str(i), gen) for i, gen in enumerate(genlist)]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gens) \
            .write.format("delta").partitionBy("c0").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)

@allow_non_gpu(delta_optimized_write_fallback_allow, *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(not is_databricks_runtime(), reason="Delta Lake optimized writes are only supported on Databricks")
@pytest.mark.parametrize("gen", [
    simple_string_to_string_map_gen,
    StructGen([("x", ArrayGen(int_gen))]),
    ArrayGen(StructGen([("x", long_gen)]))], ids=idfn)
def test_delta_write_optimized_unsupported_sort_fallback(spark_tmp_path, gen):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.sql.execution.sortBeforeRepartition": "true",
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true"
    })
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        delta_write_fallback_check,
        conf=confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(not is_databricks_runtime(), reason="Delta Lake optimized writes are only supported on Databricks")
def test_delta_write_optimized_table_confs(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    gpu_data_path = data_path + "/GPU"
    num_chunks = 20
    def do_write(confs):
        assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, int_gen)\
                .repartition(num_chunks).write.format("delta").mode("overwrite").save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            conf=confs)
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.databricks.delta.optimizeWrite.enabled" : "true"
    })
    do_write(confs)
    opmetrics = get_last_operation_metrics(gpu_data_path)
    assert int(opmetrics["numFiles"]) == 1
    # Verify SQL conf takes precedence over table setting
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.databricks.delta.optimizeWrite.enabled" : "false"
    })
    do_write(confs)
    opmetrics = get_last_operation_metrics(gpu_data_path)
    assert int(opmetrics["numFiles"]) == num_chunks
    # Verify default conf is not honored after table setting
    def do_prop_update(spark):
        spark.sql("ALTER TABLE delta.`{}`".format(gpu_data_path) +
                  " SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)")
    with_cpu_session(do_prop_update)
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" : "false"
    })
    do_write(confs)
    opmetrics = get_last_operation_metrics(gpu_data_path)
    assert int(opmetrics["numFiles"]) == 1

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(not is_databricks_runtime(), reason="Delta Lake optimized writes are only supported on Databricks")
def test_delta_write_optimized_partitioned(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    gpu_data_path = data_path + "/GPU"
    num_chunks = 20
    def do_write(confs):
        assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: two_col_df(spark, int_gen, SetValuesGen(StringType(), ["x", "y"]))\
                .repartition(num_chunks).write.format("delta")\
                .mode("overwrite").partitionBy("b").save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            conf=confs)
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.databricks.delta.optimizeWrite.enabled" : "false"
    })
    do_write(confs)
    opmetrics = get_last_operation_metrics(gpu_data_path)
    assert int(opmetrics["numFiles"]) == 2 * num_chunks
    # Verify SQL conf takes precedence over table setting
    confs=copy_and_update(delta_writes_enabled_conf, {
        "spark.databricks.delta.optimizeWrite.enabled" : "true"
    })
    do_write(confs)
    opmetrics = get_last_operation_metrics(gpu_data_path)
    assert int(opmetrics["numFiles"]) == 2

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_partial_overwrite_replace_where(spark_tmp_path):
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x", "y", "z"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")\
            .partitionBy("b", "d")\
            .save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    # Avoid checking delta log equivalence here. Using partition columns involves sorting, and
    # there's no guarantees on the task partitioning due to random sampling.
    #
    # overwrite with a subset of the original schema
    gen_list = [("b", SetValuesGen(StringType(), ["y"])),
                ("e", long_gen),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3]))]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")\
            .mode("overwrite")\
            .partitionBy("b", "d")\
            .option("replaceWhere", "b = 'y'")\
            .save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    # Avoid checking delta log equivalence here. Using partition columns involves sorting, and
    # there's no guarantees on the task partitioning due to random sampling.

# ID mapping is supported starting in Delta Lake 2.2, but currently cannot distinguish
# Delta Lake 2.1 from 2.2 in tests. https://github.com/NVIDIA/spark-rapids/issues/9276
column_mappings = ["name"]
if is_spark_340_or_later() or is_databricks_runtime():
    column_mappings.append("id")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("mapping", column_mappings)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_column_name_mapping(spark_tmp_path, mapping):
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x", "y", "z"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf, {
        "spark.databricks.delta.properties.defaults.columnMapping.mode": mapping,
        "spark.databricks.delta.properties.defaults.minReaderVersion": "2",
        "spark.databricks.delta.properties.defaults.minWriterVersion": "5",
        "spark.sql.parquet.fieldId.read.enabled": "true"
    })
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta") \
            .partitionBy("b", "d") \
            .save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)

# Hash aggregate can be used in a metadata query for compaction which completely falls back
compaction_allow = "HashAggregateExec"
if is_databricks_runtime():
    # compaction can fallback due to unsupported WriteIntoDeltaCommand
    # tracked by https://github.com/NVIDIA/spark-rapids/issues/11169
    compaction_allow += "," + delta_write_fallback_allow
@allow_non_gpu(compaction_allow, *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_compaction(spark_tmp_path, enable_deletion_vectors):
    from delta.tables import DeltaTable
    def do_write(spark,  path):
        get_writer_with_deletion_vector_property_set(spark.range(1000).write.mode("append").format("delta"), enable_deletion_vectors).save(path)
        DeltaTable.forPath(spark, path).optimize().executeCompaction()
    data_path = spark_tmp_path + "/DELTA_DATA"
    with_cpu_session(
        lambda spark: _create_cpu_gpu_tables(spark, data_path, "id bigint"), conf=_delta_confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        do_write,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=_delta_confs)
