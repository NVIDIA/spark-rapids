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

import pytest
from pyspark.sql import Row
from asserts import assert_gpu_fallback_collect, assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from delta_lake_utils import delta_meta_allow, setup_delta_dest_table, deletion_vector_values_with_350DB143_xfail_reasons
from marks import allow_non_gpu, delta_lake, ignore_order
from parquet_test import reader_opt_confs_no_native
from spark_session import with_cpu_session, with_gpu_session, is_databricks_runtime, \
    is_spark_320_or_later, is_spark_340_or_later, supports_delta_lake_deletion_vectors, is_spark_401_or_later, \
    is_before_spark_353

_conf = {'spark.rapids.sql.explain': 'ALL'}

@delta_lake
@allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(not (is_databricks_runtime() or is_spark_320_or_later()), \
    reason="Delta Lake is already configured on Databricks and CI supports Delta Lake OSS with Spark 3.2.x so far")
def test_delta_metadata_query_fallback(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_delta_table(spark):
        df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ["id", "data"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table))
    with_cpu_session(setup_delta_table)
    # note that this is just testing that any reads against a delta log json file fall back to CPU and does
    # not test the actual metadata queries that the delta lake plugin generates so does not fully test the
    # plugin code
    assert_gpu_fallback_collect(
        lambda spark : spark.read.json("/tmp/delta-table/{}/_delta_log/00000000000000000000.json".format(table)),
        "FileSourceScanExec", conf = _conf)

@delta_lake
@pytest.mark.skipif(not is_databricks_runtime(), \
    reason="This test is specific to Databricks because we only fall back to CPU for merges on Databricks")
@allow_non_gpu(any = True)
def test_delta_merge_query(spark_tmp_table_factory):
    table_name_1 = spark_tmp_table_factory.get()
    table_name_2 = spark_tmp_table_factory.get()
    def setup_delta_table1(spark):
        df = spark.createDataFrame([('a', 10), ('b', 20)], ["c0", "c1"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table_name_1))
    def setup_delta_table2(spark):
        df = spark.createDataFrame([('a', 30), ('c', 30)], ["c0", "c1"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table_name_2))
    with_cpu_session(setup_delta_table1)
    with_cpu_session(setup_delta_table2)
    def merge(spark):
        spark.read.format("delta").load("/tmp/delta-table/{}".format(table_name_1)).createOrReplaceTempView("t1")
        spark.read.format("delta").load("/tmp/delta-table/{}".format(table_name_2)).createOrReplaceTempView("t2")
        return spark.sql("MERGE INTO t1 USING t2 ON t1.c0 = t2.c0 \
            WHEN MATCHED THEN UPDATE SET c1 = t1.c1 + t2.c1 \
            WHEN NOT MATCHED THEN INSERT (c0, c1) VALUES (t2.c0, t2.c1)").collect()
    # run the MERGE on GPU
    with_gpu_session(lambda spark : merge(spark), conf = _conf)
    # check the results on CPU
    result = with_cpu_session(lambda spark: spark.sql("SELECT * FROM t1 ORDER BY c0").collect(), conf=_conf)
    assert [Row(c0='a', c1=40), Row(c0='b', c1=20), Row(c0='c', c1=30)] == result

@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
def test_delta_scan_read(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                               use_cdf=False, enable_deletion_vectors=False)
    with_cpu_session(setup_tables)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT * FROM delta.`{}`".format(data_path)))


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_deletion_vector_read(spark_tmp_path, use_cdf):
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {"spark.databricks.delta.delete.deletionVectors.persistent": "true"}
    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                               use_cdf=use_cdf, enable_deletion_vectors=True)
        spark.sql("INSERT INTO delta.`{}` VALUES(1)".format(data_path))
        spark.sql("DELETE FROM delta.`{}` WHERE a = 1".format(data_path))
    with_cpu_session(setup_tables, conf=conf)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT * FROM delta.`{}`".format(data_path)),
        conf=conf)


def do_test_scan_split(spark_tmp_path, enable_deletion_vectors, expected_num_partitions, post_setup_table_func=None):
    import os
    import math

    data_path = spark_tmp_path + "/DELTA_DATA"
    num_rows = 2048
    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen, length=num_rows, num_slices=1),
                               use_cdf=False, enable_deletion_vectors=enable_deletion_vectors)
        if post_setup_table_func:
            post_setup_table_func(spark, data_path)
    target_num_row_groups = 2
    row_group_size = int(num_rows * 4 / target_num_row_groups) # num_rows * 4 bytes per int / target_num_row_groups
    conf = {"parquet.block.size": str(row_group_size)}
    with_cpu_session(setup_tables, conf)
    # Verify that we have 1 file with 2 row groups
    def verify_files_and_row_groups():
        # list files in data_path
        files = [f for f in os.listdir(data_path) if f.endswith(".parquet")]
        files = [f"{data_path}/{f}" for f in files]
        # find the most recently modified parquet file
        most_recent_file = max(files, key=os.path.getmtime)
        parquet_file = most_recent_file

        import pyarrow.parquet as pq
        metadata = pq.read_metadata(parquet_file)
        assert metadata.num_row_groups == target_num_row_groups, f"Expected {target_num_row_groups} row groups in the parquet"
        return parquet_file
    data_file = verify_files_and_row_groups()
    file_size = os.path.getsize(data_file)

    conf = {"spark.sql.files.maxPartitionBytes": str(math.ceil(file_size/2.0))}

    def get_num_partitions(spark):
        df = spark.sql("SELECT * from delta.`{}`".format(data_path))
        return df.rdd.getNumPartitions()
    num_partitions = with_gpu_session(get_num_partitions, conf=conf)
    assert num_partitions == expected_num_partitions, f"Expected {expected_num_partitions} partitions for split read"


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Scan split works differently on Databricks")
def test_delta_scan_split_with_no_dv(spark_tmp_path):
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=False, expected_num_partitions=2)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
def test_delta_scan_split_with_DV_enabled_with_no_DV(spark_tmp_path):
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=True, expected_num_partitions=2)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
def test_delta_scan_split_with_DV_enabled_with_DVs(spark_tmp_path):
    def do_delete(spark, data_path):
        num_deleted = spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0").collect()[0][0]
        assert num_deleted > 0, "Expected some rows to be deleted"
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=True, expected_num_partitions=1, post_setup_table_func=do_delete)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
def test_delta_scan_split_with_DV_disabled_with_DVs(spark_tmp_path):
    def do_delete_and_disable_DV(spark, data_path):
        num_deleted = spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0").collect()[0][0]
        assert num_deleted > 0, "Expected some rows to be deleted"
        spark.sql(f"ALTER TABLE delta.`{data_path}` SET TBLPROPERTIES " +
                  "('delta.enableDeletionVectors' = 'false')")
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=True, expected_num_partitions=1, post_setup_table_func=do_delete_and_disable_DV)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
@pytest.mark.skipif(is_spark_401_or_later(),
                    reason="REORG is not supported in Spark 4.0.1+ (https://github.com/delta-io/delta/issues/5690)")
def test_delta_scan_split_with_DV_enabled_after_DVs_materialized(spark_tmp_path):
    def do_delete_and_reorg(spark, data_path):
        num_deleted = spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0").collect()[0][0]
        assert num_deleted > 0, "Expected some rows to be deleted"
        spark.sql(f"REORG table delta.`{data_path}` APPLY (PURGE)") # will rewrite files to purge soft-deleted data
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=True, expected_num_partitions=2, post_setup_table_func=do_delete_and_reorg)


# ID mapping is supported starting in Delta Lake 2.2, but currently cannot distinguish
# Delta Lake 2.1 from 2.2 in tests. https://github.com/NVIDIA/spark-rapids/issues/9276
column_mappings = ["name"]
if is_spark_340_or_later() or is_databricks_runtime():
    column_mappings.append("id")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("reader_confs", reader_opt_confs_no_native, ids=idfn)
@pytest.mark.parametrize("mapping", column_mappings, ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_read_column_mapping(spark_tmp_path, reader_confs, mapping, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_DATA"
    gen_list = [("a", int_gen),
                ("b", SetValuesGen(StringType(), ["x", "y", "z"])),
                ("c", string_gen),
                ("d", SetValuesGen(IntegerType(), [1, 2, 3])),
                ("e", long_gen)]
    confs = copy_and_update(reader_confs, {
        "spark.databricks.delta.properties.defaults.columnMapping.mode": mapping,
        "spark.databricks.delta.properties.defaults.minReaderVersion": "2",
        "spark.databricks.delta.properties.defaults.minWriterVersion": "5",
        "spark.sql.parquet.fieldId.read.enabled": "true"
    })
    def create_delta(spark):
        df = gen_df(spark, gen_list).coalesce(1).write.format("delta")
        if supports_delta_lake_deletion_vectors():
            df.option("delta.enableDeletionVectors", str(enable_deletion_vectors).lower())
        df.partitionBy("b", "d") \
        .save(data_path)
    with_cpu_session(create_delta, conf=confs)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.read.format("delta").load(data_path),
                                         conf=confs)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_spark_401_or_later(), \
    reason="Delta Lake 4.0.0 incompatible with Spark 4.0.1 - ParquetToSparkSchemaConverter API changed")
@pytest.mark.skipif(not (is_databricks_runtime() or is_spark_340_or_later()), \
                    reason="ParquetToSparkSchemaConverter changes not compatible with Delta Lake")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_350DB143_xfail_reasons(
                            enabled_xfail_reason='https://github.com/NVIDIA/spark-rapids/issues/12042'), ids=idfn)
def test_delta_name_column_mapping_no_field_ids(spark_tmp_path, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_parquet_table(spark):
        spark.range(10).coalesce(1).write.parquet(data_path)
    def convert_and_setup_name_mapping(spark):
        spark.sql(f"CONVERT TO DELTA parquet.`{data_path}`")
        spark.sql(f"ALTER TABLE delta.`{data_path}` SET TBLPROPERTIES " +
            "('delta.minReaderVersion' = '2', " +
            "'delta.minWriterVersion' = '5', " +
            "'delta.columnMapping.mode' = 'name')")
    with_cpu_session(setup_parquet_table, {"spark.sql.parquet.fieldId.write.enabled": str(enable_deletion_vectors).lower()})
    with_cpu_session(convert_and_setup_name_mapping, conf={"spark.databricks.delta.properties.defaults.enableDeletionVectors": "false"})
    assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.read.format("delta").load(data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
def test_delta_filter_out_metadata_col(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"

    col_a_gen = IntegerGen(min_val=0, max_val=100, nullable=False, special_cases=[])
    col_b_gen = IntegerGen(min_val=0, max_val=1, nullable=False, special_cases=[0, 1])

    def create_delta(spark):
        two_col_df(spark, col_a_gen, col_b_gen, length=4000).coalesce(1).write.format("delta") \
            .option("delta.enableDeletionVectors", "true") \
            .partitionBy("a").save(data_path)

        count = spark.sql(f"DELETE FROM delta.`{data_path}` WHERE b = 0").collect()[0][0]
        assert count > 100, "Expected enough rows to be deleted to create deletion vectors"

    def read_table(spark):
        df = spark.sql(f"SELECT * FROM delta.`{data_path}`")
        assert "__delta_internal_is_row_deleted" in df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "extended")
        return df

    with_cpu_session(create_delta)
    assert_gpu_and_cpu_are_equal_collect(read_table)
