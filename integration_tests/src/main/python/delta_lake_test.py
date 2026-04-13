# Copyright (c) 2022-2026, NVIDIA CORPORATION.
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


def do_test_delta_deletion_vector_read(data_path, use_cdf, conf, test_sql, post_setup_table_sqls=[]):
    num_rows_per_slice = 2048
    num_slices = 3
    target_num_row_groups = 3
    # num_rows_per_slice * 4 bytes per int / target_num_row_groups
    row_group_size = int(num_rows_per_slice * 4 / (target_num_row_groups))
    write_conf = copy_and_update(conf, {
        "parquet.block.size": str(row_group_size)
    })
    def setup_tables(spark):
        num_rows = num_rows_per_slice * num_slices
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen, length=num_rows, num_slices=num_slices),
                               use_cdf=use_cdf, enable_deletion_vectors=True)
        for sql in post_setup_table_sqls:
            spark.sql(sql)
    with_cpu_session(setup_tables, conf=write_conf)

    def verify_files_and_row_groups():
        import pyarrow.parquet as pq

        # list files in data_path
        files = [f for f in os.listdir(data_path) if f.endswith(".parquet")]
        files = [f"{data_path}/{f}" for f in files]
        # iterate files to find at least one with more row groups than the target_num_row_groups.
        parquet_file = None
        for f in files:
            metadata = pq.read_metadata(f)
            if metadata.num_row_groups >= target_num_row_groups:
                parquet_file = f
                break
        assert parquet_file is not None, f"Expected at least one parquet file with {target_num_row_groups} row groups in the parquet"
    verify_files_and_row_groups()

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(test_sql),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("chunk_size", ["2000", "4000", None], ids=idfn)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("parquet_reader_type", ["PERFILE", "COALESCING"], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_deletion_vector_read(spark_tmp_path, chunk_size, use_cdf, dv_predicate_pushdown, parquet_reader_type, use_metadata_row_index):
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {"spark.databricks.delta.delete.deletionVectors.persistent": "true",
            "spark.rapids.sql.reader.chunked": f"{chunk_size is not None}",
            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
            "spark.rapids.sql.format.parquet.reader.type": f"{parquet_reader_type}",
            "spark.rapids.sql.reader.batchSizeBytes": f"{chunk_size if chunk_size is not None else '0'}",
            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}"}

    do_test_delta_deletion_vector_read(
        data_path, use_cdf, conf,
        f"SELECT * FROM delta.`{data_path}`",
        post_setup_table_sqls=[
            "INSERT INTO delta.`{}` VALUES(1)".format(data_path),
            "DELETE FROM delta.`{}` WHERE a = 1".format(data_path)
        ])


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("chunk_size", ["2000", "4000", None], ids=idfn)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.parametrize("combine_size", ["0", "1M"], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_deletion_vector_multithreaded_read(spark_tmp_path, chunk_size, use_cdf,
                                                  dv_predicate_pushdown, use_metadata_row_index,
                                                  combine_size):
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {"spark.databricks.delta.delete.deletionVectors.persistent": "true",
            "spark.rapids.sql.reader.chunked": f"{chunk_size is not None}",
            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
            "spark.rapids.sql.format.parquet.reader.type": "MULTITHREADED",
            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}",
            "spark.rapids.sql.reader.batchSizeBytes": f"{chunk_size if chunk_size is not None else '0'}",
            "spark.rapids.sql.reader.multithreaded.combine.sizeBytes": f"{combine_size}"}

    do_test_delta_deletion_vector_read(
        data_path, use_cdf, conf,
        f"SELECT * FROM delta.`{data_path}`",
        post_setup_table_sqls=[
            "INSERT INTO delta.`{}` VALUES(1)".format(data_path),
            "DELETE FROM delta.`{}` WHERE a = 1".format(data_path)
        ])


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
@pytest.mark.skipif(is_databricks_runtime(), reason="This test is currently failing on Databricks due to https://github.com/nviDIA/spark-rapids/issues/14319")
def test_delta_deletion_vector_multithreaded_combine_count_star(
        spark_tmp_path, use_cdf,  dv_predicate_pushdown, use_metadata_row_index):
    """
    This test verifies the case when reading no columns from a Delta table with deletion vectors.
    In this case, the plugin will create a ColumnarBatch with 0 columns but with a valid row count.
    We should still take the deleted row count into account to make sure the row count in the
    ColumnarBatch is correct.
    """

    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {"spark.databricks.delta.delete.deletionVectors.persistent": "true",
            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
            "spark.rapids.sql.format.parquet.reader.type": "MULTITHREADED",
            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}",
            "spark.rapids.sql.reader.multithreaded.combine.sizeBytes": "1M",
            "spark.sql.files.maxRecordsPerFile": "200" # set a small maxRecordsPerFile to create more than 1 file in each partition
            }

    def setup_tables(spark):
        col_a_gen = IntegerGen(min_val=0, max_val=100, nullable=False, special_cases=[1, 2, 3])
        col_b_gen = IntegerGen(min_val=0, max_val=32, nullable=False, special_cases=[0])
        num_rows = 20480 # make sure we have enough rows to create multiple files in each partition
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: two_col_df(spark, col_a_gen, col_b_gen, length=num_rows),
                               use_cdf=False, enable_deletion_vectors=True, partition_columns=["b"])
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 0)") # make sure there will be a file with one row with a = 1, which will be deleted.
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 33)") # make sure there will be a partition with only 1 row, which will be deleted.
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 1")
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 2")
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 3")
    with_cpu_session(setup_tables, conf=conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT count(*) FROM delta.`{data_path}` WHERE b = 0"),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.parametrize("combine_size", ["0", "1M"], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_deletion_vector_multithreaded_read_partitioned_table(
        spark_tmp_path, dv_predicate_pushdown, use_metadata_row_index, combine_size):
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {"spark.databricks.delta.delete.deletionVectors.persistent": "true",
            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
            "spark.rapids.sql.format.parquet.reader.type": "MULTITHREADED",
            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}",
            "spark.rapids.sql.reader.multithreaded.combine.sizeBytes": f"{combine_size}",
            "spark.sql.files.maxRecordsPerFile": "200" # set a small maxRecordsPerFile to create more than 1 file in each partition
            }

    def setup_tables(spark):
        col_a_gen = IntegerGen(min_val=0, max_val=100, nullable=False, special_cases=[1])
        col_b_gen = IntegerGen(min_val=0, max_val=32, nullable=False, special_cases=[0])
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: two_col_df(spark, col_a_gen, col_b_gen, length=20480),
                               use_cdf=False, enable_deletion_vectors=True, partition_columns=["b"])
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 0)") # make sure there will be a file with one row with a = 1, which will be deleted.
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 33)") # make sure there will be a partition with only 1 row, which will be deleted.
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 1")
    with_cpu_session(setup_tables, conf=conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT * FROM delta.`{}`".format(data_path)),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("use_chunked_reader", [True, False], ids=idfn)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("parquet_reader_type", ["PERFILE", "COALESCING", "MULTITHREADED"], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_empty_deletion_vector_read(spark_tmp_path, use_chunked_reader, use_cdf, dv_predicate_pushdown, parquet_reader_type, use_metadata_row_index):
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {"spark.databricks.delta.delete.deletionVectors.persistent": "true",
            "spark.rapids.sql.reader.chunked": f"{use_chunked_reader}",
            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
            "spark.rapids.sql.format.parquet.reader.type": f"{parquet_reader_type}",
            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}"}
    do_test_delta_deletion_vector_read(data_path, use_cdf, conf, f"SELECT * FROM delta.`{data_path}`")


def do_test_scan_split(spark_tmp_path, enable_deletion_vectors, expected_num_partitions, post_setup_table_func=None, conf=None):
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
    table_setup_conf = {"parquet.block.size": str(row_group_size)}
    with_cpu_session(setup_tables, table_setup_conf)
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

    read_conf = {"spark.sql.files.maxPartitionBytes": str(math.ceil(file_size/2.0))}
    if conf:
        read_conf = copy_and_update(read_conf, conf)

    def get_num_partitions(spark):
        df = spark.sql("SELECT * from delta.`{}`".format(data_path))
        return df.rdd.getNumPartitions()
    num_partitions = with_gpu_session(get_num_partitions, conf=read_conf)
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
@pytest.mark.parametrize("pushdown_dv_predicate", [True, False], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
def test_delta_scan_split_with_DV_enabled_with_DVs(spark_tmp_path, pushdown_dv_predicate):
    def do_delete(spark, data_path):
        num_deleted = spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0").collect()[0][0]
        assert num_deleted > 0, "Expected some rows to be deleted"
    # The cuDF-based reader (GpuDeltaParquetFileFormat2), which is used when dv_predicate_pushdown is True, support the file split,
    # whereas the scala reader (GpuDeltaParquetFileFormat) does not support it.
    # So we expect 2 partitions when dv_predicate_pushdown is True, and 1 partition when it is False.
    expected_num_partitions = 2 if pushdown_dv_predicate else 1
    conf = {"spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{pushdown_dv_predicate}"}
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=True, expected_num_partitions=expected_num_partitions, post_setup_table_func=do_delete, conf=conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.parametrize("pushdown_dv_predicate", [True, False], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
def test_delta_scan_split_with_DV_disabled_with_DVs(spark_tmp_path, pushdown_dv_predicate):
    def do_delete_and_disable_DV(spark, data_path):
        num_deleted = spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0").collect()[0][0]
        assert num_deleted > 0, "Expected some rows to be deleted"
        spark.sql(f"ALTER TABLE delta.`{data_path}` SET TBLPROPERTIES " +
                  "('delta.enableDeletionVectors' = 'false')")
    # The cuDF-based reader (GpuDeltaParquetFileFormat2), which is used when dv_predicate_pushdown is True, supports the file split,
    # whereas the scala reader (GpuDeltaParquetFileFormat) does not support it.
    # So we expect 2 partitions when dv_predicate_pushdown is True, and 1 partition when it is False.
    expected_num_partitions = 2 if pushdown_dv_predicate else 1
    conf = {"spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{pushdown_dv_predicate}"}
    do_test_scan_split(spark_tmp_path, enable_deletion_vectors=True, expected_num_partitions=expected_num_partitions, post_setup_table_func=do_delete_and_disable_DV, conf=conf)


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

@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks Spark generates a different query plan for the test query that is not convertible to a GPU plan")
def test_delta_deletion_vector_coalescing_count_star(
        spark_tmp_path, dv_predicate_pushdown, use_metadata_row_index):
    """
    Verifies alive row counts are correct with COUNT(*) (zero-column projection) and
    the COALESCING reader.
    """
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
        "spark.rapids.sql.format.parquet.reader.type": "COALESCING",
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}",
        "spark.sql.files.maxRecordsPerFile": "200" # set a small maxRecordsPerFile to create more than 1 file in each partition
    }

    def setup_tables(spark):
        col_a_gen = IntegerGen(min_val=0, max_val=100, nullable=False, special_cases=[1, 2, 3])
        col_b_gen = IntegerGen(min_val=0, max_val=32, nullable=False, special_cases=[0])
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: two_col_df(spark, col_a_gen, col_b_gen, length=20480),
                               use_cdf=False, enable_deletion_vectors=True, partition_columns=["b"])
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 0)") # make sure there will be a file with one row with a = 1, which will be deleted.
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 33)") # make sure there will be a partition with only 1 row, which will be deleted.
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 1")
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 2")
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 3")
    with_cpu_session(setup_tables, conf=conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT count(*) FROM delta.`{data_path}` WHERE b = 0"),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.parametrize("use_metadata_row_index", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_deletion_vector_coalescing_partitioned_table(
        spark_tmp_path, dv_predicate_pushdown, use_metadata_row_index):
    """
    Verifies partition values are attached correctly after DV filtering when files
    from the same partition are coalesced into one batch.
    """
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
        "spark.rapids.sql.format.parquet.reader.type": "COALESCING",
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex": f"{use_metadata_row_index}",
        "spark.sql.files.maxRecordsPerFile": "200" # set a small maxRecordsPerFile to create more than 1 file in each partition
    }

    def setup_tables(spark):
        col_a_gen = IntegerGen(min_val=0, max_val=100, nullable=False, special_cases=[1])
        col_b_gen = IntegerGen(min_val=0, max_val=32, nullable=False, special_cases=[0])
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: two_col_df(spark, col_a_gen, col_b_gen, length=20480),
                               use_cdf=False, enable_deletion_vectors=True, partition_columns=["b"])
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 0)") # make sure there will be a file with one row with a = 1, which will be deleted.
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(1, 33)") # make sure there will be a partition with only 1 row, which will be deleted.
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 1")
    with_cpu_session(setup_tables, conf=conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT * FROM delta.`{data_path}`"),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("reader_type", ["PERFILE", "MULTITHREADED", "COALESCING"], ids=idfn)
@pytest.mark.parametrize("dv_predicate_pushdown", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
def test_delta_deletion_vector_mixed_dv_no_dv(spark_tmp_path, reader_type, dv_predicate_pushdown):
    """
    Correctly handles a batch containing both DV-bearing files and files without DVs.
    Non-DV files should use empty bitmaps so all their rows are returned.
    """
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": f"{dv_predicate_pushdown}",
        "spark.rapids.sql.format.parquet.reader.type": reader_type,
        "spark.sql.files.maxRecordsPerFile": "200",
    }

    def setup_tables(spark):
        # Initial data: rows with a=0 and a=1. DELETE only targets a=0, so files
        # containing only a=1 rows will have no DV; files with a=0 rows will.
        col_a_gen = IntegerGen(min_val=0, max_val=1, nullable=False, special_cases=[0, 1])
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, col_a_gen, length=4000),
                               use_cdf=False, enable_deletion_vectors=True)
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0")
        # Insert a fresh file with no deletions (guaranteed no DV)
        spark.sql(f"INSERT INTO delta.`{data_path}` VALUES(2)")
    with_cpu_session(setup_tables, conf=conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT * FROM delta.`{data_path}`"),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("reader_type", ["PERFILE", "MULTITHREADED", "COALESCING"], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/7733")
def test_delta_deletion_vector_ignore_missing_files(spark_tmp_path, reader_type):
    """
    When ignoreMissingFiles=true and one DV-bearing file has been removed, the reader
    does not crash and GPU/CPU results agree for the surviving files.
    """
    import os
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.format.parquet.reader.type": reader_type,
        "spark.sql.files.ignoreMissingFiles": "true",
        "spark.sql.files.maxRecordsPerFile": "200",
        "spark.sql.adaptive.enabled": "false" # disable AQE temporarily until https://github.com/nviDIA/spark-rapids/issues/14319 is resolved.
    }

    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen, length=4000),
                               use_cdf=False, enable_deletion_vectors=True)
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0")
    with_cpu_session(setup_tables, conf=conf)

    # Remove one parquet file to simulate a missing file
    parquet_files = sorted(f for f in os.listdir(data_path) if f.endswith(".parquet"))
    assert len(parquet_files) > 1, "Expected multiple parquet files for this test"
    os.remove(os.path.join(data_path, parquet_files[0]))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT * FROM delta.`{data_path}`"),
        conf=conf)


@allow_non_gpu("FileSourceScanExec", "ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("reader_type", ["PERFILE", "MULTITHREADED", "COALESCING"], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(),
                    reason="Delta Lake deletion vector support is required")
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/7733")
def test_delta_deletion_vector_ignore_corrupt_files(spark_tmp_path, reader_type):
    """
    When ignoreCorruptFiles=true, the corrupt file is silently skipped and
    GPU/CPU results agree on the surviving files.
    Note: COALESCING falls back to MULTITHREADED when ignoreCorruptFiles=true.
    """
    import os
    data_path = spark_tmp_path + "/DELTA_DATA"
    conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.format.parquet.reader.type": reader_type,
        "spark.sql.files.ignoreCorruptFiles": "true",
        "spark.sql.files.maxRecordsPerFile": "200",
        "spark.sql.adaptive.enabled": "false" # disable AQE temporarily until https://github.com/nviDIA/spark-rapids/issues/14319 is resolved.
    }

    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen, length=4000),
                               use_cdf=False, enable_deletion_vectors=True)
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a = 0")
    with_cpu_session(setup_tables, conf=conf)

    # Corrupt one parquet file
    parquet_files = sorted(f for f in os.listdir(data_path) if f.endswith(".parquet"))
    assert len(parquet_files) > 1, "Expected multiple parquet files"
    with open(os.path.join(data_path, parquet_files[0]), "wb") as f:
        f.write(b"NOT A VALID PARQUET FILE")

    # Verify GPU and CPU agree on the result (corrupt file silently skipped).
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT * FROM delta.`{data_path}`"),
        conf=conf)


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


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("parquet_reader_type", ["PERFILE", "COALESCING", "MULTITHREADED"], ids=idfn)
@pytest.mark.parametrize("footer_type", ["NATIVE", "JAVA"], ids=idfn)
@pytest.mark.parametrize("query", [
    "SELECT a FROM delta.`{path}`",
    "SELECT a, b FROM delta.`{path}`",
], ids=["one_col", "two_cols"])
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
def test_delta_deletion_vector_native_footer_multi_row_group(spark_tmp_path, parquet_reader_type,
                                                             footer_type, query):
    """
    Tests deletion vector filtering on a Delta table whose single Parquet file has multiple
    row groups, with deletions targeting rows beyond the first row group. A small
    maxPartitionBytes forces Spark to assign per-row-group splits so the footer reader
    sees only a subset of the file's row groups per split.
    """
    data_path = spark_tmp_path + "/DELTA_DATA"
    num_rows = 10000
    # Small row group size → multiple row groups per file.
    # 10000 rows * 4 bytes * 3 cols = 120KB total; with 10KB row groups we get ~12 row groups.
    row_group_size = 10000

    write_conf = {
        "parquet.block.size": str(row_group_size),
    }
    read_conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": "true",
        "spark.rapids.sql.format.parquet.reader.type": parquet_reader_type,
        "spark.rapids.sql.format.parquet.reader.footer.type": footer_type,
        # Force Spark to split the file at row group boundaries so the NATIVE footer
        # reader returns one row group per PartitionedFile split.
        "spark.sql.files.maxPartitionBytes": str(row_group_size),
    }

    def setup_tables(spark):
        # Create a multi-column table with monotonic data so row positions are predictable.
        # coalesce(1) ensures a single data file with multiple row groups.
        spark.range(num_rows).selectExpr(
            "CAST(id AS INT) AS a",
            "CAST(id * 2 AS INT) AS b",
            "CAST(id * 3 AS INT) AS c"
        ).coalesce(1).write.format("delta") \
            .option("delta.enableDeletionVectors", "true") \
            .save(data_path)
        # Delete rows in later row groups. With ~800 rows per row group,
        # rows a >= 5000 are in row group 6+.
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a >= 5000 AND a < 5100")

    with_cpu_session(setup_tables, conf=write_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(query.format(path=data_path)),
        conf=read_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("parquet_reader_type", ["COALESCING", "MULTITHREADED"], ids=idfn)
@pytest.mark.parametrize("footer_type", ["NATIVE", "JAVA"], ids=idfn)
@pytest.mark.skipif(is_before_spark_353(),
                    reason="Spark-RAPIDS supports scan with deletion vectors starting in Spark 3.5.3")
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="Deletion vector scan is not supported on Databricks")
def test_delta_deletion_vector_native_footer_multi_row_group_count_star(
        spark_tmp_path, parquet_reader_type, footer_type):
    """
    Tests zero-column projection (COUNT(*)) with deletion vectors on a partitioned Delta
    table where each partition's Parquet file has multiple row groups. Uses a partition
    filter so Spark performs a true zero-column scan while still applying DVs.
    """
    data_path = spark_tmp_path + "/DELTA_DATA"
    num_rows = 10000
    row_group_size = 10000

    write_conf = {
        "parquet.block.size": str(row_group_size),
    }
    read_conf = {
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled": "true",
        "spark.rapids.sql.format.parquet.reader.type": parquet_reader_type,
        "spark.rapids.sql.format.parquet.reader.footer.type": footer_type,
        "spark.sql.files.maxPartitionBytes": str(row_group_size),
    }

    def setup_tables(spark):
        # Partition by a column with few distinct values so each partition has enough
        # rows to produce multiple row groups per file.
        spark.range(num_rows).selectExpr(
            "CAST(id AS INT) AS a",
            "CAST(id % 2 AS INT) AS part"
        ).coalesce(1).write.format("delta") \
            .option("delta.enableDeletionVectors", "true") \
            .partitionBy("part") \
            .save(data_path)
        # Delete rows in later row groups within partition part=0.
        spark.sql(f"DELETE FROM delta.`{data_path}` WHERE a >= 5000 AND a < 5100 AND part = 0")

    with_cpu_session(setup_tables, conf=write_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT COUNT(*) FROM delta.`{data_path}` WHERE part = 0"),
        conf=read_conf)
