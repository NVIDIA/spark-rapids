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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal, assert_gpu_fallback_collect, assert_spark_exception
from data_gen import *
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_before_spark_320, is_databricks_runtime, with_cpu_session, with_gpu_session

iceberg_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen, TimestampGen ]] + \
                    [simple_string_to_string_map_gen,
                     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
                     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
                     MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

iceberg_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
     string_gen, boolean_gen, date_gen, timestamp_gen, binary_gen, ArrayGen(binary_gen),
     ArrayGen(byte_gen), ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
     ArrayGen(timestamp_gen), ArrayGen(decimal_gen_64bit), ArrayGen(ArrayGen(byte_gen)),
     StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen], ['child3', decimal_gen_64bit]]),
     ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))
    ] + iceberg_map_gens + decimal_gens ]

rapids_reader_types = ['PERFILE', 'MULTITHREADED', 'COALESCING']

# pytestmark = pytest.mark.skip(reason="Skipping all iceberg tests as it's under refactoring: https://github.com/NVIDIA/spark-rapids/issues/12176")

@allow_non_gpu("BatchScanExec")
@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
def test_iceberg_fallback_not_unsafe_row(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        spark.sql("CREATE TABLE {} (id BIGINT, data STRING) USING ICEBERG".format(table))
        spark.sql("INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c')".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT COUNT(DISTINCT id) from {}".format(table)),
        conf={"spark.rapids.sql.format.iceberg.enabled": "false"}
    )

@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320() or is_databricks_runtime(),
                    reason="AQE+DPP not supported until Spark 3.2.0+ and AQE+DPP not supported on Databricks")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_aqe_dpp(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, int_gen, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} (a INT, b INT) USING ICEBERG PARTITIONED BY (a)".format(table))
        spark.sql("INSERT INTO {} SELECT * FROM {}".format(table, tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * from {} as X JOIN {} as Y ON X.a = Y.a WHERE Y.a > 0".format(table, table)),
        conf={"spark.sql.adaptive.enabled": "true",
              "spark.rapids.sql.format.parquet.reader.type": reader_type,
              "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true"})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_round_trip_select_one(spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG AS SELECT * FROM {}".format(table, tmpview))
    with_cpu_session(setup_iceberg_table)
    # explicitly only select 1 column to make sure we test that path in the schema parsing code
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT _c0 FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.xfail(reason="Nested data is not supported in Iceberg yet: "
                          "https://github.com/NVIDIA/spark-rapids/issues/12298")
def test_iceberg_parquet_read_round_trip(spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG AS SELECT * FROM {}".format(table, tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@pytest.mark.parametrize("data_gens", [[long_gen]], ids=idfn)
@pytest.mark.parametrize("iceberg_format", ["orc", "avro"], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_unsupported_formats(spark_tmp_table_factory, data_gens, iceberg_format, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "TBLPROPERTIES('write.format.default' = '{}') ".format(iceberg_format) + \
                  "AS SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_spark_exception(
        lambda : with_gpu_session(
            lambda spark : spark.sql("SELECT * FROM {}".format(table)).collect(),
            conf={'spark.rapids.sql.format.parquet.reader.type': reader_type}),
        "UnsupportedOperationException")

@iceberg
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("disable_conf", ["spark.rapids.sql.format.iceberg.enabled",
                                          "spark.rapids.sql.format.iceberg.read.enabled"], ids=idfn)
def test_iceberg_read_fallback(spark_tmp_table_factory, disable_conf):
    table = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        spark.sql("CREATE TABLE {} (id BIGINT, data STRING) USING ICEBERG".format(table))
        spark.sql("INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c')".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_fallback_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        "BatchScanExec",
        conf = {disable_conf : "false"})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
# Compression codec to test and whether the codec is supported by cudf
# Note that compression codecs brotli and lzo need extra jars
# https://githbub.com/NVIDIA/spark-rapids/issues/143
@pytest.mark.parametrize("codec_info", [
    ("uncompressed", None),
    ("snappy", None),
    ("gzip", None),
    pytest.param(("lz4", "Unsupported compression type"),
                 marks=pytest.mark.skipif(is_before_spark_320(),
                                          reason="Hadoop with Spark 3.1.x does not support lz4 by default")),
    ("zstd", None)], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_parquet_compression_codec(spark_tmp_table_factory, codec_info, reader_type):
    codec, error_msg = codec_info
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} (id BIGINT, data BIGINT) USING ICEBERG ".format(table) + \
                  "TBLPROPERTIES('write.parquet.compression-codec' = '{}')".format(codec))
        spark.sql("INSERT INTO {} SELECT * FROM {}".format(table, tmpview))
    with_cpu_session(setup_iceberg_table)
    query = "SELECT * FROM {}".format(table)
    read_conf = {'spark.rapids.sql.format.parquet.reader.type': reader_type}
    if error_msg:
        assert_spark_exception(
            lambda : with_gpu_session(lambda spark : spark.sql(query).collect(), conf=read_conf),
            error_msg)
    else:
        assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.sql(query), conf=read_conf)

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("key_gen", [int_gen, long_gen, string_gen, boolean_gen, date_gen, timestamp_gen, decimal_gen_64bit], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_partition_key(spark_tmp_table_factory, key_gen, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, key_gen, long_gen).orderBy("a")
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG PARTITIONED BY (a) ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT a FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_input_meta(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen).orderBy("a")
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG PARTITIONED BY (a) ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(
            "SELECT a, input_file_name(), input_file_block_start(), input_file_block_length() " + \
            "FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_disorder_read_schema(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = three_col_df(spark, long_gen, string_gen, float_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT b,c,a FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
def test_iceberg_read_appended_table(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.sql("SELECT * FROM {}".format(table)))

@iceberg
# Some metadata files have types that are not supported on the GPU yet (e.g.: BinaryType)
@allow_non_gpu("BatchScanExec", "ProjectExec")
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
def test_iceberg_read_metadata_fallback(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    for subtable in ["all_data_files", "all_manifests", "files", "history",
                     "manifests", "partitions", "snapshots"]:
        # SQL does not have syntax to read table metadata
        assert_gpu_fallback_collect(
            lambda spark : spark.read.format("iceberg").load("default.{}.{}".format(table, subtable)),
            "BatchScanExec")

@iceberg
# Some metadata files have types that are not supported on the GPU yet (e.g.: BinaryType)
@allow_non_gpu("BatchScanExec", "ProjectExec")
def test_iceberg_read_metadata_count(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    for subtable in ["all_data_files", "all_manifests", "files", "history",
                     "manifests", "partitions", "snapshots"]:
        # SQL does not have syntax to read table metadata
        assert_gpu_and_cpu_row_counts_equal(
            lambda spark : spark.read.format("iceberg").load("default.{}.{}".format(table, subtable)))

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.skipif(is_before_spark_320(), reason="Spark 3.1.x has a catalog bug precluding scope prefix in table names")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_timetravel(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_snapshots(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
        return spark.sql("SELECT snapshot_id FROM default.{}.snapshots ".format(table) + \
                         "ORDER BY committed_at").head()[0]
    first_snapshot_id = with_cpu_session(setup_snapshots)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.option("snapshot-id", first_snapshot_id) \
            .format("iceberg").load("default.{}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.skipif(is_before_spark_320(), reason="Spark 3.1.x has a catalog bug precluding scope prefix in table names")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_incremental_read(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_snapshots(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=2)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
        return spark.sql("SELECT snapshot_id FROM default.{}.snapshots ".format(table) + \
                         "ORDER BY committed_at").collect()
    snapshots = with_cpu_session(setup_snapshots)
    start_snapshot, end_snapshot = [ row[0] for row in snapshots[:2] ]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .format("iceberg").load("default.{}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_reorder_columns(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ALTER COLUMN b FIRST".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_rename_column(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} RENAME COLUMN a TO c".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_column_names_swapped(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} RENAME COLUMN a TO c".format(table))
        spark.sql("ALTER TABLE {} RENAME COLUMN b TO a".format(table))
        spark.sql("ALTER TABLE {} RENAME COLUMN c TO b".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_alter_column_type(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = three_col_df(spark, int_gen, float_gen, DecimalGen(precision=7, scale=3))
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ALTER COLUMN a TYPE BIGINT".format(table))
        spark.sql("ALTER TABLE {} ALTER COLUMN b TYPE DOUBLE".format(table))
        spark.sql("ALTER TABLE {} ALTER COLUMN c TYPE DECIMAL(17, 3)".format(table))
        df = three_col_df(spark, long_gen, double_gen, DecimalGen(precision=17, scale=3))
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_add_column(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ADD COLUMNS (c DOUBLE)".format(table))
        df = three_col_df(spark, long_gen, long_gen, double_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_remove_column(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} DROP COLUMN a".format(table))
        df = unary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_add_partition_field(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ADD PARTITION FIELD b".format(table))
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {} ORDER BY b".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_drop_partition_field(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} (a INT, b INT) USING ICEBERG PARTITIONED BY (b)".format(table))
        spark.sql("INSERT INTO {} SELECT * FROM {} ORDER BY b".format(table, tmpview))
        spark.sql("ALTER TABLE {} DROP PARTITION FIELD b".format(table))
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v1_delete(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("DELETE FROM {} WHERE a < 0".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@pytest.mark.skipif(is_before_spark_320(), reason="merge-on-read not supported on Spark 3.1.x")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v2_delete_unsupported(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  "TBLPROPERTIES('format-version' = 2, 'write.delete.mode' = 'merge-on-read') " + \
                  "AS SELECT * FROM {}".format(tmpview))
        spark.sql("DELETE FROM {} WHERE a < 0".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_spark_exception(
        lambda : with_gpu_session(
            lambda spark : spark.sql("SELECT * FROM {}".format(table)).collect(),
            conf={'spark.rapids.sql.format.parquet.reader.type': reader_type}),
        "UnsupportedOperationException: Delete filter is not supported")


@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_with_input_file(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG AS SELECT * FROM {}".format(table, tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT *, input_file_name() FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_parquet_read_from_url_encoded_path(spark_tmp_table_factory, reader_type):
    table = spark_tmp_table_factory.get()
    tmp_view = spark_tmp_table_factory.get()
    partition_gen = StringGen(pattern="(.|\n){1,10}", nullable=False)\
        .with_special_case('%29%3EtkiudF4%3C', 1000)\
        .with_special_case('%2F%23_v9kRtI%27', 1000)\
        .with_special_case('aK%2BAgI%21l8%3E', 1000)\
        .with_special_case('p%2Cmtx%3FCXMd', 1000)
    def setup_iceberg_table(spark):
        df = two_col_df(spark, long_gen, partition_gen).sortWithinPartitions('b')
        df.createOrReplaceTempView(tmp_view)
        spark.sql("CREATE TABLE {} USING ICEBERG PARTITIONED BY (b) AS SELECT * FROM {}"
                  .format(table, tmp_view))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})
