# Copyright (c) 2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_sql_writes_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from datetime import date, datetime, timezone
from marks import *
from spark_session import is_hive_available, is_spark_33X, is_spark_340_or_later, with_cpu_session

# Using timestamps from 1970 to work around a cudf ORC bug
# https://github.com/NVIDIA/spark-rapids/issues/140.
# Using a limited upper end for timestamps to avoid INT96 overflow on Parquet.
def _restricted_timestamp(nullable=True):
    return TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc),
                        end=datetime(2262, 4, 11, tzinfo=timezone.utc),
                        nullable=nullable)

_basic_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                     string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
                     _restricted_timestamp()
               ] + decimal_gens

_basic_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(_basic_gens)])

_struct_gens = [_basic_struct_gen,
                StructGen([['child0', byte_gen], ['child1', _basic_struct_gen]]),
                StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

_array_gens = [ArrayGen(sub_gen) for sub_gen in _basic_gens] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

_map_gens = [simple_string_to_string_map_gen] + [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
    lambda nullable=True: _restricted_timestamp(nullable=nullable),
    lambda nullable=True: DateGen(start=date(1590, 1, 1), nullable=nullable),
    lambda nullable=True: DecimalGen(precision=15, scale=1, nullable=nullable),
    lambda nullable=True: DecimalGen(precision=36, scale=5, nullable=nullable)]]

_write_gens = [_basic_gens, _struct_gens, _array_gens, _map_gens]

# There appears to be a race when computing tasks for writing, order can be different even on CPU
@ignore_order(local=True)
@pytest.mark.skipif(not is_hive_available(), reason="Hive is missing")
@pytest.mark.parametrize("gens", _write_gens, ids=idfn)
@pytest.mark.parametrize("storage", ["PARQUET", "nativeorc", "hiveorc"])
def test_optimized_hive_ctas_basic(gens, storage, spark_tmp_table_factory):
    data_table = spark_tmp_table_factory.get()
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(gens)]
    with_cpu_session(lambda spark: gen_df(spark, gen_list).createOrReplaceTempView(data_table))
    def do_write(spark, table_name):
        store_name = storage
        if storage.endswith("orc"):
            store_name = "ORC"
        return "CREATE TABLE {} STORED AS {} AS SELECT * FROM {}".format(
            table_name, store_name, data_table)
    conf = {
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED"
    }
    if storage == "nativeorc":
        conf["spark.sql.orc.impl"] = "native"
    elif storage == "hiveorc":
        conf["spark.sql.orc.impl"] = "hive"
    assert_gpu_and_cpu_sql_writes_are_equal_collect(spark_tmp_table_factory, do_write, conf=conf)

@allow_non_gpu("DataWritingCommandExec")
@pytest.mark.skipif(not is_hive_available(), reason="Hive is missing")
@pytest.mark.parametrize("gens", [_basic_gens], ids=idfn)
@pytest.mark.parametrize("storage_with_confs", [
    ("PARQUET", {"spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",
                 "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY"}),
    ("PARQUET", {"parquet.encryption.footer.key": "k1",
                 "parquet.encryption.column.keys": "k2:a"}),
    ("PARQUET", {"spark.sql.parquet.compression.codec": "gzip"}),
    ("PARQUET", {"spark.sql.parquet.writeLegacyFormat": "true"}),
    ("ORC", {"spark.sql.orc.compression.codec": "zlib"})], ids=idfn)
def test_optimized_hive_ctas_configs_fallback(gens, storage_with_confs, spark_tmp_table_factory):
    data_table = spark_tmp_table_factory.get()
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(gens)]
    with_cpu_session(lambda spark: gen_df(spark, gen_list).createOrReplaceTempView(data_table))
    storage, confs = storage_with_confs
    assert_gpu_fallback_collect(
        lambda spark: spark.sql("CREATE TABLE {} STORED AS {} AS SELECT * FROM {}".format(
            spark_tmp_table_factory.get(), storage, data_table)),
        "DataWritingCommandExec", conf=confs)

@allow_non_gpu("DataWritingCommandExec")
@pytest.mark.skipif(not is_hive_available(), reason="Hive is missing")
@pytest.mark.parametrize("gens", [_basic_gens], ids=idfn)
@pytest.mark.parametrize("storage_with_opts", [
    ("PARQUET", {"parquet.encryption.footer.key": "k1",
                 "parquet.encryption.column.keys": "k2:a"}),
    ("ORC", {"orc.compress": "zlib"})], ids=idfn)
def test_optimized_hive_ctas_options_fallback(gens, storage_with_opts, spark_tmp_table_factory):
    data_table = spark_tmp_table_factory.get()
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(gens)]
    with_cpu_session(lambda spark: gen_df(spark, gen_list).createOrReplaceTempView(data_table))
    storage, opts = storage_with_opts
    opts_string = ", ".join(["'{}'='{}'".format(k, v) for k, v in opts.items()])
    assert_gpu_fallback_collect(
        lambda spark: spark.sql("CREATE TABLE {} OPTIONS ({}) STORED AS {} AS SELECT * FROM {}".format(
            spark_tmp_table_factory.get(), opts_string, storage, data_table)),
        "DataWritingCommandExec")

@allow_non_gpu("DataWritingCommandExec")
@pytest.mark.skipif(not (is_hive_available() and is_spark_33X()),
                    reason="Requires Hive and Spark 3.3.X to write bucketed Hive tables")
@pytest.mark.parametrize("gens", [_basic_gens], ids=idfn)
@pytest.mark.parametrize("storage", ["PARQUET", "ORC"], ids=idfn)
def test_optimized_hive_bucketed_fallback_33X(gens, storage, spark_tmp_table_factory):
    in_table = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark: three_col_df(spark, int_gen, int_gen, int_gen).createOrReplaceTempView(in_table))
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(
            """CREATE TABLE {} STORED AS {}
            CLUSTERED BY (b) INTO 3 BUCKETS
            AS SELECT * FROM {}""".format(spark_tmp_table_factory.get(), storage, in_table)),
        "DataWritingCommandExec")

# Since Spark 3.4.0, the internal "SortExec" will be pulled out by default
# from the FileFormatWriter. Then it is visible in the planning stage.
@allow_non_gpu("DataWritingCommandExec", "SortExec", "HiveHash")
@pytest.mark.skipif(not (is_hive_available() and is_spark_340_or_later()),
                    reason="Requires Hive and Spark 3.4+ to write bucketed Hive tables with SortExec pulled out")
@pytest.mark.parametrize("gens", [_basic_gens], ids=idfn)
@pytest.mark.parametrize("storage", ["PARQUET", "ORC"], ids=idfn)
@pytest.mark.parametrize("planned_write", [True, False], ids=idfn)
def test_optimized_hive_bucketed_fallback(gens, storage, planned_write, spark_tmp_table_factory):
    in_table = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark: three_col_df(spark, int_gen, int_gen, int_gen).createOrReplaceTempView(in_table))
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(
            """CREATE TABLE {} STORED AS {}
            CLUSTERED BY (b) INTO 3 BUCKETS
            AS SELECT * FROM {}""".format(spark_tmp_table_factory.get(), storage, in_table)),
        "DataWritingCommandExec",
        {"spark.sql.optimizer.plannedWrite.enabled": planned_write})
