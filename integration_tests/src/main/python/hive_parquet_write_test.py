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

import pytest

from asserts import assert_gpu_and_cpu_sql_writes_are_equal_collect
from data_gen import *
from hive_write_test import _restricted_timestamp
from marks import allow_non_gpu, ignore_order
from spark_session import with_cpu_session

# Disable the meta conversion from Hive write to FrameData write in Spark, to test
# "GpuInsertIntoHiveTable" for Parquet write.
_write_to_hive_conf = {"spark.sql.hive.convertMetastoreParquet": False}

_hive_basic_gens = [
    byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, string_gen, boolean_gen,
    DateGen(start=date(1590, 1, 1)), _restricted_timestamp(),
    DecimalGen(precision=19, scale=1, nullable=True),
    DecimalGen(precision=23, scale=5, nullable=True),
    DecimalGen(precision=36, scale=3, nullable=True)]

_hive_basic_struct_gen = StructGen(
    [['c'+str(ind), c_gen] for ind, c_gen in enumerate(_hive_basic_gens)])

_hive_struct_gens = [
    _hive_basic_struct_gen,
    StructGen([['child0', byte_gen], ['child1', _hive_basic_struct_gen]]),
    StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

_hive_array_gens = [ArrayGen(sub_gen) for sub_gen in _hive_basic_gens] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

_hive_map_gens = [simple_string_to_string_map_gen] + [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
    lambda nullable=True: _restricted_timestamp(nullable=nullable),
    lambda nullable=True: DateGen(start=date(1590, 1, 1), nullable=nullable),
    lambda nullable=True: DecimalGen(precision=19, scale=1, nullable=nullable),
    lambda nullable=True: DecimalGen(precision=36, scale=5, nullable=nullable)]]

_hive_write_gens = [_hive_basic_gens, _hive_struct_gens, _hive_array_gens, _hive_map_gens]


@allow_non_gpu('HiveTableScanExec', *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize("is_ctas", [True, False], ids=['CTAS', 'CTTW'])
@pytest.mark.parametrize("gens", _hive_write_gens, ids=idfn)
def test_write_parquet_into_hive_table(spark_tmp_table_factory, gens, is_ctas):
    # Generate hive table in Parquet format
    def gen_hive_table(spark):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list).createOrReplaceTempView(data_table)
        hive_table = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {} STORED AS PARQUET AS SELECT * FROM {}".format(
            hive_table, data_table))
        return hive_table

    input_table = with_cpu_session(gen_hive_table)

    def write_to_hive_sql(spark, output_table):
        if is_ctas:
            # Create Table As Select
            return [
                "CREATE TABLE {} STORED AS PARQUET AS SELECT * FROM {}".format(
                    output_table, input_table)
            ]
        else:
            # Create Table Then Write
            return [
                "CREATE TABLE {} LIKE {}".format(output_table, input_table),
                "INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(output_table, input_table)
            ]

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        write_to_hive_sql,
        _write_to_hive_conf)


@allow_non_gpu('HiveTableScanExec', *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize("is_static", [True, False], ids=['Static_Partition', 'Dynamic_Partition'])
def test_write_parquet_into_partitioned_hive_table(spark_tmp_table_factory, is_static):
    # Generate hive table in Parquet format
    def gen_hive_table(spark):
        # gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
        dates = [date(2024, 2, 28), date(2024, 2, 27), date(2024, 2, 26)]
        gen_list = [('a', int_gen),
                    ('b', long_gen),
                    ('c', short_gen),
                    ('d', string_gen),
                    ('part', SetValuesGen(DateType(), dates))]
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list).createOrReplaceTempView(data_table)
        hive_table = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {} STORED AS PARQUET AS SELECT * FROM {}".format(
            hive_table, data_table))
        return hive_table

    input_table = with_cpu_session(gen_hive_table)

    def partitioned_write_to_hive_sql(spark, output_table):
        sql_create_part_table = (
            "CREATE TABLE {} (a INT, b LONG, c SHORT, d STRING) "
            "PARTITIONED BY (part DATE) STORED AS PARQUET"
        ).format(output_table)
        if is_static:
            return [
                # sql_1: Create partitioned hive table
                sql_create_part_table,
                # sql_2: Static partition write only to partition 'par2'
                "INSERT OVERWRITE TABLE {} PARTITION (part='2024-02-25') "
                "SELECT a, b, c, d FROM {}".format(output_table, input_table)
            ]
        else:
            return [
                # sql_1: Create partitioned hive table
                sql_create_part_table,
                # sql_2: Dynamic partition write
                "INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(output_table, input_table)
            ]
    all_confs = copy_and_update(_write_to_hive_conf, {
        "hive.exec.dynamic.partition.mode": "nonstrict"})
    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        partitioned_write_to_hive_sql,
        all_confs)


@allow_non_gpu('HiveTableScanExec', *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize("comp_type", ['UNCOMPRESSED', 'ZSTD', 'SNAPPY'])
def test_write_compressed_parquet_into_hive_table(spark_tmp_table_factory, comp_type):
    # Generate hive table in Parquet format
    def gen_hive_table(spark):
        gens = _hive_basic_gens + _hive_struct_gens + _hive_array_gens + _hive_map_gens
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(gens)]
        data_table = spark_tmp_table_factory.get()
        gen_df(spark, gen_list).createOrReplaceTempView(data_table)
        hive_table = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {} STORED AS PARQUET AS SELECT * FROM {}".format(
            hive_table, data_table))
        return hive_table

    input_table = with_cpu_session(gen_hive_table)

    def write_to_hive_sql(spark, output_table):
        return [
            "CREATE TABLE {} LIKE {} TBLPROPERTIES ('parquet.compression'='{}')".format(
                output_table, input_table, comp_type),
            "INSERT OVERWRITE TABLE {} SELECT * FROM {}".format(output_table, input_table)
        ]

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        write_to_hive_sql,
        _write_to_hive_conf)