# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write, assert_py4j_exception
from datetime import date, datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session
import pyspark.sql.functions as f
import pyspark.sql.utils
import random
from spark_session import is_before_spark_311

pytestmark = pytest.mark.nightly_resource_consuming_test

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
        coalesce_parquet_file_reader_conf]
parquet_decimal_struct_gen= StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(decimal_gens)])
writer_confs={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
              'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED'}


def limited_timestamp(nullable=True):
    return TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc),
                        nullable=nullable)

# TODO - https://github.com/NVIDIA/spark-rapids/issues/1130 to handle TIMESTAMP_MILLIS
# TODO - we are limiting the INT96 values, see https://github.com/rapidsai/cudf/issues/8070
def limited_int96():
    return TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))

parquet_basic_gen =[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                    string_gen, boolean_gen, date_gen,
                    # we are limiting TimestampGen to avoid overflowing the INT96 value
                    # see https://github.com/rapidsai/cudf/issues/8070
                    limited_timestamp()]

parquet_basic_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen,
    limited_timestamp]] + [simple_string_to_string_map_gen,
                           MapGen(DecimalGen(20, 2, nullable=False), decimal_gen_128bit)]

parquet_struct_gen_no_maps = [
    StructGen([['child' + str(ind), sub_gen] for ind, sub_gen in enumerate(parquet_basic_gen)]),
    StructGen([['child0', StructGen([['child1', byte_gen]])]])
]

parquet_struct_of_map_gen = StructGen([['child0', MapGen(StringGen(nullable=False), StringGen(), max_length=5)], ['child1', IntegerGen()]])

parquet_struct_gen = parquet_struct_gen_no_maps + [parquet_struct_of_map_gen]

parquet_array_gen = [ArrayGen(sub_gen, max_length=10) for sub_gen in parquet_basic_gen + parquet_struct_gen] + [
    ArrayGen(ArrayGen(sub_gen, max_length=10), max_length=10) for sub_gen in parquet_basic_gen + parquet_struct_gen_no_maps] + [
    ArrayGen(ArrayGen(parquet_struct_of_map_gen, max_length=4), max_length=4)]

parquet_map_gens_sample = parquet_basic_map_gens + [MapGen(StringGen(pattern='key_[0-9]', nullable=False),
                                                           ArrayGen(string_gen), max_length=10),
                                                    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen,
                                                           max_length=10),
                                                    MapGen(StringGen(pattern='key_[0-9]', nullable=False),
                                                           simple_string_to_string_map_gen)]

parquet_map_gens = parquet_map_gens_sample + [
    MapGen(StructGen([['child0', StringGen()], ['child1', StringGen()]], nullable=False), FloatGen()),
    MapGen(StructGen([['child0', StringGen(nullable=True)]], nullable=False), StringGen())]
parquet_write_gens_list = [parquet_basic_gen, decimal_gens] +  [ [single_gen] for single_gen in parquet_struct_gen + parquet_array_gen + parquet_map_gens]
parquet_ts_write_options = ['INT96', 'TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS']

@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
def test_write_round_trip(spark_tmp_path, parquet_gens):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

@pytest.mark.parametrize('parquet_gens', [[
    limited_timestamp(),
    ArrayGen(limited_timestamp(), max_length=10),
    MapGen(limited_timestamp(nullable=False), limited_timestamp())]], ids=idfn)
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
def test_timestamp_write_round_trip(spark_tmp_path, parquet_gens, ts_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(writer_confs, {'spark.sql.parquet.outputTimestampType': ts_type})
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=all_confs)

@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['CORRECTED'])
@ignore_order
def test_write_ts_millis(spark_tmp_path, ts_type, ts_rebase):
    # we are limiting TimestampGen to avoid overflowing the INT96 value
    # see https://github.com/rapidsai/cudf/issues/8070
    gen = TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
              'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
              'spark.sql.parquet.outputTimestampType': ts_type})


parquet_part_write_gens = [
    byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    # Some file systems have issues with UTF8 strings so to help the test pass even there
    StringGen('(\\w| ){0,50}'),
    boolean_gen, date_gen,
    # we are limiting TimestampGen to avoid overflowing the INT96 value
    # see https://github.com/rapidsai/cudf/issues/8070
    TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('parquet_gen', parquet_part_write_gens, ids=idfn)
def test_part_write_round_trip(spark_tmp_path, parquet_gen):
    gen_list = [('a', RepeatSeqGen(parquet_gen, 10)),
            ('b', parquet_gen)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

# we are limiting TimestampGen to avoid overflowing the INT96 value
# see https://github.com/rapidsai/cudf/issues/8070
@pytest.mark.parametrize('data_gen', [TimestampGen(end=datetime(1677, 9, 22, tzinfo=timezone.utc)),
                                      TimestampGen(start=datetime(2262, 4, 11, tzinfo=timezone.utc))], ids=idfn)
def test_catch_int96_overflow(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = copy_and_update(writer_confs, {'spark.sql.parquet.outputTimestampType': 'INT96'})
    assert_py4j_exception(lambda: with_gpu_session(
        lambda spark: unary_op_df(spark, data_gen).coalesce(1).write.parquet(data_path), conf=confs), "org.apache.spark.SparkException: Job aborted.")

@pytest.mark.parametrize('data_gen', [TimestampGen()], ids=idfn)
@pytest.mark.allow_non_gpu("DataWritingCommandExec")
def test_int96_write_conf(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = copy_and_update(writer_confs, {
        'spark.sql.parquet.outputTimestampType': 'INT96',
        'spark.rapids.sql.format.parquet.writer.int96.enabled': 'false'})
    with_gpu_session(lambda spark: unary_op_df(spark, data_gen).coalesce(1).write.parquet(data_path), conf=confs)

def test_all_null_int96(spark_tmp_path):
    class AllNullTimestampGen(TimestampGen):
        def start(self, rand):
            self._start(rand, lambda : None)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = copy_and_update(writer_confs, {'spark.sql.parquet.outputTimestampType': 'INT96'})
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path : unary_op_df(spark, AllNullTimestampGen()).coalesce(1).write.parquet(path),
        lambda spark, path : spark.read.parquet(path),
        data_path,
        conf=confs)

parquet_write_compress_options = ['none', 'uncompressed', 'snappy']
@pytest.mark.parametrize('compress', parquet_write_compress_options)
def test_compress_write_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = {'spark.sql.parquet.compression.codec': compress}
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.parquet(path),
            lambda spark, path : spark.read.parquet(path),
            data_path,
            conf=all_confs)

@pytest.mark.order(2)
@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
def test_write_save_table(spark_tmp_path, parquet_gens, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

def write_parquet_sql_from(spark, df, data_path, write_to_table):
    tmp_view_name = 'tmp_view_{}'.format(random.randint(0, 1000000))
    df.createOrReplaceTempView(tmp_view_name)
    write_cmd = 'CREATE TABLE `{}` USING PARQUET location \'{}\' AS SELECT * from `{}`'.format(write_to_table, data_path, tmp_view_name)
    spark.sql(write_cmd)

@pytest.mark.order(2)
@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
def test_write_sql_save_table(spark_tmp_path, parquet_gens, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: write_parquet_sql_from(spark, gen_df(spark, gen_list).coalesce(1), path, spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=writer_confs)

def writeParquetUpgradeCatchException(spark, df, data_path, spark_tmp_table_factory, int96_rebase, datetime_rebase, ts_write):
    spark.conf.set('spark.sql.parquet.outputTimestampType', ts_write)
    spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', datetime_rebase)
    spark.conf.set('spark.sql.legacy.parquet.int96RebaseModeInWrite', int96_rebase) # for spark 310
    with pytest.raises(Exception) as e_info:
        df.coalesce(1).write.format("parquet").mode('overwrite').option("path", data_path).saveAsTable(spark_tmp_table_factory.get())
    assert e_info.match(r".*SparkUpgradeException.*")

# TODO - https://github.com/NVIDIA/spark-rapids/issues/1130 to handle TIMESTAMP_MILLIS
# TODO - we are limiting the INT96 values, see https://github.com/rapidsai/cudf/issues/8070
@pytest.mark.parametrize('ts_write_data_gen', [('INT96', limited_int96()), ('TIMESTAMP_MICROS', TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc), end=datetime(1582, 1, 1, tzinfo=timezone.utc)))])
@pytest.mark.parametrize('rebase', ["CORRECTED","EXCEPTION"])
def test_ts_write_fails_datetime_exception(spark_tmp_path, ts_write_data_gen, spark_tmp_table_factory, rebase):
    ts_write, gen = ts_write_data_gen
    data_path = spark_tmp_path + '/PARQUET_DATA'
    int96_rebase = "EXCEPTION" if (ts_write == "INT96") else rebase
    date_time_rebase = "EXCEPTION" if (ts_write == "TIMESTAMP_MICROS") else rebase
    if is_before_spark_311() and ts_write == 'INT96':
        all_confs = {'spark.sql.parquet.outputTimestampType': ts_write}
        all_confs.update({'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': date_time_rebase,
                          'spark.sql.legacy.parquet.int96RebaseModeInWrite': int96_rebase})
        assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)
    else:
        with_gpu_session(
            lambda spark : writeParquetUpgradeCatchException(spark,
                                                             unary_op_df(spark, gen),
                                                             data_path,
                                                             spark_tmp_table_factory,
                                                             int96_rebase, date_time_rebase, ts_write))
        with_cpu_session(
            lambda spark: writeParquetUpgradeCatchException(spark,
                                                            unary_op_df(spark, gen), data_path,
                                                            spark_tmp_table_factory,
                                                            int96_rebase, date_time_rebase, ts_write))

def writeParquetNoOverwriteCatchException(spark, df, data_path, table_name):
    with pytest.raises(Exception) as e_info:
        df.coalesce(1).write.format("parquet").option("path", data_path).saveAsTable(table_name)
    assert e_info.match(r".*already exists.*")

def test_ts_write_twice_fails_exception(spark_tmp_path, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    table_name = spark_tmp_table_factory.get()
    with_gpu_session(
            lambda spark : unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", data_path).saveAsTable(table_name))
    with_gpu_session(
            lambda spark : writeParquetNoOverwriteCatchException(spark, unary_op_df(spark, gen), data_path, table_name))

@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['LEGACY'])
def test_parquet_write_legacy_fallback(spark_tmp_path, ts_write, ts_rebase, spark_tmp_table_factory):
    gen = TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
            'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
            'spark.sql.parquet.outputTimestampType': ts_write}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@allow_non_gpu('DataWritingCommandExec')
# note that others should fail as well but requires you to load the libraries for them
# 'lzo', 'brotli', 'lz4', 'zstd' should all fallback
@pytest.mark.parametrize('codec', ['gzip'])
def test_parquet_write_compression_fallback(spark_tmp_path, codec, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.compression.codec': codec}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@allow_non_gpu('DataWritingCommandExec')
def test_parquet_writeLegacyFormat_fallback(spark_tmp_path, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.writeLegacyFormat': 'true'}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
def test_buckets_write_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_fallback_write(
            lambda spark, path: spark.range(10e4).write.bucketBy(4, "id").sortBy("id").format('parquet').mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec')

# This test is testing how the parquet_writer will behave if column has a validity mask without having any nulls.
# There is no straight forward to do it besides creating a vector with nulls and then dropping nulls
# cudf will create a vector with a null_mask even though we have just filtered them
def test_write_map_nullable(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'

    def generate_map_with_empty_validity(spark, path):
        gen_data = StructGen([['number', IntegerGen()], ['word', LongGen()]], nullable=False)
        gen_df(spark, gen_data)
        df = gen_df(spark, gen_data)
        df_noNulls = df.filter("number is not null")
        df_map = df_noNulls.withColumn("map", f.create_map(["number", "word"])).drop("number").drop("word")
        df_map.coalesce(1).write.parquet(path)

    assert_gpu_and_cpu_writes_are_equal_collect(
            generate_map_with_empty_validity,
            lambda spark, path: spark.read.parquet(path),
            data_path)

@pytest.mark.parametrize('ts_write_data_gen', [('INT96', limited_int96()), ('TIMESTAMP_MICROS', TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc), end=datetime(1582, 1, 1, tzinfo=timezone.utc)))])
@pytest.mark.parametrize('date_time_rebase_write', ["CORRECTED"])
@pytest.mark.parametrize('date_time_rebase_read', ["EXCEPTION", "CORRECTED"])
@pytest.mark.parametrize('int96_rebase_write', ["CORRECTED"])
@pytest.mark.parametrize('int96_rebase_read', ["EXCEPTION", "CORRECTED"])
def test_roundtrip_with_rebase_values(spark_tmp_path, ts_write_data_gen, date_time_rebase_read,
                                       date_time_rebase_write, int96_rebase_read, int96_rebase_write):
    ts_write, gen = ts_write_data_gen
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = {'spark.sql.parquet.outputTimestampType': ts_write}
    all_confs.update({'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': date_time_rebase_write,
                      'spark.sql.legacy.parquet.int96RebaseModeInWrite': int96_rebase_write})
    all_confs.update({'spark.sql.legacy.parquet.datetimeRebaseModeInRead': date_time_rebase_read,
                      'spark.sql.legacy.parquet.int96RebaseModeInRead': int96_rebase_read})

    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=all_confs)

@pytest.mark.allow_non_gpu("DataWritingCommandExec", "HiveTableScanExec")
@pytest.mark.parametrize('allow_non_empty', [True, False])
def test_non_empty_ctas(spark_tmp_path, spark_tmp_table_factory, allow_non_empty):
    data_path = spark_tmp_path + "/CTAS"
    conf = {
        "spark.sql.hive.convertCTAS": "true",
        "spark.sql.legacy.allowNonEmptyLocationInCTAS": str(allow_non_empty)
    }
    def test_it(spark):
        src_name = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {}(id string) LOCATION '{}/src1'".format(src_name, data_path))
        spark.sql("INSERT INTO TABLE {} SELECT 'A'".format(src_name))
        ctas1_name = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE {}(id string) LOCATION '{}/ctas/ctas1'".format(ctas1_name, data_path))
        spark.sql("INSERT INTO TABLE {} SELECT 'A'".format(ctas1_name))
        try:
            ctas_with_existing_name = spark_tmp_table_factory.get()
            spark.sql("CREATE TABLE {} LOCATION '{}/ctas' AS SELECT * FROM {}".format(
                ctas_with_existing_name, data_path, src_name))
        except pyspark.sql.utils.AnalysisException as e:
            if allow_non_empty or e.desc.find('non-empty directory') == -1:
                raise e
    with_gpu_session(test_it, conf)

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
def test_write_empty_parquet_round_trip(spark_tmp_path, parquet_gens):
    def create_empty_df(spark, path):
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
        return gen_df(spark, gen_list, length=0).write.parquet(path)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        create_empty_df,
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf=writer_confs)
