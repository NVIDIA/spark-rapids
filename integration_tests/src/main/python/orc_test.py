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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_collect_with_capture
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, is_before_spark_320, is_before_spark_330
from parquet_test import _nested_pruning_schemas
from conftest import is_databricks_runtime

pytestmark = pytest.mark.nightly_resource_consuming_test

def read_orc_df(data_path):
    return lambda spark : spark.read.orc(data_path)

def read_orc_sql(data_path):
    return lambda spark : spark.sql('select * from orc.`{}`'.format(data_path))

# test with original orc file reader, the multi-file parallel reader for cloud
original_orc_file_reader_conf = {'spark.rapids.sql.format.orc.reader.type': 'PERFILE'}
multithreaded_orc_file_reader_conf = {'spark.rapids.sql.format.orc.reader.type': 'MULTITHREADED'}
coalescing_orc_file_reader_conf = {'spark.rapids.sql.format.orc.reader.type': 'COALESCING'}
reader_opt_confs = [original_orc_file_reader_conf, multithreaded_orc_file_reader_conf, coalescing_orc_file_reader_conf]

@pytest.mark.parametrize('name', ['timestamp-date-test.orc'])
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_basic_read(std_input_path, name, read_func, v1_enabled_list, orc_impl, reader_confs):
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.orc.impl': orc_impl})
    assert_gpu_and_cpu_are_equal_collect(
            read_func(std_input_path + '/' + name),
            conf=all_confs)

# ORC does not support negative scale for decimal. So here is "decimal_gens_no_neg".
# Otherwsie it will get the below exception.
# ...
#E                   Caused by: java.lang.IllegalArgumentException: Missing integer at
#   'struct<`_c0`:decimal(7,^-3),`_c1`:decimal(7,3),`_c2`:decimal(7,7),`_c3`:decimal(12,2)>'
#E                   	at org.apache.orc.TypeDescription.parseInt(TypeDescription.java:244)
#E                   	at org.apache.orc.TypeDescription.parseType(TypeDescription.java:362)
# ...
orc_basic_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))
                  ] + decimal_gens

orc_basic_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(orc_basic_gens)])

# Some array gens, but not all because of nesting
orc_array_gens_sample = [ArrayGen(sub_gen) for sub_gen in orc_basic_gens] + [
    ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
    ArrayGen(ArrayGen(decimal_gen_64bit, max_length=10), max_length=10),
    ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

# Some struct gens, but not all because of nesting.
# No empty struct gen because it leads to an error as below.
#   '''
#     E               pyspark.sql.utils.AnalysisException:
#     E               Datasource does not support writing empty or nested empty schemas.
#     E               Please make sure the data schema has at least one or more column(s).
#   '''
orc_struct_gens_sample = [orc_basic_struct_gen,
    StructGen([['child0', byte_gen], ['child1', orc_basic_struct_gen]]),
    StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

orc_basic_map_gens = [simple_string_to_string_map_gen] + [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
    lambda nullable=True: TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc), nullable=nullable),
    lambda nullable=True: DateGen(start=date(1590, 1, 1), nullable=nullable),
    lambda nullable=True: DecimalGen(precision=15, scale=1, nullable=nullable),
    lambda nullable=True: DecimalGen(precision=36, scale=5, nullable=nullable)]]

# Some map gens, but not all because of nesting
orc_map_gens_sample = orc_basic_map_gens + [
    MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(decimal_gen_128bit), max_length=10),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False),
           ArrayGen(StructGen([["c0", decimal_gen_64bit], ["c1", decimal_gen_128bit]])), max_length=10),
    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen),
    MapGen(StructGen([['child0', byte_gen], ['child1', long_gen]], nullable=False),
           StructGen([['child0', byte_gen], ['child1', long_gen]]))]

orc_gens_list = [orc_basic_gens,
    orc_array_gens_sample,
    orc_struct_gens_sample,
    orc_map_gens_sample,
    pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/131')),
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/131'))]

flattened_orc_gens = orc_basic_gens + orc_array_gens_sample + orc_struct_gens_sample

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.orc.enabled', 'spark.rapids.sql.format.orc.read.enabled'])
def test_orc_fallback(spark_tmp_path, read_func, disable_conf):
    data_gens =[string_gen,
        byte_gen, short_gen, int_gen, long_gen, boolean_gen]
 
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/ORC_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.orc(data_path))
    assert_gpu_fallback_collect(
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2') + f.col('_c3')),
            'FileSourceScanExec',
            conf={disable_conf: 'false',
                "spark.sql.sources.useV1SourceList": "orc"})

@pytest.mark.order(2)
@pytest.mark.parametrize('orc_gens', orc_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_read_round_trip(spark_tmp_path, orc_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            read_func(data_path),
            conf=all_confs)

orc_pred_push_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, boolean_gen,
        string_gen,
        # Once https://github.com/NVIDIA/spark-rapids/issues/139 is fixed replace this with
        # date_gen
        DateGen(start=date(1590, 1, 1)),
        # Once https://github.com/NVIDIA/spark-rapids/issues/140 is fixed replace this with
        # timestamp_gen 
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc))]

@pytest.mark.order(2)
@pytest.mark.parametrize('orc_gen', orc_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_pred_push_round_trip(spark_tmp_path, orc_gen, read_func, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/ORC_DATA'
    # Append two struct columns to verify nested predicate pushdown.
    gen_list = [('a', RepeatSeqGen(orc_gen, 100)), ('b', orc_gen),
        ('s1', StructGen([['sa', orc_gen]])),
        ('s2', StructGen([['sa', StructGen([['ssa', orc_gen]])]]))]
    s0 = gen_scalar(orc_gen, force_no_nulls=True)
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.orc(data_path))
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    rf = read_func(data_path)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: rf(spark).select(f.col('a') >= s0, f.col('s1.sa') >= s0, f.col('s2.sa.ssa') >= s0),
            conf=all_confs)

orc_compress_options = ['none', 'uncompressed', 'snappy', 'zlib']
# zstd is available in spark 3.2.0 and later.
if not is_before_spark_320():
    orc_compress_options.append('zstd')

# The following need extra jars 'lzo'
# https://github.com/NVIDIA/spark-rapids/issues/143

# Test the different compress combinations
@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_mixed_compress_read(spark_tmp_path, v1_enabled_list, reader_confs):
    data_pathes = []
    for compress in orc_compress_options:
        data_path = spark_tmp_path + '/ORC_DATA' + compress
        with_cpu_session(
                lambda spark : binary_op_df(spark, long_gen).write.orc(data_path),
                conf={'spark.sql.orc.compression.codec': compress})
        data_pathes.append(data_path)

    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_pathes),
            conf=all_confs)

@pytest.mark.parametrize('compress', orc_compress_options)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_compress_read_round_trip(spark_tmp_path, compress, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : binary_op_df(spark, long_gen).write.orc(data_path),
            conf={'spark.sql.orc.compression.codec': compress})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path),
            conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/131 is fixed
    # we should go with a more standard set of generators
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0/key2=20'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1/key2=21'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/ORC_DATA/key=2/key2=22'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(third_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path),
            conf=all_confs)

# Setup external table by altering column names
def setup_external_table_with_forced_positions(spark, table_name, data_path):
    rename_cols_query = "CREATE EXTERNAL TABLE `{}` (`col10` INT, `_c1` STRING, `col30` DOUBLE) STORED AS orc LOCATION '{}'".format(table_name, data_path)
    spark.sql(rename_cols_query).collect

@pytest.mark.skipif(is_before_spark_320(), reason='ORC forced positional evolution support is added in Spark-3.2')
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('forced_position', ["true", "false"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_orc_forced_position(spark_tmp_path, spark_tmp_table_factory, reader_confs, forced_position, orc_impl):
    orc_gens = [int_gen, string_gen, double_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + 'ORC_DATA'
    with_cpu_session(lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_external_table_with_forced_positions(spark, table_name, data_path))

    all_confs = copy_and_update(reader_confs, {
        'orc.force.positional.evolution': forced_position,
        'spark.sql.orc.impl': orc_impl})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table_name)),
        conf=all_confs)

# In this we are reading the data, but only reading the key the data was partitioned by
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_partitioned_read_just_partitions(spark_tmp_path, v1_enabled_list, reader_confs):
    orc_gens = [byte_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path).select("key"),
            conf=all_confs)

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/135')
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_merge_schema_read(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/131 is fixed
    # we should go with a more standard set of generators
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list).write.orc(first_data_path))
    second_gen_list = [(('_c' if i % 2 == 0 else '_b') + str(i), gen) for i, gen in enumerate(orc_gens)]
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.option('mergeSchema', 'true').orc(data_path),
            conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_read_orc_with_empty_clipped_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', int_gen)], length=100).write.orc(data_path))
    schema = StructType([StructField('b', IntegerType()), StructField('c', StringType())])
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).orc(data_path), conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_orc_read_multiple_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    first_gen_list = [('a', int_gen), ('b', int_gen)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
        lambda spark: gen_df(spark, first_gen_list, num_slices=10).write.orc(first_data_path))
    second_gen_list = [('c', int_gen), ('b', int_gen), ('a', int_gen)]
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
        lambda spark: gen_df(spark, second_gen_list, num_slices=10).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    read_schema = StructType([StructField("b", IntegerType()),
                              StructField("a", IntegerType()),
                              StructField("c", IntegerType())])
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).orc(data_path),
        conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_input_meta(spark_tmp_path, v1_enabled_list, reader_confs):
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=all_confs)

@allow_non_gpu('ProjectExec', 'Alias', 'InputFileName', 'InputFileBlockStart', 'InputFileBlockLength',
               'FilterExec', 'And', 'IsNotNull', 'GreaterThan', 'Literal',
               'FileSourceScanExec', 'ColumnarToRowExec',
               'BatchScanExec', 'OrcScan')
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.orc.enabled', 'spark.rapids.sql.format.orc.read.enabled'])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_input_meta_fallback(spark_tmp_path, v1_enabled_list, reader_confs, disable_conf):
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        disable_conf: 'false'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=all_confs)

def setup_orc_file_no_column_names(spark, table_name):
    drop_query = "DROP TABLE IF EXISTS {}".format(table_name)
    create_query = "CREATE TABLE `{}` (`_col1` INT, `_col2` STRING, `_col3` INT) USING orc".format(table_name)
    insert_query = "INSERT INTO {} VALUES(13, '155', 2020)".format(table_name)
    spark.sql(drop_query).collect
    spark.sql(create_query).collect
    spark.sql(insert_query).collect

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_missing_column_names(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT _col3,_col2 FROM {}".format(table_name)),
        reader_confs)

def setup_orc_file_with_column_names(spark, table_name):
    drop_query = "DROP TABLE IF EXISTS {}".format(table_name)
    create_query = "CREATE TABLE `{}` (`c_1` INT, `c_2` STRING, `c_3` ARRAY<INT>) USING orc".format(table_name)
    insert_query = "INSERT INTO {} VALUES(13, '155', array(2020))".format(table_name)
    spark.sql(drop_query).collect
    spark.sql(create_query).collect
    spark.sql(insert_query).collect

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_disorder_read_schema(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_with_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_2,c_1 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_1 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_2 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_1,c_3,c_2 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_1,c_2,c_3 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_2,c_1,c_3 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_2,c_3,c_1 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_1,c_2 FROM {}".format(table_name)),
        reader_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c_3,c_2,c_1 FROM {}".format(table_name)),
        reader_confs)


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_missing_column_names_filter(spark_tmp_table_factory, reader_confs):
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_orc_file_no_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT _col3,_col2 FROM {} WHERE _col2 = '155'".format(table_name)),
        reader_confs)


@pytest.mark.parametrize('data_gen,read_schema', _nested_pruning_schemas, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('nested_enabled', ["true", "false"])
def test_read_nested_pruning(spark_tmp_path, data_gen, read_schema, reader_confs, v1_enabled_list, nested_enabled):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, data_gen).write.orc(data_path))
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.optimizer.nestedSchemaPruning.enabled': nested_enabled})
    # This is a hack to get the type in a slightly less verbose way
    rs = StructGen(read_schema, nullable=False).data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(rs).orc(data_path),
            conf=all_confs)


# This is for the corner case of reading only a struct column that has no nulls.
# Then there will be no streams in a stripe connecting to this column (Its ROW_INDEX
# streams have been pruned by the Plugin.), and CUDF throws an exception for such case.
# Here is the tracking issue: 'https://github.com/rapidsai/cudf/issues/8878'. But it has
# been fixed. Still keep the test here to have this corner case tested.
def test_read_struct_without_stream(spark_tmp_path):
    data_gen = StructGen([['c_byte', ByteGen(nullable=False)]], nullable=False)
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen, 10).write.orc(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path))


@pytest.mark.parametrize('orc_gen', flattened_orc_gens, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('case_sensitive', ["false", "true"])
def test_read_with_more_columns(spark_tmp_path, orc_gen, reader_confs, v1_enabled_list, case_sensitive):
    struct_gen = StructGen([('nested_col', orc_gen)])
    # Map is not supported yet.
    gen_list = [("top_pri", orc_gen),
                ("top_st", struct_gen),
                ("top_ar", ArrayGen(struct_gen, max_length=10))]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.caseSensitive': case_sensitive})
    # This is a hack to get the type in a slightly less verbose way
    extra_struct_gen = StructGen([('nested_col', orc_gen), ("nested_non_existing", orc_gen)])
    extra_gen_list = [("top_pri", orc_gen),
                      ("top_non_existing_mid", orc_gen),
                      ("TOP_AR", ArrayGen(extra_struct_gen, max_length=10)),
                      ("top_ST", extra_struct_gen),
                      ("top_non_existing_end", orc_gen)]
    rs = StructGen(extra_gen_list, nullable=False).data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(rs).orc(data_path),
            conf=all_confs)

@pytest.mark.skipif(is_before_spark_330(), reason='Hidden file metadata columns are a new feature of Spark 330')
@allow_non_gpu(any = True)
@pytest.mark.parametrize('metadata_column', ["file_path", "file_name", "file_size", "file_modification_time"])
def test_orc_scan_with_hidden_metadata_fallback(spark_tmp_path, metadata_column):
    data_path = spark_tmp_path + "/hidden_metadata.orc"
    with_cpu_session(lambda spark : spark.range(10) \
                     .selectExpr("id", "id % 3 as p") \
                     .write \
                     .partitionBy("p") \
                     .mode("overwrite") \
                     .orc(data_path))

    def do_orc_scan(spark):
        df = spark.read.orc(data_path).selectExpr("id", "_metadata.{}".format(metadata_column))
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_orc_scan,
        exist_classes= "FileSourceScanExec",
        non_exist_classes= "GpuBatchScanExec")


@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
def test_orc_read_with_corrupt_files(spark_tmp_path, reader_confs, v1_enabled_list):
    first_data_path = spark_tmp_path + '/ORC_DATA/first'
    with_cpu_session(lambda spark : spark.range(1).toDF("a").write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/second'
    with_cpu_session(lambda spark : spark.range(1, 2).toDF("a").write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/ORC_DATA/third'
    with_cpu_session(lambda spark : spark.range(2, 3).toDF("a").write.json(third_data_path))

    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.files.ignoreCorruptFiles': "true",
                                 'spark.sql.sources.useV1SourceList': v1_enabled_list})

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc([first_data_path, second_data_path, third_data_path]),
            conf=all_confs)

# Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
# Note that Min/Max don't push down partition column. Only Count does.
# The following tests that GPU falls back to CPU when aggregates are pushed down on ORC.
#
# When the spark configuration is enabled we check the following:
# ----------------------------------------------+
# | Aggregate | Partition Column | FallBack CPU |
# +-----------+------------------+--------------+
# |   COUNT   |        Y         |      Y       |
# |    MIN    |        Y         |      N       |
# |    MAX    |        Y         |      N       |
# |   COUNT   |        N         |      Y       |
# |    MIN    |        N         |      Y       |
# |    MAX    |        N         |      Y       |

_aggregate_orc_list_col_partition = ['COUNT']
_aggregate_orc_list_no_col_partition = ['MAX', 'MIN']
_aggregate_orc_list = _aggregate_orc_list_col_partition + _aggregate_orc_list_no_col_partition
_orc_aggregate_pushdown_enabled_conf = {'spark.rapids.sql.format.orc.write.enabled': 'true',
                                        'spark.sql.orc.aggregatePushdown': 'true',
                                        "spark.sql.sources.useV1SourceList": ""}

def _do_orc_scan_with_agg(spark, path, agg):
    spark.range(10).selectExpr("id", "id % 3 as p").write.mode("overwrite").orc(path)
    return spark.read.orc(path).selectExpr('{}(p)'.format(agg))

def _do_orc_scan_with_agg_on_partitioned_column(spark, path, agg):
    spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").mode("overwrite").orc(path)
    return spark.read.orc(path).selectExpr('{}(p)'.format(agg))

@pytest.mark.skipif(is_before_spark_330(), reason='Aggregate push down on ORC is a new feature of Spark 330')
@pytest.mark.parametrize('aggregate', _aggregate_orc_list)
@allow_non_gpu(any = True)
def test_orc_scan_with_aggregate_pushdown(spark_tmp_path, aggregate):
    """
    Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
    When the spark configuration is enabled we check the following:
    ---------------------------+
    | Aggregate | FallBack CPU |
    +-----------+--------------+
    |   COUNT   |      Y       |
    |    MIN    |      Y       |
    |    MAX    |      Y       |
    """
    data_path = spark_tmp_path + '/ORC_DATA/pushdown_00.orc'

    # fallback to CPU
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _do_orc_scan_with_agg(spark, data_path, aggregate),
        exist_classes="BatchScanExec",
        non_exist_classes="GpuBatchScanExec",
        conf=_orc_aggregate_pushdown_enabled_conf)

@pytest.mark.skipif(is_before_spark_330(), reason='Aggregate push down on ORC is a new feature of Spark 330')
@pytest.mark.parametrize('aggregate', _aggregate_orc_list_col_partition)
@allow_non_gpu(any = True)
def test_orc_scan_with_aggregate_pushdown_on_col_partition(spark_tmp_path, aggregate):
    """
    Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
    Note that Min/Max don't push down partition column. Only Count does.
    This test checks that GPU falls back to CPU when aggregates are pushed down on ORC.
    When the spark configuration is enabled we check the following:
    ----------------------------------------------+
    | Aggregate | Partition Column | FallBack CPU |
    +-----------+------------------+--------------+
    |   COUNT   |        Y         |      Y       |
    """
    data_path = spark_tmp_path + '/ORC_DATA/pushdown_01.orc'

    # fallback to CPU only if aggregate is COUNT
    assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: _do_orc_scan_with_agg_on_partitioned_column(spark, data_path, aggregate),
            exist_classes="BatchScanExec",
            non_exist_classes="GpuBatchScanExec",
            conf=_orc_aggregate_pushdown_enabled_conf)

@pytest.mark.skipif(is_before_spark_330(), reason='Aggregate push down on ORC is a new feature of Spark 330')
@pytest.mark.parametrize('aggregate', _aggregate_orc_list_no_col_partition)
def test_orc_scan_with_aggregate_no_pushdown_on_col_partition(spark_tmp_path, aggregate):
    """
    Spark(330,_) allows aggregate pushdown on ORC by enabling spark.sql.orc.aggregatePushdown.
    Note that Min/Max don't push down partition column.
    When the spark configuration is enabled we check the following:
    ----------------------------------------------+
    | Aggregate | Partition Column | FallBack CPU |
    +-----------+------------------+--------------+
    |    MIN    |        Y         |      N       |
    |    MAX    |        Y         |      N       |
    """
    data_path = spark_tmp_path + '/ORC_DATA/pushdown_02.orc'
    
    # should not fallback to CPU
    assert_gpu_and_cpu_are_equal_collect(
                lambda spark: _do_orc_scan_with_agg_on_partitioned_column(spark, data_path, aggregate),
                conf=_orc_aggregate_pushdown_enabled_conf)


@pytest.mark.parametrize('offset', [1,2,3,4], ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_read_type_casting_integral(spark_tmp_path, offset, reader_confs, v1_enabled_list):
    int_gens = [boolean_gen] + integral_gens
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(int_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.orc(data_path))

    # build the read schema by a left shift of int_gens
    shifted_int_gens = int_gens[offset:] + int_gens[:offset]
    rs_gen_list = [('c' + str(i), gen) for i, gen in enumerate(shifted_int_gens)]
    rs = StructGen(rs_gen_list, nullable=False).data_type
    all_confs = copy_and_update(reader_confs,
        {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(rs).orc(data_path),
        conf=all_confs)
