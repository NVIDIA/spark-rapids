# Copyright (c) 2021, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_collect
from datetime import date, datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session

def read_parquet_df(data_path):
    return lambda spark : spark.read.parquet(data_path)

def read_parquet_sql(data_path):
    return lambda spark : spark.sql('select * from parquet.`{}`'.format(data_path))


# Override decimal_gens because decimal with negative scale is unsupported in parquet reading
decimal_gens = [DecimalGen(), DecimalGen(precision=7, scale=3), DecimalGen(precision=10, scale=10),
                DecimalGen(precision=9, scale=0), DecimalGen(precision=18, scale=15)]

parquet_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, date_gen,
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)), ArrayGen(byte_gen),
    ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
    ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
    ArrayGen(DecimalGen()),
    ArrayGen(ArrayGen(byte_gen)),
    StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen], ['child3', DecimalGen()]]),
    ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))] +
                     map_gens_sample + decimal_gens,
                     pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/132'))]

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
                    coalesce_parquet_file_reader_conf]

@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_read_round_trip(spark_tmp_path, parquet_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list, 'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path),
            conf=all_confs)

