# Copyright (c) 2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from parquet_test import reader_opt_confs
from pyarrow_utils import *
from spark_session import with_gpu_session


def read_on_pyarrow(parquet_path):
    """
    Read parquet file with pyarrow API and convert pyarrow Table to CPU result
    :param parquet_path:  parquet file path
    :return: Spark Row List
    """
    # By default, pyarrow read timestamp column with precision ns instead of us,
    # Spark only supports us,
    # coerce_int96_timestamp_unit is used to read GPU generated timestamp column as us
    # refer to: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    # In pyspark: INT96 timestamps will be inferred as timestamps in nanoseconds
    #
    pyarrow_table = pa_pq.read_table(parquet_path, coerce_int96_timestamp_unit="us")
    # convert pyarrow table to CPU result
    return convert_pyarrow_table_to_cpu_object(pyarrow_table)

def read_parquet(data_path):
    """
    (Fetches a function that) Reads Parquet from the specified `data_path`.
    If the plugin is enabled, the read is done via Spark APIs, through the plugin.
    If the plugin is disabled, the data is read via `pyarrow`.
    :param data_path: Location of the (single) Parquet input file.
    :return: A function that reads Parquet, via the plugin or `pyarrow`.
    """

    def read_with_pyarrow_or_plugin(spark):
        plugin_enabled = spark.conf.get("spark.rapids.sql.enabled", "false") == "true"
        if plugin_enabled:
            # read on GPU
            return spark.read.parquet(data_path)
        else:
            # read on pyarrow
            rows = read_on_pyarrow(data_path)
            # get the schema
            schema = spark.read.parquet(data_path).schema
            # create data frame to wrap Row list
            # should pass schema here to avoid complex type inference error 
            return spark.createDataFrame(rows, schema=schema)

    return read_with_pyarrow_or_plugin

def assert_gpu_and_pyarrow_are_compatible(base_write_path, gen_list, conf={}):
    """
    Test scenarios:
      Write by pyarrow, test readings on pyarrow and GPU
      Write by GPU, test readings on pyarrow and GPU
    The data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    pyarrow_parquet_path = base_write_path + "/pyarrow"
    gpu_parquet_path = base_write_path + "/gpu"

    def write_on_pyarrow():
        pa_table = gen_pyarrow_table(gen_list)
        pa_pq.write_table(pa_table, pyarrow_parquet_path)

    def write_on_gpu():
        with_gpu_session(lambda spark: gen_df(spark, gen_list).coalesce(1).write.parquet(gpu_parquet_path), conf=conf)

    # write on pyarrow, compare reads between pyarrow and GPU
    write_on_pyarrow()
    assert_gpu_and_cpu_are_equal_collect(read_parquet(pyarrow_parquet_path), conf=conf)

    # write on GPU, compare reads between pyarrow and GPU
    write_on_gpu()
    assert_gpu_and_cpu_are_equal_collect(read_parquet(gpu_parquet_path), conf=conf)


# types for test_parquet_read_round_trip_for_pyarrow
sub_gens = all_basic_gens_no_null + [decimal_gen_64bit, decimal_gen_128bit]

struct_gen = StructGen([('child_' + str(i), sub_gens[i]) for i in range(len(sub_gens))])
array_gens = [ArrayGen(sub_gen) for sub_gen in sub_gens]
parquet_gens_list = [
    [binary_gen],
    sub_gens,
    [struct_gen],
    single_level_array_gens_no_null,
    map_gens_sample,
]


#
# test read/write by pyarrow/GPU
#
@ignore_order(local=True)
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_round_trip_for_pyarrow(
        spark_tmp_path,
        parquet_gens,
        reader_confs,
        v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',

        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.datetimeRebaseModeInRead': 'CORRECTED'})

    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_pyarrow_are_compatible(spark_tmp_path, gen_list, conf=all_confs)
