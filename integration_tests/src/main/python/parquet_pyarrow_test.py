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
import difflib
import pyarrow as pa
import pyarrow.parquet as pa_pq
import pytest
import sys
import time

from asserts import _prep_incompat_conf, _RowCmp, _assert_equal
from conftest import get_float_check
from data_gen import *
from parquet_test import reader_opt_confs
from spark_session import with_gpu_session


def _get_pa_type(data_gen):
    """Map from data gen to pyarrow data type"""
    if isinstance(data_gen, NullGen):
        return pa.null()
    elif isinstance(data_gen, BooleanGen):
        return pa.bool_()
    elif isinstance(data_gen, ByteGen):
        return pa.int8()
    elif isinstance(data_gen, ShortGen):
        return pa.int16()
    elif isinstance(data_gen, IntegerGen):
        return pa.int32()
    elif isinstance(data_gen, LongGen):
        return pa.int64()
    elif isinstance(data_gen, FloatGen):
        return pa.float32()
    elif isinstance(data_gen, DoubleGen):
        return pa.float64()
    elif isinstance(data_gen, DateGen):
        return pa.date32()
    elif isinstance(data_gen, TimestampGen):
        return pa.timestamp('us')
    elif isinstance(data_gen, BinaryGen):
        return pa.binary()
    elif isinstance(data_gen, StringGen):
        return pa.string()
    elif isinstance(data_gen, DecimalGen):
        return pa.decimal128(data_gen.precision, data_gen.scale)
    elif isinstance(data_gen, StructGen):
        fields = [pa.field(name, _get_pa_type(child)) for name, child in data_gen.children]
        return pa.struct(fields)
    elif isinstance(data_gen, ArrayGen):
        return pa.list_(_get_pa_type(data_gen._child_gen))
    elif isinstance(data_gen, MapGen):
        return pa.map_(_get_pa_type(data_gen._key_gen), _get_pa_type(data_gen._value_gen))
    else:
        raise Exception("unexpected data_gen: " + str(data_gen))


def _gen_pyarrow_table(data_gen, length=2048, seed=0):
    """Generate pyarrow table from data gen"""
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen
        # we cannot create a data frame from a nullable struct
        assert not data_gen.nullable
    # gen row based data
    data = gen_df_help(src, length, seed)
    # convert row based data to column based data
    columnar_data = [[data[row][col] for row in range(length)] for col in range(len(src.children))]
    # generate pa schema
    fields = [(name, _get_pa_type(child)) for name, child in src.children]
    pa_schema = pa.schema(fields)
    pa_data = [pa.array(col, type=field[1]) for col, field in zip(columnar_data, fields)]
    # generate pa table
    return pa.Table.from_arrays(pa_data, schema=pa_schema)


def _convert_pyarrow_struct_object_to_cpu_object(pyarrow_struct_scalar):
    """Convert pyarrow struct to pyspark.Row"""
    # convert pyarrow.StructScalar obj to python dict
    py_dict = pyarrow_struct_scalar.as_py()
    # convert py_dict to Pyspark Row
    if py_dict is None:
        return None
    else:
        return Row(**py_dict)


def _convert_pyarrow_list_object_to_cpu_object(pyarrow_list_scalar):
    """Convert pyarrow list to python list"""
    # pyarrow.ListScalar obj to python list
    py_list = pyarrow_list_scalar.as_py()
    if py_list is None:
        return None
    else:
        return py_list


def _convert_pyarrow_map_object_to_cpu_object(pyarrow_map_scalar):
    """Convert pyarrow map to python dict"""
    # pyarrow.MapScalar obj to python (key, value) pair list
    py_key_value_pair_list = pyarrow_map_scalar.as_py()
    if py_key_value_pair_list is None:
        return None
    else:
        return dict(py_key_value_pair_list)


def _convert_pyarrow_binary_object_to_cpu_object(pyarrow_binary_scalar):
    """Convert pyarrow binary to bytearray"""
    # pyarrow.BinaryScalar obj to bytearray
    py_bytearray = pyarrow_binary_scalar.as_py()
    if py_bytearray is None:
        return None
    else:
        return bytearray(py_bytearray)


def _convert_pyarrow_column_to_cpu_object(pyarrow_column, pa_type):
    """ convert pyarrow column to a column contains CPU type objects"""
    if isinstance(pa_type, pa.StructType):
        # struct column
        return [_convert_pyarrow_struct_object_to_cpu_object(v) for v in pyarrow_column]
    elif isinstance(pa_type, pa.ListType):
        # list column
        return [_convert_pyarrow_list_object_to_cpu_object(v) for v in pyarrow_column]
    elif isinstance(pa_type, pa.MapType):
        # map column
        return [_convert_pyarrow_map_object_to_cpu_object(v) for v in pyarrow_column]
    elif pa_type.equals(pa.binary()):
        # binary column should be converted to bytearray
        return [_convert_pyarrow_binary_object_to_cpu_object(v) for v in pyarrow_column]
    else:
        # other type column
        return [v.as_py() for v in pyarrow_column]


def _gen_row_from_columnar_cpu_data(column_names, columnar_data, row_idx):
    """Gen a Pyspark Row from columnar data for a row index"""
    key_value_pair_list = [(column_names[col], columnar_data[col][row_idx]) for col in range(len(column_names))]
    return Row(**dict(key_value_pair_list))


def _convert_pyarrow_table_to_cpu_object(pa_table):
    """
    Convert pyarrow table to cpu type data to reuse the `_assert_equal(cpu, gpu)` in asserts.py
     e.g.: convert pyarrow map_scalar to Pyspark Row
    """
    columnar_data = []
    for pa_column, pa_type in zip(pa_table.columns, pa_table.schema.types):
        converted_column = _convert_pyarrow_column_to_cpu_object(pa_column, pa_type)
        columnar_data.append(converted_column)
    cpu_data = [
        _gen_row_from_columnar_cpu_data(pa_table.schema.names, columnar_data, row_idx)
        for row_idx in range(pa_table.num_rows)
    ]
    return cpu_data


def assert_equal(cpu_adapted_from_pyarrow, gpu):
    """Verify that the result from the pyarrow(already adapted to CPU data format) and the GPU are equal"""
    try:
        _assert_equal(cpu_adapted_from_pyarrow, gpu, float_check=get_float_check(), path=[],
                      cpu_data_adapted_from="pyarrow")
    except:
        sys.stdout.writelines(difflib.unified_diff(
            a=[f"{x}\n" for x in cpu_adapted_from_pyarrow],
            b=[f"{x}\n" for x in gpu],
            fromfile='pyarrow OUTPUT',
            tofile='GPU OUTPUT'))
        raise


def assert_gpu_and_pyarrow_are_compatible(base_write_path, gen_list, conf={}):
    """
    Test scenarios:
      Write by pyarrow, test readings on pyarrow and GPU
      Write by GPU, test readings on pyarrow and GPU
    The data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    conf = _prep_incompat_conf(conf)
    pyarrow_parquet_path = base_write_path + "/pyarrow"
    gpu_parquet_path = base_write_path + "/gpu"

    def write_on_pyarrow():
        pa_table = _gen_pyarrow_table(gen_list)
        pa_pq.write_table(pa_table, pyarrow_parquet_path)

    def write_on_gpu():
        with_gpu_session(lambda spark: gen_df(spark, gen_list, length=1).coalesce(1).write.parquet(gpu_parquet_path), conf=conf)

    def read_on_pyarrow(parquet_path):
        print('### pyarrow RUN ###')
        pyarrow_start = time.time()
        # By default, pyarrow read timestamp column with precision ns instead of us
        # Spark only supports us
        # coerce_int96_timestamp_unit is used to read GPU generated timestamp column as us
        #
        pyarrow_table = pa_pq.read_table(parquet_path, coerce_int96_timestamp_unit="us")
        # adapt pyarrow table to CPU result
        from_pyarrow = _convert_pyarrow_table_to_cpu_object(pyarrow_table)
        pyarrow_end = time.time()
        return (from_pyarrow, pyarrow_end - pyarrow_start)

    def read_on_gpu(parquet_path):
        print('### GPU RUN ###')
        gpu_start = time.time()
        # collect all data to driver
        from_gpu = with_gpu_session(lambda spark: spark.read.parquet(parquet_path).collect(), conf=conf)
        gpu_end = time.time()
        return (from_gpu, gpu_end - gpu_start)

    def compare_reads(parquet_path):
        from_pyarrow, time_pyarrow = read_on_pyarrow(parquet_path)
        from_gpu, time_gpu = read_on_gpu(parquet_path)
        print('### collect: GPU TOOK {} pyarrow TOOK {} ###'.format(time_pyarrow, time_gpu))
        # sort locally
        from_pyarrow.sort(key=_RowCmp)
        from_gpu.sort(key=_RowCmp)
        assert_equal(from_pyarrow, from_gpu)

    # write on pyarrow, compare reads between pyarrow and GPU
    write_on_pyarrow()
    compare_reads(pyarrow_parquet_path)

    # write on GPU, compare reads between pyarrow and GPU
    write_on_gpu()
    compare_reads(gpu_parquet_path)


# types for test_parquet_read_round_trip_write_by_pyarrow
parquet_gens_list = [
    [boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen],  # basic types
    [date_gen],
    [timestamp_gen],
    [binary_gen],
    [StructGen([('child1', short_gen)])],
    [ArrayGen(date_gen)],
    [MapGen(IntegerGen(nullable=False), int_gen)],
]


#
# test read/write by pyarrow/GPU
#
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
        'spark.sql.legacy.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',

        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        'spark.sql.parquet.datetimeRebaseModeInRead': 'CORRECTED'})

    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_pyarrow_are_compatible(spark_tmp_path, gen_list, conf=all_confs)
