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
import pyarrow as pa
import pyarrow.parquet as pa_pq
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from parquet_test import read_parquet_df, read_parquet_sql, reader_opt_confs


def get_pa_type(data_gen):
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
        fields = [pa.field(name, get_pa_type(child)) for name, child in data_gen.children]
        return pa.struct(fields)
    elif isinstance(data_gen, ArrayGen):
        return pa.list_(get_pa_type(data_gen._child_gen))
    elif isinstance(data_gen, MapGen):
        return pa.map_(get_pa_type(data_gen._key_gen), get_pa_type(data_gen._value_gen))
    else:
        raise Exception("unexpected data_gen: " + str(data_gen))


def gen_pyarrow_table(data_gen, length=3, seed=0):
    """Generate pyarrow table from data gen"""
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen
        # we cannot create a data frame from a nullable struct
        assert not data_gen.nullable
    # gen row based data
    data = gen_df_help(src, length, seed)
    # gen columnar base data
    columnar_data = [[data[row][col] for row in range(length)] for col in range(len(src.children))]
    # generate pa schema
    fields = [(name, get_pa_type(child)) for name, child in src.children]
    pa_schema = pa.schema(fields)
    pa_data = [pa.array(col, type=field[1]) for col, field in zip(columnar_data, fields)]
    # generate pa table
    return pa.Table.from_arrays(pa_data, schema=pa_schema)


# types for test_parquet_read_round_trip_write_by_pyarrow
parquet_gens_list = [
    [null_gen, boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, date_gen,
     timestamp_gen, binary_gen, string_gen, decimal_gen_128bit],  # basic types
    [StructGen([('child1', short_gen)])],
    [ArrayGen(date_gen)],
    [MapGen(IntegerGen(nullable=False), int_gen)],
]


#
# Wrote by pyarrow, test reading by pyarrow and GPU
#
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_round_trip_write_by_pyarrow(
        spark_tmp_path,
        parquet_gens,
        read_func,
        reader_confs,
        v1_enabled_list
):
    # 1. Wrote by pyarrow
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    pa_table = gen_pyarrow_table(gen_list)
    pa_pq.write_table(pa_table, data_path)

    # 2. verify on CPU and GPU
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path), conf=all_confs)
