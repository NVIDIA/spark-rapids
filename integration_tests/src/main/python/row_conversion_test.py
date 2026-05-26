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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import allow_non_gpu, approximate_float, incompat
from pyspark.sql.types import *
from spark_session import with_cpu_session


# This is one of the most basic tests where we verify that we can
# move data onto and off of the GPU without messing up. All data
# that comes from data_gen is row formatted, with how pyspark
# currently works and when we do a collect all of that data has
# to be brought back to the CPU (rows) to be returned.
# So we just need a very simple operation in the middle that
# can be done on the GPU.
def test_row_conversions():
    gens = [["a", byte_gen], ["b", short_gen], ["c", int_gen], ["d", long_gen],
            ["e", float_gen], ["f", double_gen], ["g", string_gen], ["h", boolean_gen],
            ["i", timestamp_gen], ["j", date_gen], ["k", ArrayGen(byte_gen)],
            ["l", ArrayGen(string_gen)], ["m", ArrayGen(float_gen)],
            ["n", ArrayGen(boolean_gen)], ["o", ArrayGen(ArrayGen(short_gen))],
            ["p", StructGen([["c0", byte_gen], ["c1", ArrayGen(byte_gen)]])],
            ["q", simple_string_to_string_map_gen],
            ["r", MapGen(BooleanGen(nullable=False), ArrayGen(boolean_gen), max_length=2)],
            ["s", null_gen], ["t", decimal_gen_64bit], ["u", decimal_gen_32bit],
            ["v", decimal_gen_128bit]]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gens).selectExpr("*", "a as a_again"))

def test_row_conversions_fixed_width():
    gens = [["a", byte_gen], ["b", short_gen], ["c", int_gen], ["d", long_gen],
            ["e", float_gen], ["f", double_gen], ["h", boolean_gen],
            ["i", timestamp_gen], ["j", date_gen], ["k", decimal_gen_64bit],
            ["l", decimal_gen_32bit]]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gens).selectExpr("*", "a as a_again"))

def test_row_conversions_fixed_width_wide():
    gens = [["a{}".format(i), ByteGen(nullable=True)] for i in range(10)] + \
           [["b{}".format(i), ShortGen(nullable=True)] for i in range(10)] + \
           [["c{}".format(i), IntegerGen(nullable=True)] for i in range(10)] + \
           [["d{}".format(i), LongGen(nullable=True)] for i in range(10)] + \
           [["e{}".format(i), FloatGen(nullable=True)] for i in range(10)] + \
           [["f{}".format(i), DoubleGen(nullable=True)] for i in range(10)] + \
           [["h{}".format(i), BooleanGen(nullable=True)] for i in range(10)] + \
           [["i{}".format(i), TimestampGen(nullable=True)] for i in range(10)] + \
           [["j{}".format(i), DateGen(nullable=True)] for i in range(10)] + \
           [["k{}".format(i), DecimalGen(precision=12, scale=2, nullable=True)] for i in range(10)] + \
           [["l{}".format(i), DecimalGen(precision=7, scale=3, nullable=True)] for i in range(10)]
    def do_it(spark):
        df=gen_df(spark, gens, length=1).selectExpr("*", "a0 as a_again")
        debug_df(df)
        return df
    assert_gpu_and_cpu_are_equal_collect(do_it)

# Round-trip coverage for the AcceleratedColumnarToRow fast path on the column types it
# now accepts (DECIMAL128 and STRING) after the spark-rapids-jni row_conversion fixes.
# The schema is wide enough (> 4 cols) to clear the fast-path gate, and each row carries
# a mix of types so packMap, the JCUDF variable-width slot, and the DECIMAL128 16-byte
# branch are all exercised. Parametrize the accelerated-C2R conf to run both the fast
# and the slow path against the same query — both should match CPU results.
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
def test_accelerated_c2r_wide_with_string_and_dec128(use_fast_path):
    gens = [["l{}".format(i), LongGen(nullable=True)]    for i in range(10)] + \
           [["d{}".format(i), DoubleGen(nullable=True)]  for i in range(5)]  + \
           [["i{}".format(i), IntegerGen(nullable=True)] for i in range(5)]  + \
           [["s{}".format(i), StringGen(nullable=True)]  for i in range(3)]  + \
           [["dec{}".format(i),
             DecimalGen(precision=30, scale=4, nullable=True)] for i in range(3)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*", "l0 as l_again"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# DECIMAL128 alone (no STRING) at boundary precisions. The 16-byte path in
# CudfUnsafeRow.getDecimal and the row_conversion default branch only kick in for
# precision > MAX_LONG_DIGITS (= 18). Exercise both sides of that boundary plus a wide
# fan-out to keep the schema above the fast-path 4-column gate.
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
@pytest.mark.parametrize('precision,scale', [(18, 4), (19, 4), (30, 8), (38, 10)])
def test_accelerated_c2r_decimal128(use_fast_path, precision, scale):
    gens = [["dec{}".format(i),
             DecimalGen(precision=precision, scale=scale, nullable=True)] for i in range(8)] + \
           [["l{}".format(i), LongGen(nullable=True)] for i in range(4)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# STRING alone with values that span multiple length classes (empty, short inline, multi-
# byte UTF-8, long enough to overflow an 8-byte inline). Exercises the JCUDF variable-
# width slot and CudfUnsafeRow.getUTF8String for the offset/length decoding.
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
def test_accelerated_c2r_strings(use_fast_path):
    gens = [["s{}".format(i), StringGen(nullable=True)] for i in range(6)] + \
           [["l{}".format(i), LongGen(nullable=True)]   for i in range(4)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# Wide pure-INT32 schema: the column count is chosen so determine_tiles produces more
# than one tile, which in pre-fix code could race at non-8-aligned tile boundaries (jni
# issue #4590). With the fix, multiple repeated runs should always equal CPU results.
@pytest.mark.parametrize('use_fast_path', ['true', 'false'], ids=['fast', 'slow'])
def test_accelerated_c2r_wide_int32(use_fast_path):
    gens = [["c{}".format(i), IntegerGen(nullable=False)] for i in range(500)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens, length=256).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': use_fast_path})


# Hand-picked schemas that exercise alignment / tile-boundary edge cases the random
# data_gen-driven tests are unlikely to hit:
#   - byte_then_99_int:  leading 1-byte column forces the rest of the row off the 4-byte
#                        natural alignment; tests that compute_column_information's
#                        per-column round_up_unsafe does not produce a non-8-aligned tile
#                        boundary that the C2R kernel writes past.
#   - byte_then_int_alt: 50 INT8 / 50 INT32 packs the row with mixed alignments so the
#                        tile close point can land at a non-8-aligned cumulative byte.
#   - 200_short:         pure 2-byte columns; cumulative byte width is a multiple of 2 but
#                        not always of 8, so tile_row_size's round_up_8 padding can spill.
#   - dec128_then_int:   16-byte DECIMAL128 followed by 4-byte INT32 mixes the largest
#                        alignment with the smaller one; tile boundaries between them
#                        used to be the riskiest in the legacy kernel.
#   - 383_int:           383 INT32 columns. The tile close logic's `row_size_with_end_pad`
#                        rounding makes (k+1) parity influence whether the boundary is
#                        8-aligned; 383 is on the wrong side of that parity for some
#                        shmem budgets.
@pytest.mark.parametrize('schema', [
    pytest.param([['c0', ByteGen()]] + [['c{}'.format(i), IntegerGen()] for i in range(1, 100)],
                 id='byte_then_99_int'),
    pytest.param([['b{}'.format(i), ByteGen()] for i in range(50)] +
                 [['i{}'.format(i), IntegerGen()] for i in range(50)],
                 id='50_byte_50_int'),
    pytest.param([['s{}'.format(i), ShortGen()] for i in range(200)],
                 id='200_short'),
    pytest.param([['d', DecimalGen(precision=30, scale=4)]] +
                 [['i{}'.format(i), IntegerGen()] for i in range(99)],
                 id='dec128_then_99_int'),
    pytest.param([['i{}'.format(i), IntegerGen()] for i in range(383)],
                 id='383_int'),
])
def test_accelerated_c2r_dangerous_schemas(schema):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, schema, length=256).selectExpr("*"))


# TEMPORARY stress sweep: 56 column counts x 3 repeats = ~170 cases. The goal is to push
# the GPU through every shmem-tile boundary parity the determine_tiles logic can produce,
# repeating each schema 3 times so non-deterministic races (jni #4590 family) have more
# chances to surface. Each case still passes if and only if the GPU output equals the CPU,
# so a single mismatch on any iteration fails the run.
#
# DELETE THIS AFTER MANUAL REGRESSION SIGN-OFF. The targeted dangerous-schema test above
# stays; this one is overkill for routine CI.
@pytest.mark.parametrize('num_cols', list(range(50, 601, 10)))
@pytest.mark.parametrize('iteration', list(range(3)))
def test_temp_accelerated_c2r_tile_boundary_sweep(num_cols, iteration):
    gens = [['c{}'.format(i), IntegerGen(nullable=False)] for i in range(num_cols)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens, length=256).selectExpr("*"))


# Sanity that nested types still fall back through the slow path without crashing, even
# with the fast-path conf left on. (areAllC2RSupported should report false, dropping the
# whole table off the AcceleratedColumnarToRow gate.)
def test_accelerated_c2r_falls_back_for_nested():
    gens = [["s", StringGen(nullable=True)],
            ["a", ArrayGen(int_gen)],
            ["st", StructGen([["x", int_gen], ["y", string_gen]])],
            ["l", LongGen(nullable=True)],
            ["dec", DecimalGen(precision=30, scale=4, nullable=True)]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("*"),
        conf={'spark.rapids.sql.acceleratedColumnarToRow.enabled': 'true'})


# Test handling of transitions when the data is already columnar on the host
# Note that Apache Spark will automatically convert a load of nested types to rows, so
# the nested types will not test a host columnar transition in that case.
# Databricks does support returning nested types as columnar data on the host, and that
# is where we would expect any problems with handling nested types in host columnar form to appear.
@pytest.mark.parametrize('data_gen', [
    int_gen,
    string_gen,
    decimal_gen_64bit,
    decimal_gen_128bit,
    ArrayGen(string_gen, max_length=10),
    ArrayGen(decimal_gen_128bit, max_length=10),
    StructGen([('a', string_gen)]) ] + map_string_string_gen, ids=idfn)
@allow_non_gpu('ColumnarToRowExec', 'FileSourceScanExec')
def test_host_columnar_transition(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(lambda spark : unary_op_df(spark, data_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter("a IS NOT NULL"),
        conf={ 'spark.rapids.sql.exec.FileSourceScanExec' : 'false'})
