# Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from conftest import is_databricks_runtime
from marks import incompat, allow_non_gpu
from spark_session import is_before_spark_313, is_before_spark_330, is_databricks113_or_later, is_spark_330_or_later, is_databricks104_or_later, is_spark_33X, is_spark_340_or_later, is_spark_330, is_spark_330cdh
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
from pyspark.sql.functions import array_contains, col, element_at, lit, array

# max_val is a little larger than the default max size(20) of ArrayGen
# so we can get the out-of-bound indices.
array_neg_index_gen = IntegerGen(min_val=-25, max_val=-1, special_cases=[None])
array_out_index_gen = IntegerGen(min_val=25, max_val=100, special_cases=[None])
array_zero_index_gen = IntegerGen(min_val=0, max_val=0, special_cases=[])
array_no_zero_index_gen = IntegerGen(min_val=1, max_val=25,
    special_cases=[(-25, 100), (-20, 100), (-10, 100), (-4, 100), (-3, 100), (-2, 100), (-1, 100), (None, 100)])

array_all_null_gen = ArrayGen(int_gen, all_null=True)
array_item_test_gens = array_gens_sample + [array_all_null_gen,
    ArrayGen(MapGen(StringGen(pattern='key_[0-9]', nullable=False), StringGen(), max_length=10), max_length=10),
    ArrayGen(BinaryGen(max_length=10), max_length=10)]


# Need these for set-based operations
# See https://issues.apache.org/jira/browse/SPARK-39845
_non_neg_zero_float_special_cases = [
    FLOAT_MIN,
    FLOAT_MAX,
    -1.0,
    1.0,
    0.0,
    float('inf'),
    float('-inf'),
    float('nan'),
    NEG_FLOAT_NAN_MAX_VALUE
]

_non_neg_zero_double_special_cases = [
    DoubleGen.make_from(1, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(0, DOUBLE_MAX_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(1, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    DoubleGen.make_from(0, DOUBLE_MIN_EXP, DOUBLE_MAX_FRACTION),
    -1.0,
    1.0,
    0.0,
    float('inf'),
    float('-inf'),
    float('nan'),
    NEG_DOUBLE_NAN_MAX_VALUE
]

no_neg_zero_all_basic_gens = [byte_gen, short_gen, int_gen, long_gen,
        # -0.0 cannot work because of -0.0 == 0.0 in cudf for distinct
        # but nans and other default special cases do work
        FloatGen(special_cases=_non_neg_zero_float_special_cases), 
        DoubleGen(special_cases=_non_neg_zero_double_special_cases),
        string_gen, boolean_gen, date_gen, timestamp_gen]

no_neg_zero_all_basic_gens_no_nulls = [StringGen(nullable=False), ByteGen(nullable=False),
        ShortGen(nullable=False), IntegerGen(nullable=False), LongGen(nullable=False),
        BooleanGen(nullable=False), DateGen(nullable=False), TimestampGen(nullable=False),
        FloatGen(special_cases=_non_neg_zero_float_special_cases, nullable=False),
        DoubleGen(special_cases=_non_neg_zero_double_special_cases, nullable=False)]

decimal_gens_no_nulls = [DecimalGen(precision=7, scale=3, nullable=False),
        DecimalGen(precision=12, scale=2, nullable=False),
        DecimalGen(precision=20, scale=2, nullable=False)]

# This non-nans version is only used for Spark version < 3.1.3
no_neg_zero_all_basic_gens_no_nans = [byte_gen, short_gen, int_gen, long_gen,
        # -0.0 cannot work because of -0.0 == 0.0 in cudf for distinct
        FloatGen(special_cases=[], no_nans=True), 
        DoubleGen(special_cases=[], no_nans=True),
        string_gen, boolean_gen, date_gen, timestamp_gen]


byte_array_index_gen = ByteGen(min_val=-25, max_val=25, special_cases=[None])
short_array_index_gen = ShortGen(min_val=-25, max_val=25, special_cases=[None])
int_array_index_gen = IntegerGen(min_val=-25, max_val=25, special_cases=[None])
# include special case indexes that should be valid indexes after the long is truncated.
# this is becaue Spark will truncarte the long to an int, and we want to catch if it ever changes
# -4294967286 is 0xFFFFFFFF0000000A, but python does not translate it the same way as scala does
# so I had to write it out manually
long_array_index_gen = LongGen(min_val=-25, max_val=25, special_cases=[0x1111111100000000, -4294967286])

array_index_gens = [byte_array_index_gen, short_array_index_gen, int_array_index_gen, long_array_index_gen]

@pytest.mark.parametrize('data_gen', array_item_test_gens, ids=idfn)
@pytest.mark.parametrize('index_gen', array_index_gens, ids=idfn)
def test_array_item(data_gen, index_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, index_gen).selectExpr('a[b]'))

@pytest.mark.parametrize('data_gen', array_item_test_gens, ids=idfn)
def test_array_item_lit_ordinal(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'a[CAST(0 as BYTE)]',
            'a[CAST(1 as SHORT)]',
            'a[null]',
            'a[3]',
            'a[CAST(50 as LONG)]',
            'a[-1]',
            'a[2147483648]',
            'a[-2147483648]'))

# No need to test this for multiple data types for array. Only one is enough
@pytest.mark.skipif(not is_spark_33X() or is_databricks_runtime(), reason="'strictIndexOperator' is introduced from Spark 3.3.0 and removed in Spark 3.4.0 and DB11.3")
@pytest.mark.parametrize('strict_index_enabled', [True, False])
@pytest.mark.parametrize('index', [-2, 100, array_neg_index_gen, array_out_index_gen], ids=idfn)
def test_array_item_with_strict_index(strict_index_enabled, index):
    message = "SparkArrayIndexOutOfBoundsException"
    if isinstance(index, int):
        test_df = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(col('a')[index])
    else:
        test_df = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr('a[b]')

    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': strict_index_enabled})

    if strict_index_enabled:
        assert_gpu_and_cpu_error(
            lambda spark: test_df(spark).collect(),
            conf=test_conf,
            error_message=message)
    else:
        assert_gpu_and_cpu_are_equal_collect(
            test_df,
            conf=test_conf)

# No need to test this for multiple data types for array. Only one is enough, but with two kinds of invalid index.
@pytest.mark.parametrize('index', [-2, 100, array_neg_index_gen, array_out_index_gen], ids=idfn)
def test_array_item_ansi_fail_invalid_index(index):
    message = "SparkArrayIndexOutOfBoundsException" if (is_databricks104_or_later() or is_spark_330_or_later()) else "java.lang.ArrayIndexOutOfBoundsException"
    if isinstance(index, int):
        test_func = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(col('a')[index]).collect()
    else:
        test_func = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr('a[b]').collect()
    assert_gpu_and_cpu_error(
        test_func,
        conf=ansi_enabled_conf,
        error_message=message)


def test_array_item_ansi_not_fail_all_null_data():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, array_all_null_gen, array_neg_index_gen, array_out_index_gen).selectExpr(
            'a[100]',
            'a[-2]',
            'a[b]',
            'a[c]'),
        conf=ansi_enabled_conf)


@pytest.mark.parametrize('data_gen', all_basic_gens + [
                         decimal_gen_32bit, decimal_gen_64bit, decimal_gen_128bit, binary_gen,
                         StructGen([['child0', StructGen([['child01', IntegerGen()]])], ['child1', string_gen], ['child2', float_gen]], nullable=False),
                         StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]], nullable=False)], ids=idfn)
def test_make_array(data_gen):
    (s1, s2) = with_cpu_session(
        lambda spark: gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen)))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'array(null)',
                'array(a, b)',
                'array(b, a, null, {}, {})'.format(s1, s2),
                'array(array(b, a, null, {}, {}), array(a), array(null))'.format(s1, s2)))


@pytest.mark.parametrize('data_gen', single_level_array_gens, ids=idfn)
def test_orderby_array_unique(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'array_table',
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
                                      ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10)], ids=idfn)
def test_orderby_array_of_arrays(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
    lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'array_table',
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [ArrayGen(StructGen([['child0', byte_gen],
                                                          ['child1', string_gen],
                                                          ['child2', float_gen]]))], ids=idfn)
def test_orderby_array_of_structs(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'array_table',
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [byte_gen, short_gen, int_gen, long_gen,
                                      float_gen, double_gen,
                                      string_gen, boolean_gen, date_gen, timestamp_gen], ids=idfn)
def test_array_contains(data_gen):
    arr_gen = ArrayGen(data_gen)
    literal = with_cpu_session(lambda spark: gen_scalar(data_gen, force_no_nulls=True))

    def get_input(spark):
        return two_col_df(spark, arr_gen, data_gen)

    assert_gpu_and_cpu_are_equal_collect(lambda spark: get_input(spark).select(
                                            array_contains(array(lit(None)), col('b')),
                                            array_contains(array(), col('b')),
                                            array_contains(array(lit(literal), lit(literal)), col('b')),
                                            array_contains(col('a'), literal.cast(data_gen.data_type)),
                                            array_contains(col('a'), col('b')),
                                            array_contains(col('a'), col('a')[5])))


@pytest.mark.parametrize('data_gen',
                         [FloatGen(special_cases=[(float('nan'), 20)]),
                          DoubleGen(special_cases=[(float('nan'), 20)])], ids=idfn)
def test_array_contains_for_nans(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, ArrayGen(data_gen), data_gen).select(
            array_contains(col('a'), col('b')),
            array_contains(col('a'), lit(float('nan')).cast(data_gen.data_type))))


@pytest.mark.parametrize('data_gen', array_item_test_gens, ids=idfn)
def test_array_element_at(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, array_no_zero_index_gen).selectExpr(
            'element_at(a, cast(NULL as int))',
            'element_at(a, 1)',
            'element_at(a, 30)',
            'element_at(a, -1)',
            'element_at(a, -30)',
            'element_at(a, b)'))


# No need tests for multiple data types for list data. Only one is enough.
@pytest.mark.parametrize('index', [100, array_out_index_gen], ids=idfn)
def test_array_element_at_ansi_fail_invalid_index(index):
    message = "ArrayIndexOutOfBoundsException" if is_before_spark_330() else "SparkArrayIndexOutOfBoundsException"
    if isinstance(index, int):
        test_func = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(
            element_at(col('a'), index)).collect()
    else:
        test_func = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr(
            'element_at(a, b)').collect()
    # For 3.3.0+ strictIndexOperator should not affect element_at
    # `strictIndexOperator` has been removed in Spark3.4+ and Databricks11.3+
    test_conf = ansi_enabled_conf if (is_spark_340_or_later() or is_databricks113_or_later()) else \
        copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': 'false'})
    assert_gpu_and_cpu_error(
        test_func,
        conf=test_conf,
        error_message=message)


def test_array_element_at_ansi_not_fail_all_null_data():
    # No exception when zero index but all the array rows are null
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, array_all_null_gen, array_zero_index_gen, array_out_index_gen).selectExpr(
            'element_at(a, 0)',
            'element_at(a, b)',
            'element_at(a, c)'),
        conf=ansi_enabled_conf)


@pytest.mark.parametrize('index', [0, array_zero_index_gen], ids=idfn)
@pytest.mark.parametrize('ansi_enabled', [False, True], ids=idfn)
def test_array_element_at_zero_index_fail(index, ansi_enabled):
    if is_spark_340_or_later():
        message = "org.apache.spark.SparkRuntimeException: [INVALID_INDEX_OF_ZERO] The index 0 is invalid"
    elif is_databricks113_or_later():
        message = "org.apache.spark.SparkRuntimeException: [ELEMENT_AT_BY_INDEX_ZERO] The index 0 is invalid"
    else:
        message = "SQL array indices start at 1"

    if isinstance(index, int):
        test_func = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(
            element_at(col('a'), index)).collect()
    else:
        test_func = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr(
            'element_at(a, b)').collect()
    assert_gpu_and_cpu_error(
        test_func,
        conf={'spark.sql.ansi.enabled':ansi_enabled},
        error_message=message)


@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_array_transform(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                'transform(a, item -> item) as ident',
                'transform(a, item -> null) as n',
                'transform(a, item -> 1) as one',
                'transform(a, (item, index) -> index) as indexed',
                'transform(a, item -> b) as b_val',
                'transform(a, (item, index) -> index - b) as math_on_index']
        element_type = data_gen.data_type.elementType
        # decimal types can grow too large so we are avoiding those here for now
        if isinstance(element_type, IntegralType):
            columns.extend(['transform(a, item -> item + 1) as add',
                'transform(a, item -> item + item) as mul',
                'transform(a, (item, index) -> item + index + b) as all_add'])

        if isinstance(element_type, StringType):
            columns.extend(['transform(a, entry -> concat(entry, "-test")) as con'])

        if isinstance(element_type, ArrayType):
            columns.extend(['transform(a, entry -> transform(entry, sub_entry -> 1)) as sub_one',
                'transform(a, (entry, index) -> transform(entry, (sub_entry, sub_index) -> index)) as index_as_sub_entry',
                'transform(a, (entry, index) -> transform(entry, (sub_entry, sub_index) -> index + sub_index)) as index_add_sub_index',
                'transform(a, (entry, index) -> transform(entry, (sub_entry, sub_index) -> index + sub_index + b)) as add_indexes_and_value'])

        return two_col_df(spark, data_gen, byte_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)

non_utc_allow_for_sequence = ['ProjectExec'] # Update after non-utc time zone is supported for sequence
@allow_non_gpu(*non_utc_allow_for_sequence)
def test_array_transform_non_deterministic():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(1).selectExpr("transform(sequence(0, cast(rand(5)*10 as int) + 1), x -> x * 22) as t"),
            conf={'spark.rapids.sql.castFloatToIntegralTypes.enabled': True})

@allow_non_gpu(*non_utc_allow_for_sequence)
def test_array_transform_non_deterministic_second_param():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : debug_df(spark.range(1).selectExpr("transform(sequence(0, cast(rand(5)*10 as int) + 1), (x, i) -> x + i) as t")),
            conf={'spark.rapids.sql.castFloatToIntegralTypes.enabled': True})

# TODO add back in string_gen when https://github.com/rapidsai/cudf/issues/9156 is fixed
array_min_max_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen] + decimal_gens

@pytest.mark.parametrize('data_gen', array_min_max_gens, ids=idfn)
def test_array_min_max(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, ArrayGen(data_gen)).selectExpr(
                'array_min(a)', 'array_max(a)'))

@pytest.mark.parametrize('data_gen', [ArrayGen(SetValuesGen(datatype, [math.nan, None])) for datatype in [FloatType(), DoubleType()]], ids=idfn)
def test_array_min_max_all_nans(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'array_min(a)', 'array_max(a)'))

@pytest.mark.parametrize('data_gen', [ArrayGen(int_gen, all_null=True)], ids=idfn)
def test_array_min_max_all_nulls(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'array_min(a)', 'array_max(a)'))

@pytest.mark.parametrize('data_gen', decimal_gens, ids=idfn)
def test_array_concat_decimal(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : debug_df(unary_op_df(spark, ArrayGen(data_gen)).selectExpr(
            'concat(a, a)')))

@pytest.mark.parametrize('data_gen', orderable_gens + nested_gens_sample, ids=idfn)
def test_array_repeat_with_count_column(data_gen):
    cnt_gen = IntegerGen(min_val=-5, max_val=5, special_cases=[])
    cnt_not_null_gen = IntegerGen(min_val=-5, max_val=5, special_cases=[], nullable=False)
    gen = StructGen(
        [('elem', data_gen), ('cnt', cnt_gen), ('cnt_nn', cnt_not_null_gen)], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'array_repeat(elem, cnt)',
            'array_repeat(elem, cnt_nn)',
            'array_repeat("abc", cnt)'))


@pytest.mark.parametrize('data_gen', orderable_gens + nested_gens_sample, ids=idfn)
def test_array_repeat_with_count_scalar(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'array_repeat(a, 3)',
            'array_repeat(a, 1)',
            'array_repeat(a, 0)',
            'array_repeat(a, -2)',
            'array_repeat("abc", 2)',
            'array_repeat("abc", 0)',
            'array_repeat("abc", -1)'))


# We add in several types of processing for foldable functions because the output
# can be different types.
@pytest.mark.parametrize('query', [
    'sequence(1, 5) as s',
    'array(1, 2, 3) as a',
    'array(sequence(1, 5), sequence(2, 7)) as a_a',
    'array(map(1, "a", 2, "b")) as a_m',
    'array(map_from_arrays(sequence(1, 2), array("1", "2"))) as a_m',
    'array(struct(1 as a, 2 as b), struct(3 as a, 4 as b)) as a_s',
    'array(struct(1 as a, sequence(1, 5) as b), struct(3 as a, sequence(2, 7) as b)) as a_s_a',
    'array(array(struct(1 as a, 2 as b), struct(3 as a, 4 as b))) as a_a_s'], ids=idfn)
def test_sql_array_scalars(query):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.sql('SELECT {}'.format(query)))


@pytest.mark.parametrize('data_gen', all_basic_gens + nested_gens_sample, ids=idfn)
def test_get_array_struct_fields(data_gen):
    array_struct_gen = ArrayGen(
        StructGen([['child0', data_gen], ['child1', int_gen]]),
        max_length=6)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, array_struct_gen).selectExpr('a.child0'))

@pytest.mark.parametrize('data_gen', [ArrayGen(string_gen), ArrayGen(int_gen)])
@pytest.mark.parametrize('threeVL', [
    pytest.param(False, id='3VL:off'),
    pytest.param(True, id='3VL:on'),
])
def test_array_exists(data_gen, threeVL):
    def do_it(spark):
        columns = ['a']
        element_type = data_gen.data_type.elementType
        if isinstance(element_type, IntegralType):
            columns.extend([
                'exists(a, item -> item % 2 = 0) as exists_even',
                'exists(a, item -> item < 0) as exists_negative',
                'exists(a, item -> item >= 0) as exists_non_negative'
            ])

        if isinstance(element_type, StringType):
            columns.extend(['exists(a, entry -> length(entry) > 5) as exists_longer_than_5'])

        return unary_op_df(spark, data_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it, conf= {
        'spark.sql.legacy.followThreeValuedLogicInArrayExists' : threeVL,
    })


@pytest.mark.parametrize('data_gen', [
    ArrayGen(string_gen), 
    ArrayGen(int_gen),
    ArrayGen(ArrayGen(int_gen)),
    ArrayGen(ArrayGen(StructGen([["A", int_gen], ["B", string_gen]])))], ids=idfn)
def test_array_filter(data_gen):
    def do_it(spark):
        columns = ['a']
        element_type = data_gen.data_type.elementType
        if isinstance(element_type, IntegralType):
            columns.extend([
                'filter(a, item -> item % 2 = 0) as filter_even',
                'filter(a, item -> item < 0) as filter_negative',
                'filter(a, item -> item >= 0) as filter_non_negative'
            ])

        if isinstance(element_type, StringType):
            columns.extend(['filter(a, entry -> length(entry) > 5) as filter_longer_than_5'])

        if isinstance(element_type, ArrayType):
            columns.extend(['filter(a, entry -> size(entry) < 5) as filter_shorter_than_5'])

        return unary_op_df(spark, data_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)


array_zips_gen = array_gens_sample + [ArrayGen(map_string_string_gen[0], max_length=5),
                                      ArrayGen(BinaryGen(max_length=5), max_length=5)]


@pytest.mark.parametrize('data_gen', array_zips_gen, ids=idfn)
def test_arrays_zip(data_gen):
    gen = StructGen(
        [('a', data_gen), ('b', data_gen), ('c', data_gen), ('d', data_gen)], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'arrays_zip(a, b, c, d)',
            'arrays_zip(a, b, c)',
            'arrays_zip(a, b, array())',
            'arrays_zip(a)')
    )


def test_arrays_zip_corner_cases():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen), length=100).selectExpr(
            'arrays_zip()',
            'arrays_zip(null)',
            'arrays_zip(null, null)',
            'arrays_zip(null, a)',
            'arrays_zip(a, array())',
            'arrays_zip(a, array(), array(1, 2))',
            'arrays_zip(a, array(1, 2, 4, 3), array(5))')
    )

def test_array_max_q1():
    def q1(spark):
        return spark.sql('SELECT ARRAY_MAX(TRANSFORM(ARRAY_REPEAT(STRUCT(1, 2), 0), s -> s.col2))')
    assert_gpu_and_cpu_are_equal_collect(q1)


@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens + decimal_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_313() or is_spark_330() or is_spark_330cdh(), reason="NaN equality is only handled in Spark 3.1.3+ and SPARK-39976 issue with null and ArrayIntersect in Spark 3.3.0")
def test_array_intersect(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_intersect(a, b))',
            'sort_array(array_intersect(b, a))',
            'sort_array(array_intersect(a, array()))',
            'sort_array(array_intersect(array(), b))',
            'sort_array(array_intersect(a, a))',
            'sort_array(array_intersect(array(1), array(1, 2, 3)))',
            'sort_array(array_intersect(array(), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens_no_nulls + decimal_gens_no_nulls, ids=idfn)
@pytest.mark.skipif(not is_spark_330() and not is_spark_330cdh(), reason="SPARK-39976 issue with null and ArrayIntersect in Spark 3.3.0")
def test_array_intersect_spark330(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_intersect(a, b))',
            'sort_array(array_intersect(b, a))',
            'sort_array(array_intersect(a, array()))',
            'sort_array(array_intersect(array(), b))',
            'sort_array(array_intersect(a, a))',
            'sort_array(array_intersect(array(1), array(1, 2, 3)))',
            'sort_array(array_intersect(array(), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens_no_nans + decimal_gens, ids=idfn)
@pytest.mark.skipif(not is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_array_intersect_before_spark313(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_intersect(a, b))',
            'sort_array(array_intersect(b, a))',
            'sort_array(array_intersect(a, array()))',
            'sort_array(array_intersect(array(), b))',
            'sort_array(array_intersect(a, a))',
            'sort_array(array_intersect(array(1), array(1, 2, 3)))',
            'sort_array(array_intersect(array(), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens + decimal_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_array_union(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_union(a, b))',
            'sort_array(array_union(b, a))',
            'sort_array(array_union(a, array()))',
            'sort_array(array_union(array(), b))',
            'sort_array(array_union(a, a))',
            'sort_array(array_union(array(1), array(1, 2, 3)))',
            'sort_array(array_union(array(), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens_no_nans + decimal_gens, ids=idfn)
@pytest.mark.skipif(not is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_array_union_before_spark313(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_union(a, b))',
            'sort_array(array_union(b, a))',
            'sort_array(array_union(a, array()))',
            'sort_array(array_union(array(), b))',
            'sort_array(array_union(a, a))',
            'sort_array(array_union(array(1), array(1, 2, 3)))',
            'sort_array(array_union(array(), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens + decimal_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_array_except(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_except(a, b))',
            'sort_array(array_except(b, a))',
            'sort_array(array_except(a, array()))',
            'sort_array(array_except(array(), b))',
            'sort_array(array_except(a, a))',
            'sort_array(array_except(array(1, 2, 3), array(1, 2, 3)))',
            'sort_array(array_except(array(1), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens_no_nans + decimal_gens, ids=idfn)
@pytest.mark.skipif(not is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_array_except_before_spark313(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'sort_array(array_except(a, b))',
            'sort_array(array_except(b, a))',
            'sort_array(array_except(a, array()))',
            'sort_array(array_except(array(), b))',
            'sort_array(array_except(a, a))',
            'sort_array(array_except(array(1, 2, 3), array(1, 2, 3)))',
            'sort_array(array_except(array(1), array(1, 2, 3)))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens + decimal_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_arrays_overlap(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'arrays_overlap(a, b)',
            'arrays_overlap(b, a)',
            'arrays_overlap(a, array())',
            'arrays_overlap(array(), b)',
            'arrays_overlap(a, a)',
            'arrays_overlap(array(1), array(1, 2))',
            'arrays_overlap(array(3, 4), array(1, 2))',
            'arrays_overlap(array(), array(1, 2))')
    )

@incompat
@pytest.mark.parametrize('data_gen', no_neg_zero_all_basic_gens_no_nans + decimal_gens, ids=idfn)
@pytest.mark.skipif(not is_before_spark_313(), reason="NaN equality is only handled in Spark 3.1.3+")
def test_arrays_overlap_before_spark313(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'arrays_overlap(a, b)',
            'arrays_overlap(b, a)',
            'arrays_overlap(a, array())',
            'arrays_overlap(array(), b)',
            'arrays_overlap(a, a)',
            'arrays_overlap(array(1), array(1, 2))',
            'arrays_overlap(array(3, 4), array(1, 2))',
            'arrays_overlap(array(), array(1, 2))')
    )

@pytest.mark.parametrize('data_gen', [ByteGen(special_cases=[-10, 0, 10]), ShortGen(special_cases=[-10, 0, 10]), 
                                      IntegerGen(special_cases=[-10, 0, 10]), LongGen(special_cases=[-10, 0, 10])], ids=idfn)
def test_array_remove_scalar(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True))],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'array_remove(a, -10)',
            'array_remove(a, 0)',
            'array_remove(a, 10)')
    )

@pytest.mark.parametrize('data_gen', [ByteGen(special_cases=[5]), ShortGen(special_cases=[5]), 
                                      IntegerGen(special_cases=[5]), LongGen(special_cases=[5]),
                                      FloatGen(special_cases=_non_neg_zero_float_special_cases + [-0.0]), 
                                      DoubleGen(special_cases=_non_neg_zero_double_special_cases + [-0.0]),
                                      StringGen(pattern='[0-9]{1,5}'), boolean_gen, date_gen, timestamp_gen] + decimal_gens, ids=idfn)
def test_array_remove(data_gen):
    gen = StructGen(
        [('a', ArrayGen(data_gen, nullable=True)),
        ('b', data_gen)],
        nullable=False)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'array_remove(a, b)',
            'array_remove(a, null)')
    )


@pytest.mark.parametrize('data_gen', [ArrayGen(sub_gen) for sub_gen in array_gens_sample], ids=idfn)
def test_flatten_array(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('flatten(a)')
    )
