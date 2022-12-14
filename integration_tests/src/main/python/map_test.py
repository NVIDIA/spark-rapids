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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, \
    assert_gpu_fallback_collect
from data_gen import *
from conftest import is_databricks_runtime
from marks import incompat, allow_non_gpu
from spark_session import is_before_spark_330, is_databricks104_or_later, is_databricks113_or_later, is_spark_33X, is_spark_340_or_later
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType


basic_struct_gen = StructGen([
    ['child' + str(ind), sub_gen]
    for ind, sub_gen in enumerate([StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                   BooleanGen(), DateGen(), TimestampGen(), null_gen] + decimal_gens)],
    nullable=False)

maps_with_binary = [MapGen(IntegerGen(nullable=False), BinaryGen(max_length=5))]

@pytest.mark.parametrize('data_gen', map_gens_sample + maps_with_binary + decimal_64_map_gens + decimal_128_map_gens, ids=idfn)
def test_map_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the keys could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_keys(a)'))


@pytest.mark.parametrize('data_gen', map_gens_sample + maps_with_binary + decimal_64_map_gens + decimal_128_map_gens, ids=idfn)
def test_map_values(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the values could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_values(a)'))


@pytest.mark.parametrize('data_gen', map_gens_sample  + maps_with_binary + decimal_64_map_gens + decimal_128_map_gens, ids=idfn)
def test_map_entries(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the values could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_entries(a)'))


def get_map_value_gens(precision=18, scale=0):
    def simple_struct_value_gen():
        return StructGen([["child", IntegerGen()]])

    def nested_struct_value_gen():
        return StructGen([["child", simple_struct_value_gen()]])

    def nested_map_value_gen():
        return MapGen(StringGen(pattern='key_[0-9]', nullable=False), IntegerGen(), max_length=6)

    def array_value_gen():
        return ArrayGen(IntegerGen(), max_length=6)

    def decimal_value_gen():
        return DecimalGen(precision, scale)

    return [ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
            StringGen, DateGen, TimestampGen, decimal_value_gen, BinaryGen,
            simple_struct_value_gen, nested_struct_value_gen, nested_map_value_gen, array_value_gen]


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()],
                         ids=idfn)
def test_get_map_value_string_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["key_0"]',
                'a["key_1"]',
                'a[null]',
                'a["key_9"]',
                'a["NOT_FOUND"]',
                'a["key_5"]'))


numeric_key_gens = [key(nullable=False) if key in [FloatGen, DoubleGen, DecimalGen]
                    else key(nullable=False, min_val=0, max_val=100)
                    for key in [ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DecimalGen]]

numeric_key_map_gens = [MapGen(key, value(), max_length=6)
                        for key in numeric_key_gens for value in get_map_value_gens()]


@pytest.mark.parametrize('data_gen', numeric_key_map_gens, ids=idfn)
def test_get_map_value_numeric_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a[0]',
                'a[1]',
                'a[null]',
                'a[-9]',
                'a[999]'))


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('key_gen', numeric_key_gens, ids=idfn)
def test_cpu_fallback_map_scalars(key_gen):
    def query_map_scalar(spark):
        return unary_op_df(spark, key_gen).selectExpr('map(0, "zero", 1, "one")[a]')
    assert_gpu_fallback_collect(query_map_scalar, "ProjectExec", {"spark.rapids.sql.explain": "NONE"})


@pytest.mark.parametrize('data_gen',
                         [MapGen(DateGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()], ids=idfn)
def test_get_map_value_date_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'a[date "1997"]',
            'a[date "2022-01-01"]',
            'a[null]'))


@pytest.mark.parametrize('data_gen',
                         [MapGen(TimestampGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()], ids=idfn)
def test_get_map_value_timestamp_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'a[timestamp "1997"]',
            'a[timestamp "2022-01-01"]',
            'a[null]'))


def test_map_side_effects():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.range(10).selectExpr(
                'id',
                'if(id == 0, null, map(id, id, id DIV 2, id)) as m'),
            conf={'spark.sql.mapKeyDedupPolicy': 'EXCEPTION'})


@pytest.mark.parametrize('key_gen', [StringGen(nullable=False), IntegerGen(nullable=False), basic_struct_gen], ids=idfn)
@pytest.mark.parametrize('value_gen', [StringGen(nullable=True), IntegerGen(nullable=True), basic_struct_gen], ids=idfn)
def test_single_entry_map(key_gen, value_gen):
    data_gen = [('a', key_gen), ('b', value_gen)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("literal_key", b) as map1',
                'map(a, b) as map2'))


def test_map_expr_no_pairs():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map() as m1'))


def test_map_expr_multiple_pairs():
    # we don't hit duplicate keys in this test due to the high cardinality of the generated strings
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key2", a) as m1',
                'map(a, b, b, a) as m2'))


def test_map_expr_expr_keys_dupe_last_win():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map(a, b, a, b) as m2'),
            conf={'spark.sql.mapKeyDedupPolicy':'LAST_WIN'})


def test_map_expr_expr_keys_dupe_exception():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_error(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map(a, b, a, b) as m2').collect(),
            conf={'spark.sql.mapKeyDedupPolicy':'EXCEPTION'},
            error_message = "Duplicate map key")


def test_map_expr_literal_keys_dupe_last_win():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key1", a) as m1'),
            conf={'spark.sql.mapKeyDedupPolicy':'LAST_WIN'})


def test_map_expr_literal_keys_dupe_exception():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_error(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key1", a) as m1').collect(),
            conf={'spark.sql.mapKeyDedupPolicy':'EXCEPTION'},
            error_message = "Duplicate map key")


def test_map_expr_multi_non_literal_keys():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, data_gen).selectExpr(
                'map(a, b, b, a) as m1'))


def test_map_scalar_project():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.range(2).selectExpr(
                "map(1, 2, 3, 4) as i",
                "map('a', 'b', 'c', 'd') as s",
                "map('a', named_struct('foo', 10, 'bar', 'bar')) as st"
                "id"))


def test_str_to_map_expr_fixed_pattern_input():
    # Test pattern "key1:val1,key2:val2".
    # In order to prevent duplicate keys, the first key starts with a number [0-9] and the second
    # key start with a letter [a-zA-Z].
    data_gen = [('a', StringGen(pattern='[0-9].{0,10}:.{0,10},[a-zA-Z].{0,10}:.{0,10}',
                                nullable=True))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m1',
            'str_to_map(a, ",") as m2',
            'str_to_map(a, ",", ":") as m3'))


def test_str_to_map_expr_fixed_delimiters():
    data_gen = [('a', StringGen(pattern='[0-9a-zA-Z:,]{0,100}', nullable=True)
                 .with_special_pattern('[abc]:.{0,20},[abc]:.{0,20}', weight=100))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m1',
            'str_to_map(a, ",") as m2',
            'str_to_map(a, ",", ":") as m3'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_random_delimiters():
    data_gen = [('a', StringGen(pattern='[0-9a-z:,]{0,100}', nullable=True)
                 .with_special_pattern('[abc]:.{0,20},[abc]:.{0,20}', weight=100))]
    delim_gen = StringGen(pattern='[0-9a-z :,]', nullable=False)
    (pair_delim, keyval_delim) = ('', '')
    while pair_delim == keyval_delim:
        (pair_delim, keyval_delim) = gen_scalars_for_sql(delim_gen, 2, force_no_nulls=True)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a) as m1',
            'str_to_map(a, {}) as m2'.format(pair_delim),
            'str_to_map(a, {}, {}) as m3'.format(pair_delim, keyval_delim)
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_input_no_delimiter():
    # Test input strings that contain either one delimiter or do not contain delimiters at all.
    data_gen = [('a', StringGen(pattern='[0-9:,]{0,100}', nullable=True)
                 .with_special_pattern('[abc]:.{0,20},[abc]:.{0,20}', weight=100))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a, "A", ":") as m1',  # input doesn't contain pair delimiter
            'str_to_map(a, ",", "A") as m2',  # input doesn't contain key-value delimiter
            'str_to_map(a, "A", "A") as m3'   # input doesn't contain any delimiter
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_with_regex_and_non_regex_delimiters():
    data_gen = [('a', StringGen(pattern='(([bf]:{0,5}){1,7},{0,5}[0-9]{1,10}){0,10}',
                                nullable=True))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a, "[,]") as m1',
            'str_to_map(a, "[,]{1,5}") as m2',
            'str_to_map(a, "[,b]") as m3',
            'str_to_map(a, ",", "[:]") as m4',
            'str_to_map(a, ",", "[:f]") as m5',
            'str_to_map(a, ",", "[:]{1,10}") as m6'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


def test_str_to_map_expr_with_all_regex_delimiters():
    data_gen = [('a', StringGen(pattern='(([bf]:{0,5}){1,7},{0,5}[0-9]{1,10}){0,10}',
                                nullable=True))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(
            'str_to_map(a, "[,]") as m1',
            'str_to_map(a, "[,]", "[:]") as m2',
            'str_to_map(a, "[,b]", "[:f]") as m3',
            'str_to_map(a, "[,]", "[:]{1,10}") as m4',
            'str_to_map(a, "[,]{1,10}", "[:]{1,10}") as m5'
        ), conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.skipif(not is_before_spark_330(),
                    reason="Only in Spark 3.1.1+ (< 3.3.0) + ANSI mode, map key throws on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_ansi_fail(data_gen):
    message = "org.apache.spark.SparkNoSuchElementException" if is_databricks104_or_later() else "java.util.NoSuchElementException"
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["NOT_FOUND"]').collect(),
            conf=ansi_enabled_conf,
            error_message=message)


@pytest.mark.skipif(not is_spark_33X() or is_databricks_runtime(),
                    reason="Only in Spark 3.3.X + ANSI mode + Strict Index, map key throws on no such element")
@pytest.mark.parametrize('strict_index', ['true', 'false'])
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_with_strict_index(strict_index, data_gen):
    message = "org.apache.spark.SparkNoSuchElementException"
    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': strict_index})
    if strict_index == 'true':
        assert_gpu_and_cpu_error(
                lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a["NOT_FOUND"]').collect(),
                conf=test_conf,
                error_message=message)
    else:
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a["NOT_FOUND"]'),
                conf=test_conf)


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()],
                         ids=idfn)
def test_element_at_map_string_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "key_0")',
                'element_at(a, "key_1")',
                'element_at(a, "null")',
                'element_at(a, "key_9")',
                'element_at(a, "NOT_FOUND")',
                'element_at(a, "key_5")'),
            conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen', numeric_key_map_gens, ids=idfn)
def test_element_at_map_numeric_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'element_at(a, 0)',
            'element_at(a, 1)',
            'element_at(a, null)',
            'element_at(a, -9)',
            'element_at(a, 999)'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen',
                         [MapGen(DecimalGen(precision=35, scale=2, nullable=False), value(), max_length=6)
                          for value in get_map_value_gens(precision=37, scale=0)],
                         ids=idfn)
def test_get_map_value_element_at_map_dec_col_keys(data_gen):
    keys = DecimalGen(precision=35, scale=2)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)', 'a[b]'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key', nullable=False),
                                 IntegerGen(nullable=False), max_length=1, min_length=1, nullable=False)],
                         ids=idfn)
@pytest.mark.parametrize('ansi', [True, False], ids=idfn)
def test_get_map_value_element_at_map_string_col_keys_ansi(data_gen, ansi):
    keys = StringGen(pattern='key', nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)', 'a[b]'),
        conf={'spark.sql.ansi.enabled': ansi})


@pytest.mark.parametrize('data_gen',
                         [MapGen(StringGen(pattern='key_[0-9]', nullable=False), value(), max_length=6)
                          for value in get_map_value_gens(precision=37, scale=0)],
                         ids=idfn)
def test_get_map_value_element_at_map_string_col_keys(data_gen):
    keys = StringGen(pattern='key_[0-9]')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)', 'a[b]'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
@pytest.mark.skipif(is_spark_340_or_later() or is_databricks113_or_later(),
                    reason="Since Spark3.4 and DB11.3, null will always be returned on invalid access to map")
def test_element_at_map_string_col_keys_ansi_fail(data_gen):
    keys = StringGen(pattern='NOT_FOUND')
    message = "org.apache.spark.SparkNoSuchElementException" if (not is_before_spark_330() or is_databricks104_or_later()) else "java.util.NoSuchElementException"
    # For 3.3.0+ strictIndexOperator should not affect element_at
    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': 'false'})
    assert_gpu_and_cpu_error(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'element_at(a, b)').collect(),
        conf=test_conf,
        error_message=message)


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
@pytest.mark.skipif(is_spark_340_or_later() or is_databricks113_or_later(),
                    reason="Since Spark3.4 and DB11.3, null will always be returned on invalid access to map")
def test_get_map_value_string_col_keys_ansi_fail(data_gen):
    keys = StringGen(pattern='NOT_FOUND')
    message = "org.apache.spark.SparkNoSuchElementException" if (not is_before_spark_330() or is_databricks104_or_later()) else "java.util.NoSuchElementException"
    assert_gpu_and_cpu_error(
        lambda spark: two_col_df(spark, data_gen, keys).selectExpr(
            'a[b]').collect(),
        conf=ansi_enabled_conf,
        error_message=message)


@pytest.mark.parametrize('data_gen',
                         [MapGen(DateGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()], ids=idfn)
def test_element_at_map_date_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'element_at(a, date "1997")',
            'element_at(a, date "2022-01-01")',
            'element_at(a, null)'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen',
                         [MapGen(TimestampGen(nullable=False), value(), max_length=6)
                          for value in get_map_value_gens()],
                         ids=idfn)
def test_element_at_map_timestamp_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(
            'element_at(a, timestamp "1997")',
            'element_at(a, timestamp "2022-01-01")',
            'element_at(a, null)'),
        conf={'spark.sql.ansi.enabled': False})


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
@pytest.mark.skipif(is_spark_340_or_later() or is_databricks113_or_later(),
                    reason="Since Spark3.4 and DB11.3, null will always be returned on invalid access to map")
def test_map_element_at_ansi_fail(data_gen):
    message = "org.apache.spark.SparkNoSuchElementException" if (not is_before_spark_330() or is_databricks104_or_later()) else "java.util.NoSuchElementException"
    # For 3.3.0+ strictIndexOperator should not affect element_at
    test_conf = copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': 'false'})
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")').collect(),
            conf=test_conf,
            error_message=message)


@pytest.mark.parametrize('data_gen', map_gens_sample, ids=idfn)
def test_transform_values(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                   'transform_values(a, (key, value) -> value) as ident',
                   'transform_values(a, (key, value) -> null) as n',
                   'transform_values(a, (key, value) -> 1) as one',
                   'transform_values(a, (key, value) -> key) as indexed',
                   'transform_values(a, (key, value) -> b) as b_val']
        value_type = data_gen.data_type.valueType
        # decimal types can grow too large so we are avoiding those here for now
        if isinstance(value_type, IntegralType):
            columns.extend([
                'transform_values(a, (key, value) -> value + 1) as add',
                'transform_values(a, (key, value) -> value + value) as mul',
                'transform_values(a, (key, value) -> value + b) as all_add'])

        if isinstance(value_type, StringType):
            columns.extend(['transform_values(a, (key, value) -> concat(value, "-test")) as con'])

        if isinstance(value_type, ArrayType):
            columns.extend([
                'transform_values(a, '
                '                 (key, value) -> transform(value, sub_entry -> 1)) as sub_one',
                'transform_values(a, '
                '                 (key, value) -> transform(value, (sub_entry, sub_index) -> sub_index)) as sub_index',
                'transform_values(a, '
                '                 (key, value) -> transform(value, (sub_entry, sub_index) -> sub_index + b)) as add_indexes'])

        if isinstance(value_type, MapType):
            columns.extend([
                'transform_values(a, (key, value) -> transform_values(value, (sub_key, sub_value) -> 1)) as sub_one'])

        return two_col_df(spark, data_gen, byte_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)


@pytest.mark.parametrize('data_gen', map_gens_sample + decimal_128_map_gens + decimal_64_map_gens, ids=idfn)
def test_transform_keys(data_gen):
    # The processing here is very limited, because we need to be sure we do not create duplicate keys.
    # This can happen because of integer overflow, round off errors in floating point, etc. So for now
    # we really are only looking at a very basic transformation.
    def do_it(spark):
        columns = ['a', 'transform_keys(a, (key, value) -> key) as ident']
        key_type = data_gen.data_type.keyType
        if isinstance(key_type, StringType):
            columns.extend(['transform_keys(a, (key, value) -> concat(key, "-test")) as con'])

        return unary_op_df(spark, data_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_null_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'transform_keys(a, (key, value) -> CAST(null as INT))').collect(),
            conf={},
            error_message='Cannot use null as map key')


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_duplicate_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'transform_keys(a, (key, value) -> 1)').collect(),
            conf={},
            error_message='Duplicate map key')


@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_last_win(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr('transform_keys(a, (key, value) -> 1)'),
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


@pytest.mark.parametrize('data_gen', [MapGen(IntegerGen(nullable=False), long_gen)], ids=idfn)
def test_transform_keys_last_win2(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('transform_keys(a, (key, value) -> key % 2)'),
        conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})


# We add in several types of processing for foldable functions because the output
# can be different types.
@pytest.mark.parametrize('query', [
    'map_from_arrays(sequence(1, 5), sequence(1, 5)) as m_a',
    'map("a", "a", "b", "c") as m',
    'map(1, sequence(1, 5)) as m'], ids=idfn)
def test_sql_map_scalars(query):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.sql('SELECT {}'.format(query)))


@pytest.mark.parametrize('data_gen', map_gens_sample, ids=idfn)
def test_map_filter(data_gen):
    columns = ['map_filter(a, (key, value) -> isnotnull(value) )',
               'map_filter(a, (key, value) -> isnull(value) )',
               'map_filter(a, (key, value) -> isnull(key) or isnotnull(value) )',
               'map_filter(a, (key, value) -> isnotnull(key) and isnull(value) )']
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr(columns))
