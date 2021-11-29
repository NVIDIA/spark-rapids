# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from marks import incompat, allow_non_gpu
from spark_session import is_before_spark_311
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
import pyspark.sql.functions as f

# Mark all tests in current file as premerge_ci_1 in order to be run in first k8s pod for parallel build premerge job
pytestmark = pytest.mark.premerge_ci_1

basic_struct_gen = StructGen([
    ['child' + str(ind), sub_gen]
    for ind, sub_gen in enumerate([StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                   BooleanGen(), DateGen(), TimestampGen(), null_gen, decimal_gen_default] + decimal_128_gens_no_neg)],
    nullable=False)

@pytest.mark.parametrize('data_gen', map_gens_sample + decimal_64_map_gens + decimal_128_map_gens, ids=idfn)
def test_map_keys(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the keys could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_keys(a)'),
        conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', map_gens_sample + decimal_64_map_gens + decimal_128_map_gens, ids=idfn)
def test_map_values(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the values could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_values(a)'),
        conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', map_gens_sample  + decimal_64_map_gens + decimal_128_map_gens, ids=idfn)
def test_map_entries(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                # Technically the order of the values could change, and be different and still correct
                # but it works this way for now so lets see if we can maintain it.
                # Good thing too, because we cannot support sorting all of the types that could be
                # in here yet, and would need some special case code for checking equality
                'map_entries(a)'),
        conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a["key_0"]',
                'a["key_1"]',
                'a[null]',
                'a["key_9"]',
                'a["NOT_FOUND"]',
                'a["key_5"]'))

@pytest.mark.parametrize('key_gen', [StringGen(nullable=False), IntegerGen(nullable=False), basic_struct_gen], ids=idfn)
@pytest.mark.parametrize('value_gen', [StringGen(nullable=True), IntegerGen(nullable=True), basic_struct_gen], ids=idfn)
def test_single_entry_map(key_gen, value_gen):
    data_gen = [('a', key_gen), ('b', value_gen)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'map("literal_key", b) as map1',
                'map(a, b) as map2'))

def test_map_expr_no_pairs():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'map() as m1'))

def test_map_expr_multiple_pairs():
    # we don't hit duplicate keys in this test due to the high cardinality of the generated strings
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key2", a) as m1',
                'map(a, b, b, a) as m2'),
                conf={'spark.rapids.sql.createMap.enabled':True})

@allow_non_gpu('ProjectExec,Alias,CreateMap,Literal')
def test_map_expr_dupe_keys_fallback():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_fallback_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'map("key1", b, "key1", a) as m1'),
                'ProjectExec',
                conf={'spark.rapids.sql.createMap.enabled':True,
                    'spark.sql.mapKeyDedupPolicy':'LAST_WIN'})

@allow_non_gpu('ProjectExec,Alias,CreateMap,Literal')
def test_map_expr_multi_non_literal_keys_fallback():
    data_gen = [('a', StringGen(nullable=False)), ('b', StringGen(nullable=False))]
    assert_gpu_fallback_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'map(a, b, b, a) as m1'),
                'ProjectExec',
                conf={'spark.rapids.sql.createMap.enabled':False})

def test_map_scalar_project():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(2).selectExpr(
                "map(1, 2, 3, 4) as i", 
                "map('a', 'b', 'c', 'd') as s",
                "map('a', named_struct('foo', 10, 'bar', 'bar')) as st"
                "id"))

@pytest.mark.skipif(is_before_spark_311(), reason="Only in Spark 3.1.1 + ANSI mode, map key throws on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value_ansi_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["NOT_FOUND"]').collect(),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True},
                error_message='java.util.NoSuchElementException')

@pytest.mark.skipif(not is_before_spark_311(), reason="For Spark before 3.1.1 + ANSI mode, null will be returned instead of an exception if key is not found")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_get_map_value_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'a["NOT_FOUND"]'),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_element_at_map(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "key_0")',
                'element_at(a, "key_1")',
                'element_at(a, "null")',
                'element_at(a, "key_9")',
                'element_at(a, "NOT_FOUND")',
                'element_at(a, "key_5")'),
                conf={'spark.sql.ansi.enabled':False})

@pytest.mark.skipif(is_before_spark_311(), reason="Only in Spark 3.1.1 + ANSI mode, map key throws on no such element")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_element_at_ansi_fail(data_gen):
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")').collect(),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True},
                error_message='java.util.NoSuchElementException')

@pytest.mark.skipif(not is_before_spark_311(), reason="For Spark before 3.1.1 + ANSI mode, null will be returned instead of an exception if key is not found")
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_map_element_at_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'element_at(a, "NOT_FOUND")'),
                conf={'spark.sql.ansi.enabled':True,
                      'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

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
                'transform_values(a, (key, value) -> transform(value, sub_entry -> 1)) as sub_one',
                'transform_values(a, (key, value) -> transform(value, (sub_entry, sub_index) -> sub_index)) as sub_index',
                'transform_values(a, (key, value) -> transform(value, (sub_entry, sub_index) -> sub_index + b)) as add_indexes'])

        if isinstance(value_type, MapType):
            columns.extend([
                'transform_values(a, (key, value) -> transform_values(value, (sub_key, sub_value) -> 1)) as sub_one'])

        return two_col_df(spark, data_gen, byte_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it, 
            conf=allow_negative_scale_of_decimal_conf)


@pytest.mark.parametrize('data_gen', map_gens_sample + decimal_128_map_gens + decimal_64_map_gens, ids=idfn)
def test_transform_keys(data_gen):
    # The processing here is very limited, because we need to be sure we do not create duplicate keys.
    # This can happen because of integer overflow, round off errors in floating point, etc. So for now
    # we really are only looking at a very basic transformation.
    def do_it(spark):
        columns = ['a',
                'transform_keys(a, (key, value) -> key) as ident']
        key_type = data_gen.data_type.keyType
        if isinstance(key_type, StringType):
            columns.extend(['transform_keys(a, (key, value) -> concat(key, "-test")) as con'])

        return unary_op_df(spark, data_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it, 
            conf=allow_negative_scale_of_decimal_conf)

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


@allow_non_gpu('ProjectExec,Alias,TransformKeys,Literal,LambdaFunction,NamedLambdaVariable')
@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_transform_keys_last_win_fallback(data_gen):
    assert_gpu_fallback_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr('transform_keys(a, (key, value) -> 1)'),
            'TransformKeys',
            conf={'spark.sql.mapKeyDedupPolicy': 'LAST_WIN'})

# We add in several types of processing for foldable functions because the output
# can be different types.
@pytest.mark.parametrize('query', [
    'map_from_arrays(sequence(1, 5), sequence(1, 5)) as m_a',
    'map("a", "a", "b", "c") as m',
    'map(1, sequence(1, 5)) as m'], ids=idfn)
def test_sql_map_scalars(query):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.sql('SELECT {}'.format(query)),
            conf={'spark.sql.legacy.allowNegativeScaleOfDecimal': 'true'})
