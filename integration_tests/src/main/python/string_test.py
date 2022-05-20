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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, \
    assert_gpu_sql_fallback_collect, assert_gpu_fallback_collect, assert_gpu_and_cpu_error, \
    assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import is_databricks_runtime
from data_gen import *
from marks import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_320

_regexp_conf = { 'spark.rapids.sql.regexp.enabled': 'true' }

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

def test_split_no_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'split(a, "AB")',
                'split(a, "C")',
                'split(a, "_")'),
                conf=_regexp_conf)

def test_split_negative_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", -1)',
            'split(a, "C", -2)',
            'split(a, "_", -999)'),
            conf=_regexp_conf)

# https://github.com/NVIDIA/spark-rapids/issues/4720
@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_zero_limit_fallback():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", 0)'),
        conf=_regexp_conf,
        exist_classes= "ProjectExec",
        non_exist_classes= "GpuProjectExec")

# https://github.com/NVIDIA/spark-rapids/issues/4720
@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_one_limit_fallback():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", 1)'),
        conf=_regexp_conf,
        exist_classes= "ProjectExec",
        non_exist_classes= "GpuProjectExec")

def test_split_positive_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", 2)',
            'split(a, "C", 3)',
            'split(a, "_", 999)'))

def test_split_re_negative_limit():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]", -1)',
            'split(a, "[o:]", -1)',
            'split(a, "[^:]", -1)',
            'split(a, "[^o]", -1)',
            'split(a, "[o]{1,2}", -1)',
            'split(a, "[bf]", -1)',
            'split(a, "[o]", -2)'),
            conf=_regexp_conf)

# https://github.com/NVIDIA/spark-rapids/issues/4720
@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_re_zero_limit_fallback():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]", 0)',
            'split(a, "[o:]", 0)',
            'split(a, "[o]", 0)'),
            exist_classes= "ProjectExec",
            non_exist_classes= "GpuProjectExec")

# https://github.com/NVIDIA/spark-rapids/issues/4720
@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_re_one_limit_fallback():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]", 1)',
            'split(a, "[o:]", 1)',
            'split(a, "[o]", 1)'),
        exist_classes= "ProjectExec",
        non_exist_classes= "GpuProjectExec")

def test_split_re_positive_limit():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]", 2)',
            'split(a, "[o:]", 5)',
            'split(a, "[^:]", 2)',
            'split(a, "[^o]", 55)',
            'split(a, "[o]{1,2}", 999)',
            'split(a, "[bf]", 2)',
            'split(a, "[o]", 5)'),
            conf=_regexp_conf)

def test_split_re_no_limit():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]")',
            'split(a, "[o:]")',
            'split(a, "[^:]")',
            'split(a, "[^o]")',
            'split(a, "[o]{1,2}")',
            'split(a, "[bf]")',
            'split(a, "[o]")'),
            conf=_regexp_conf)

def test_split_optimized_no_re():
    data_gen = mk_str_gen('([bf]o{0,2}[.?+\\^$|{}]{1,2}){1,7}') \
        .with_special_case('boo.and.foo') \
        .with_special_case('boo?and?foo') \
        .with_special_case('boo+and+foo') \
        .with_special_case('boo^and^foo') \
        .with_special_case('boo$and$foo') \
        .with_special_case('boo|and|foo') \
        .with_special_case('boo{and}foo') \
        .with_special_case('boo$|and$|foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "\\\\.")',
            'split(a, "\\\\?")',
            'split(a, "\\\\+")',
            'split(a, "\\\\^")',
            'split(a, "\\\\$")',
            'split(a, "\\\\|")',
            'split(a, "\\\\{")',
            'split(a, "\\\\}")',
            'split(a, "\\\\$\\\\|")'),
            conf=_regexp_conf)

def test_split_optimized_no_re_combined():
    data_gen = mk_str_gen('([bf]o{0,2}[AZ.?+\\^$|{}]{1,2}){1,7}') \
        .with_special_case('booA.ZandA.Zfoo') \
        .with_special_case('booA?ZandA?Zfoo') \
        .with_special_case('booA+ZandA+Zfoo') \
        .with_special_case('booA^ZandA^Zfoo') \
        .with_special_case('booA$ZandA$Zfoo') \
        .with_special_case('booA|ZandA|Zfoo') \
        .with_special_case('boo{Zand}Zfoo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "A\\\\.Z")',
            'split(a, "A\\\\?Z")',
            'split(a, "A\\\\+Z")',
            'split(a, "A\\\\^Z")',
            'split(a, "A\\\\$Z")',
            'split(a, "A\\\\|Z")',
            'split(a, "\\\\{Z")',
            'split(a, "\\\\}Z")'),
            conf=_regexp_conf)

def test_split_regexp_disabled_no_fallback():
    conf = { 'spark.rapids.sql.regexp.enabled': 'false' }
    data_gen = mk_str_gen('([bf]o{0,2}[.?+\\^$|&_]{1,2}){1,7}') \
        .with_special_case('boo.and.foo') \
        .with_special_case('boo?and?foo') \
        .with_special_case('boo+and+foo') \
        .with_special_case('boo^and^foo') \
        .with_special_case('boo$and$foo') \
        .with_special_case('boo|and|foo') \
        .with_special_case('boo&and&foo') \
        .with_special_case('boo_and_foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "\\\\.")',
            'split(a, "\\\\?")',
            'split(a, "\\\\+")',
            'split(a, "\\\\^")',
            'split(a, "\\\\$")',
            'split(a, "\\\\|")',
            'split(a, "&")',
            'split(a, "_")',
        ), conf
    )

@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_regexp_disabled_fallback():
    conf = { 'spark.rapids.sql.regexp.enabled': 'false' }
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_sql_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen),
            'StringSplit',
            'string_split_table',
            'select ' +
            'split(a, "[:]", 2), ' +
            'split(a, "[o:]", 5), ' +
            'split(a, "[^:]", 2), ' +
            'split(a, "[^o]", 55), ' +
            'split(a, "[o]{1,2}", 999), ' +
            'split(a, "[bf]", 2), ' +
            'split(a, "[o]", 5) from string_split_table',
            conf)


@pytest.mark.parametrize('data_gen,delim', [(mk_str_gen('([ABC]{0,3}_?){0,7}'), '_'),
    (mk_str_gen('([MNP_]{0,3}\\.?){0,5}'), '.'),
    (mk_str_gen('([123]{0,3}\\^?){0,5}'), '^')], ids=idfn)
def test_substring_index(data_gen,delim):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.substring_index(f.col('a'), delim, 1),
                f.substring_index(f.col('a'), delim, 3),
                f.substring_index(f.col('a'), delim, 0),
                f.substring_index(f.col('a'), delim, -1),
                f.substring_index(f.col('a'), delim, -4)))

# ONLY LITERAL WIDTH AND PAD ARE SUPPORTED
def test_lpad():
    gen = mk_str_gen('.{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LPAD(a, 2, " ")',
                'LPAD(a, NULL, " ")',
                'LPAD(a, 5, NULL)',
                'LPAD(a, 5, "G")',
                'LPAD(a, -1, "G")'))

# ONLY LITERAL WIDTH AND PAD ARE SUPPORTED
def test_rpad():
    gen = mk_str_gen('.{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'RPAD(a, 2, " ")',
                'RPAD(a, NULL, " ")',
                'RPAD(a, 5, NULL)',
                'RPAD(a, 5, "G")',
                'RPAD(a, -1, "G")'))

# ONLY LITERAL SEARCH PARAMS ARE SUPPORTED
def test_position():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'POSITION(NULL IN a)',
                'POSITION("Z_" IN a)',
                'POSITION("" IN a)',
                'POSITION("_" IN a)',
                'POSITION("A" IN a)'))

def test_locate():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'locate("Z", a, -1)',
                'locate("Z", a, 4)',
                'locate("A", a, 500)',
                'locate("_", a, NULL)'))

def test_contains():
    gen = mk_str_gen('.{0,3}Z?_Z?.{0,3}A?.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').contains('Z'),
                f.col('a').contains('Z_'),
                f.col('a').contains(''),
                f.col('a').contains(None)
                ))

def test_trim():
    gen = mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'TRIM(a)',
                'TRIM("Ab" FROM a)',
                'TRIM("A\ud720" FROM a)',
                'TRIM(BOTH NULL FROM a)',
                'TRIM("" FROM a)'))

def test_ltrim():
    gen = mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LTRIM(a)',
                'LTRIM("Ab", a)',
                'TRIM(LEADING "A\ud720" FROM a)',
                'TRIM(LEADING NULL FROM a)',
                'TRIM(LEADING "" FROM a)'))

def test_rtrim():
    gen = mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'RTRIM(a)',
                'RTRIM("Ab", a)',
                'TRIM(TRAILING "A\ud720" FROM a)',
                'TRIM(TRAILING NULL FROM a)',
                'TRIM(TRAILING "" FROM a)'))

def test_startswith():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').startswith('A'),
                f.col('a').startswith(''),
                f.col('a').startswith(None),
                f.col('a').startswith('A\ud720')))


def test_endswith():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').endswith('A'),
                f.col('a').endswith(''),
                f.col('a').endswith(None),
                f.col('a').endswith('A\ud720')))

def test_concat_ws_basic():
    gen = StringGen(nullable=True)
    (s1, s2) = gen_scalars(gen, 2, force_no_nulls=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat_ws("-"),
                f.concat_ws("-", f.col('a')),
                f.concat_ws(None, f.col('a')),
                f.concat_ws("-", f.col('a'), f.col('b')),
                f.concat_ws("-", f.col('a'), f.lit('')),
                f.concat_ws("*", f.col('a'), f.col('b'), f.col('a')),
                f.concat_ws("*", s1, f.col('b')),
                f.concat_ws("+", f.col('a'), s2),
                f.concat_ws("-", f.lit(None), f.lit(None)),
                f.concat_ws("-", f.lit(None).cast('string'), f.col('b')),
                f.concat_ws("+", f.col('a'), f.lit(None).cast('string')),
                f.concat_ws(None, f.col('a'), f.col('b')),
                f.concat_ws("+", f.col('a'), f.lit(''))))

def test_concat_ws_arrays():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    (s1, s2) = gen_scalars(gen, 2, force_no_nulls=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat_ws("*", f.array(f.lit('2'), f.lit(''), f.lit('3'), f.lit('Z'))),
                f.concat_ws("*", s1, s2),
                f.concat_ws("-", f.array()),
                f.concat_ws("-", f.array(), f.lit('u')),
                f.concat_ws(None, f.lit('z'), s1, f.lit('b'), s2, f.array()),
                f.concat_ws("+", f.lit('z'), s1, f.lit('b'), s2, f.array()),
                f.concat_ws("*", f.col('b'), f.lit('z')),
                f.concat_ws("*", f.lit('z'), s1, f.lit('b'), s2, f.array(), f.col('b')),
                f.concat_ws("-", f.array(f.lit(None))),
                f.concat_ws("-", f.array(f.lit('')))))

def test_concat_ws_nulls_arrays():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat_ws("*", f.lit('z'), f.array(f.lit('2'), f.lit(None), f.lit('Z'))),
                f.concat_ws("*", f.array(f.lit(None), f.lit(None))),
                f.concat_ws("*", f.array(f.lit(None), f.lit(None)), f.col('b'), f.lit('a'))))

def test_concat_ws_sql_basic():
    gen = StringGen(nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: binary_op_df(spark, gen),
            'concat_ws_table',
            'select ' +
                'concat_ws("-"), ' +
                'concat_ws("-", a), ' +
                'concat_ws(null, a), ' +
                'concat_ws("-", a, b), ' +
                'concat_ws("-", null, null), ' +
                'concat_ws("+", \'aaa\', \'bbb\', \'zzz\'), ' +
                'concat_ws(null, b, \'aaa\', \'bbb\', \'zzz\'), ' +
                'concat_ws("=", b, \'\', \'bbb\', \'zzz\'), ' +
                'concat_ws("*", b, a, cast(null as string)) from concat_ws_table')

def test_concat_ws_sql_col_sep():
    gen = StringGen(nullable=True)
    sep = StringGen('[-,*,+,!]', nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, gen, sep),
            'concat_ws_table',
            'select ' +
                'concat_ws(c, a), ' +
                'concat_ws(c, a, b), ' +
                'concat_ws(c, null, null), ' +
                'concat_ws(c, \'aaa\', \'bbb\', \'zzz\'), ' +
                'concat_ws(c, b, \'\', \'bbb\', \'zzz\'), ' +
                'concat_ws(c, b, a, cast(null as string)) from concat_ws_table')


@pytest.mark.skipif(is_databricks_runtime(),
    reason='Databricks optimizes out concat_ws call in this case')
@allow_non_gpu('ProjectExec', 'Alias', 'ConcatWs')
def test_concat_ws_sql_col_sep_only_sep_specified():
    gen = StringGen(nullable=True)
    sep = StringGen('[-,*,+,!]', nullable=True)
    assert_gpu_sql_fallback_collect(
            lambda spark: three_col_df(spark, gen, gen, sep),
            'ConcatWs',
            'concat_ws_table',
            'select ' +
                'concat_ws(c) from concat_ws_table')

def test_concat_ws_sql_arrays():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, gen, StringGen(nullable=True)),
            'concat_ws_table',
            'select ' +
                'concat_ws("-", array()), ' +
                'concat_ws(null, c, c, array(c)), ' +
                'concat_ws("-", array(), c), ' +
                'concat_ws("-", a, b), ' +
                'concat_ws("-", a, array(null, c), b, array()), ' +
                'concat_ws("-", array(null, null)), ' +
                'concat_ws("-", a, array(null), b, array()), ' +
                'concat_ws("*", array(\'2\', \'\', \'3\', \'Z\', c)) from concat_ws_table')

def test_concat_ws_sql_arrays_col_sep():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    sep = StringGen('[-,*,+,!]', nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, StringGen(nullable=True), sep),
            'concat_ws_table',
            'select ' +
                'concat_ws(c, array()) as emptyCon, ' +
                'concat_ws(c, b, b, array(b)), ' +
                'concat_ws(c, a, array(null, c), b, array()), ' +
                'concat_ws(c, array(null, null)), ' +
                'concat_ws(c, a, array(null), b, array()), ' +
                'concat_ws(c, array(\'2\', \'\', \'3\', \'Z\', b)) from concat_ws_table')

def test_concat_ws_sql_arrays_all_null_col_sep():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    sep = NullGen()
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, StringGen(nullable=True), sep),
            'concat_ws_table',
            'select ' +
                'concat_ws(c, array(null, null)), ' +
                'concat_ws(c, a, array(null), b, array()), ' +
                'concat_ws(c, b, b, array(b)) from concat_ws_table')

def test_substring():
    gen = mk_str_gen('.{0,30}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'SUBSTRING(a, 1, 5)',
                'SUBSTRING(a, 1)',
                'SUBSTRING(a, -3)',
                'SUBSTRING(a, 3, -2)',
                'SUBSTRING(a, 100)',
                'SUBSTRING(a, NULL)',
                'SUBSTRING(a, 1, NULL)',
                'SUBSTRING(a, 0, 0)'))

def test_repeat_scalar_and_column():
    gen_s = StringGen(nullable=False)
    gen_r = IntegerGen(min_val=-100, max_val=100, special_cases=[0], nullable=True)
    (s,) = gen_scalars_for_sql(gen_s, 1)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen_r).selectExpr(
                'repeat({}, a)'.format(s),
                'repeat({}, null)'.format(s)))

def test_repeat_column_and_scalar():
    gen_s = StringGen(nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen_s).selectExpr(
                'repeat(a, -10)',
                'repeat(a, 0)',
                'repeat(a, 10)',
                'repeat(a, null)'
            ))

def test_repeat_null_column_and_scalar():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.range(100).selectExpr('CAST(NULL as STRING) AS a').selectExpr(
                'repeat(a, -10)',
                'repeat(a, 0)',
                'repeat(a, 10)'
            ))

def test_repeat_column_and_column():
    gen_s = StringGen(nullable=True)
    gen_r = IntegerGen(min_val=-100, max_val=100, special_cases=[0], nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: two_col_df(spark, gen_s, gen_r).selectExpr('repeat(a, b)'))

def test_replace():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REPLACE(a, "TEST", "PROD")',
                'REPLACE(a, "T\ud720", "PROD")',
                'REPLACE(a, "", "PROD")',
                'REPLACE(a, "T", NULL)',
                'REPLACE(a, NULL, "PROD")',
                'REPLACE(a, "T", "")'))

def test_re_replace():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REGEXP_REPLACE(a, "TEST", "PROD")',
                'REGEXP_REPLACE(a, "^TEST", "PROD")',
                'REGEXP_REPLACE(a, "^TEST\\z", "PROD")',
                'REGEXP_REPLACE(a, "TEST\\z", "PROD")',
                'REGEXP_REPLACE(a, "\\zTEST", "PROD")',
                'REGEXP_REPLACE(a, "TEST\\z", "PROD")',
                'REGEXP_REPLACE(a, "\\^TEST\\z", "PROD")',
                'REGEXP_REPLACE(a, "\\^TEST\\z", "PROD")',
                'REGEXP_REPLACE(a, "TEST", "")',
                'REGEXP_REPLACE(a, "TEST", "%^[]\ud720")',
                'REGEXP_REPLACE(a, "TEST", NULL)'),
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RegExpReplace')
def test_re_replace_issue_5492():
    # https://github.com/NVIDIA/spark-rapids/issues/5492
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "[^\\\\sa-zA-Z0-9]", "x")'),
        'RegExpReplace',
        conf=_regexp_conf)

def test_re_replace_backrefs():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}TEST')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "(TEST)", "$1")',
            'REGEXP_REPLACE(a, "(TEST)", "[$0]")',
            'REGEXP_REPLACE(a, "(TEST)", "[\\1]")',
            'REGEXP_REPLACE(a, "(T)[a-z]+(T)", "[$2][$1][$0]")',
            'REGEXP_REPLACE(a, "([0-9]+)(T)[a-z]+(T)", "[$3][$2][$1]")',
            'REGEXP_REPLACE(a, "(.)([0-9]+TEST)", "$0 $1 $2")',
            'REGEXP_REPLACE(a, "(TESTT)", "\\0 \\1")'  # no match
        ),
        conf=_regexp_conf)

def test_re_replace_anchors():
    gen = mk_str_gen('.{0,2}TEST[\ud720 A]{0,5}TEST[\r\n\u0085\u2028\u2029]?') \
        .with_special_case("TEST") \
        .with_special_case("TEST\n") \
        .with_special_case("TEST\r\n") \
        .with_special_case("TEST\r")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "TEST$", "")',
            'REGEXP_REPLACE(a, "TEST$", "PROD")',
            'REGEXP_REPLACE(a, "\ud720[A-Z]+$", "PROD")',
            'REGEXP_REPLACE(a, "(\ud720[A-Z]+)$", "PROD")',
            'REGEXP_REPLACE(a, "(TEST)$", "$1")',
            'REGEXP_REPLACE(a, "^(TEST)$", "$1")',
            'REGEXP_REPLACE(a, "\\\\ATEST\\\\Z", "PROD")',
            'REGEXP_REPLACE(a, "\\\\ATEST$", "PROD")',
            'REGEXP_REPLACE(a, "^TEST\\\\Z", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\Z", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\z", "PROD")',
            'REGEXP_REPLACE(a, "\\\\zTEST", "PROD")',
            'REGEXP_REPLACE(a, "^TEST$", "PROD")',
            'REGEXP_REPLACE(a, "^TEST\\\\z", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\z", "PROD")',
        ),
        conf=_regexp_conf)

# For GPU runs, cuDF will check the range and throw exception if index is out of range
def test_re_replace_backrefs_idx_out_of_bounds():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_error(lambda spark: unary_op_df(spark, gen).selectExpr(
        'REGEXP_REPLACE(a, "(T)(E)(S)(T)", "[$5]")').collect(),
        conf=_regexp_conf,
        error_message='')

def test_re_replace_backrefs_escaped():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "(TEST)", "[\\\\$0]")',
            'REGEXP_REPLACE(a, "(TEST)", "[\\\\$1]")'),
        conf=_regexp_conf)

def test_re_replace_escaped():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "[A-Z]+", "\\\\A\\A\\\\t\\\\r\\\\n\\t\\r\\n")'),
        conf=_regexp_conf)

def test_re_replace_null():
    gen = mk_str_gen('[\u0000 ]{0,2}TE[\u0000 ]{0,2}ST[\u0000 ]{0,2}')\
        .with_special_case("\u0000")\
        .with_special_case("\u0000\u0000")
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REGEXP_REPLACE(a, "\u0000", "")',
                'REGEXP_REPLACE(a, "\000", "")',
                'REGEXP_REPLACE(a, "\00", "")',
                'REGEXP_REPLACE(a, "\x00", "")',
                'REGEXP_REPLACE(a, "\0", "")',
                'REGEXP_REPLACE(a, "\u0000", "NULL")',
                'REGEXP_REPLACE(a, "\000", "NULL")',
                'REGEXP_REPLACE(a, "\00", "NULL")',
                'REGEXP_REPLACE(a, "\x00", "NULL")',
                'REGEXP_REPLACE(a, "\0", "NULL")',
                'REGEXP_REPLACE(a, "TE\u0000ST", "PROD")',
                'REGEXP_REPLACE(a, "TE\u0000\u0000ST", "PROD")'),
        conf=_regexp_conf)

def test_length():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LENGTH(a)',
                'CHAR_LENGTH(a)',
                'CHARACTER_LENGTH(a)'))

def test_byte_length():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'BIT_LENGTH(a)', 'OCTET_LENGTH(a)'))

@incompat
def test_initcap():
    # Because we don't use the same unicode version we need to limit
    # the charicter set to something more reasonable
    # upper and lower should cover the corner cases, this is mostly to
    # see if there are issues with spaces
    gen = mk_str_gen('([aAbB1357ȺéŸ_@%-]{0,15}[ \r\n\t]{1,2}){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.initcap(f.col('a'))))

@incompat
@pytest.mark.xfail(reason='Spark initcap will not convert ŉ to ʼN')
def test_initcap_special_chars():
    gen = mk_str_gen('ŉ([aAbB13ȺéŸ]{0,5}){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.initcap(f.col('a'))))

def test_like_null():
    gen = mk_str_gen('.{0,3}a[|b*.$\r\n]{0,2}c.{0,3}')\
            .with_special_pattern('.{0,3}oo.{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').like('_'))) 

def test_like():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').like('%o%'), # turned into contains
                f.col('a').like('%a%'), # turned into contains
                f.col('a').like(''), #turned into equals
                f.col('a').like('12345678'), #turned into equals
                f.col('a').like('\\%SystemDrive\\%\\\\Users%'),
                f.col('a').like('_'),
                f.col('a').like('_oo_'),
                f.col('a').like('_oo%'),
                f.col('a').like('%oo_'),
                f.col('a').like('_\u201c%'),
                f.col('a').like('_a[d]%'),
                f.col('a').like('_a(d)%'),
                f.col('a').like('_$'),
                f.col('a').like('_$%'),
                f.col('a').like('_._'),
                f.col('a').like('_?|}{_%'),
                f.col('a').like('%a{3}%'))) 

def test_like_simple_escape():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a like "_a^d%" escape "c"',
                'a like "a_a" escape "c"',
                'a like "a%a" escape "c"',
                'a like "c_" escape "c"',
                'a like x "6162632325616263" escape "#"',
                'a like x "61626325616263" escape "#"'))
 
def test_like_complex_escape():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a like x "256f6f5f"',
                'a like x "6162632325616263" escape "#"',
                'a like x "61626325616263" escape "#"',
                'a like ""',
                'a like "_oo_"',
                'a like "_oo%"',
                'a like "%oo_"',
                'a like "_\u20AC_"',
                'a like "\\%SystemDrive\\%\\\\\\\\Users%"',
                'a like "_oo"'),
            conf={'spark.sql.parser.escapedStringLiterals': 'true'})
 
def test_regexp_replace():
    gen = mk_str_gen('[abcd]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_replace(a, "a", "A")',
                'regexp_replace(a, "[^xyz]", "A")',
                'regexp_replace(a, "([^x])|([^y])", "A")',
                'regexp_replace(a, "(?:aa)+", "A")',
                'regexp_replace(a, "a|b|c", "A")'),
        conf=_regexp_conf)

@pytest.mark.skipif(is_before_spark_320(), reason='regexp is synonym for RLike starting in Spark 3.2.0')
def test_regexp():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp(a, "a{2}")',
                'regexp(a, "a{1,3}")',
                'regexp(a, "a{1,}")',
                'regexp(a, "a[bc]d")'),
        conf=_regexp_conf)

@pytest.mark.skipif(is_before_spark_320(), reason='regexp_like is synonym for RLike starting in Spark 3.2.0')
def test_regexp_like():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_like(a, "a{2}")',
                'regexp_like(a, "a{1,3}")',
                'regexp_like(a, "a{1,}")',
                'regexp_like(a, "a[bc]d")'),
        conf=_regexp_conf)

def test_regexp_replace_character_set_negated():
    gen = mk_str_gen('[abcd]{0,3}[\r\n]{0,2}[abcd]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_replace(a, "([^a])|([^b])", "1")',
                'regexp_replace(a, "[^a]", "1")',
                'regexp_replace(a, "([^a]|[\r\n])", "1")',
                'regexp_replace(a, "[^a\r\n]", "1")',
                'regexp_replace(a, "[^a\r]", "1")',
                'regexp_replace(a, "[^a\n]", "1")',
                'regexp_replace(a, "[^\r\n]", "1")',
                'regexp_replace(a, "[^\r]", "1")',
                'regexp_replace(a, "[^\n]", "1")'),
        conf=_regexp_conf)

def test_regexp_extract():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "([0-9]+)", 1)',
                'regexp_extract(a, "([0-9])([abcd]+)", 1)',
                'regexp_extract(a, "([0-9])([abcd]+)", 2)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)\\z", 1)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)\\z", 2)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)\\z", 3)'),
        conf=_regexp_conf)

def test_regexp_extract_no_match():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)\\z", 0)',
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)\\z", 1)',
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)\\z", 2)',
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)\\z", 3)'),
        conf=_regexp_conf)

# if we determine that the index is out of range we fall back to CPU and let
# Spark take care of the error handling
@allow_non_gpu('ProjectExec', 'RegExpExtract')
def test_regexp_extract_idx_negative():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}')
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)$", -1)').collect(),
            error_message = "The specified group index cannot be less than zero",
        conf=_regexp_conf)

# if we determine that the index is out of range we fall back to CPU and let
# Spark take care of the error handling
@allow_non_gpu('ProjectExec', 'RegExpExtract')
def test_regexp_extract_idx_out_of_bounds():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}')
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)$", 4)').collect(),
            error_message = "Regex group count is 3, but the specified group index is 4",
            conf=_regexp_conf)

def test_regexp_extract_multiline():
    gen = mk_str_gen('[abcd]{2}[\r\n]{0,2}[0-9]{2}[\r\n]{0,2}[abcd]{2}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "^([a-d]*)([\r\n]*)", 2)'),
        conf=_regexp_conf)

def test_regexp_extract_multiline_negated_character_class():
    gen = mk_str_gen('[abcd]{2}[\r\n]{0,2}[0-9]{2}[\r\n]{0,2}[abcd]{2}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "^([a-d]*)([^a-z]*)([a-d]*)\\z", 2)'),
        conf=_regexp_conf)

def test_regexp_extract_idx_0():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "([0-9]+)[abcd]([abcd]+)", 0)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)\\z", 0)',
                'regexp_extract(a, "^([a-d]*)[0-9]*([a-d]*)\\z", 0)'),
        conf=_regexp_conf)

def test_regexp_hexadecimal_digits():
    gen = mk_str_gen(
        '[abcd]\\\\x00\\\\x7f\\\\x80\\\\xff\\\\x{10ffff}\\\\x{00eeee}[\\\\xa0-\\\\xb0][abcd]')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\x7f")',
                'rlike(a, "\\\\x80")',
                'rlike(a, "\\\\x{00eeee}")',
                'regexp_extract(a, "([a-d]+)\\\\xa0([a-d]+)", 1)',
                'regexp_replace(a, "\\\\xff", "")',
                'regexp_replace(a, "\\\\x{10ffff}", "")',
            ),
        conf=_regexp_conf)

def test_regexp_whitespace():
    gen = mk_str_gen('\u001e[abcd]\t\n{1,3} [0-9]\n {1,3}\x0b\t[abcd]\r\f[0-9]{0,10}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\s{2}")',
                'rlike(a, "\\s{3}")',
                'rlike(a, "[abcd]+\\s+[0-9]+")',
                'rlike(a, "\\S{3}")',
                'rlike(a, "[abcd]+\\s+\\S{2,3}")',
                'regexp_extract(a, "([a-d]+)([0-9\\s]+)([a-d]+)", 2)',
                'regexp_extract(a, "([a-d]+)(\\S+)([0-9]+)", 2)',
                'regexp_extract(a, "([a-d]+)(\\S+)([0-9]+)", 3)',
                'regexp_replace(a, "(\\s+)", "@")',
                'regexp_replace(a, "(\\S+)", "#")',
            ),
        conf=_regexp_conf)

def test_regexp_horizontal_vertical_whitespace():
    gen = mk_str_gen(
        '''\xA0\u1680\u180e[abcd]\t\n{1,3} [0-9]\n {1,3}\x0b\t[abcd]\r\f[0-9]{0,10}
            [\u2001-\u200a]{1,3}\u202f\u205f\u3000\x85\u2028\u2029
        ''')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\h{2}")',
                'rlike(a, "\\\\v{3}")',
                'rlike(a, "[abcd]+\\\\h+[0-9]+")',
                'rlike(a, "[abcd]+\\\\v+[0-9]+")',
                'rlike(a, "\\\\H")',
                'rlike(a, "\\\\V")',
                'rlike(a, "[abcd]+\\\\h+\\\\V{2,3}")',
                'regexp_extract(a, "([a-d]+)([0-9]+\\\\v)([a-d]+)", 2)',
                'regexp_extract(a, "([a-d]+)(\\\\H+)([0-9]+)", 2)',
                'regexp_extract(a, "([a-d]+)(\\\\V+)([0-9]+)", 3)',
                'regexp_replace(a, "(\\\\v+)", "@")',
                'regexp_replace(a, "(\\\\H+)", "#")',
            ),
        conf=_regexp_conf)

def test_regexp_linebreak():
    gen = mk_str_gen(
        '[abc]{1,3}\u000D\u000A[def]{1,3}[\u000A\u000B\u000C\u000D\u0085\u2028\u2029]{0,5}[123]')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\R")',
                'regexp_extract(a, "([a-d]+)(\\\\R)([a-d]+)", 1)',
                'regexp_replace(a, "\\\\R", "")',
            ),
        conf=_regexp_conf)

def test_regexp_octal_digits():
    gen = mk_str_gen('[abcd]\u0000\u0041\u007f\u0080\u00ff[\\\\xa0-\\\\xb0][abcd]')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\0177")',
                'rlike(a, "\\\\0200")',
                'rlike(a, "\\\\0101")',
                'regexp_extract(a, "([a-d]+)\\\\0240([a-d]+)", 1)',
                'regexp_replace(a, "\\\\0377", "")',
                'regexp_replace(a, "\\\\0260", "")',
            ),
        conf=_regexp_conf)

def test_regexp_replace_digit():
    gen = mk_str_gen('[a-z]{0,2}[0-9]{0,2}') \
        .with_special_case('䤫畍킱곂⬡❽ࢅ獰᳌蛫青')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'regexp_replace(a, "\\\\d", "x")',
            'regexp_replace(a, "\\\\D", "x")',
            'regexp_replace(a, "[0-9]", "x")',
            'regexp_replace(a, "[^0-9]", "x")',
        ),
        conf=_regexp_conf)

def test_regexp_replace_word():
    gen = mk_str_gen('[a-z]{0,2}[_]{0,1}[0-9]{0,2}') \
        .with_special_case('䤫畍킱곂⬡❽ࢅ獰᳌蛫青')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'regexp_replace(a, "\\\\w", "x")',
            'regexp_replace(a, "\\\\W", "x")',
            'regexp_replace(a, "[a-zA-Z_0-9]", "x")',
            'regexp_replace(a, "[^a-zA-Z_0-9]", "x")',
        ),
        conf=_regexp_conf)

def test_rlike():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a{2}"',
                'a rlike "a{1,3}"',
                'a rlike "a{1,}"',
                'a rlike "a[bc]d"'),
        conf=_regexp_conf)

def test_rlike_embedded_null():
    gen = mk_str_gen('[abcd]{1,3}')\
            .with_special_case('\u0000aaa')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a{2}"',
                'a rlike "a{1,3}"',
                'a rlike "a{1,}"',
                'a rlike "a[bc]d"'),
        conf=_regexp_conf)

def test_rlike_null_pattern():
    gen = mk_str_gen('[abcd]{1,3}')
    # Spark optimizes out `RLIKE NULL` in this test
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike NULL'))

@allow_non_gpu('ProjectExec', 'RLike')
def test_rlike_fallback_null_pattern():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_fallback_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a\u0000"'),
            'RLike',
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RLike')
def test_rlike_fallback_empty_group():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_fallback_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a()?"'),
            'RLike',
        conf=_regexp_conf)

def test_rlike_escape():
    gen = mk_str_gen('[ab]{0,2}[\\-\\+]{0,2}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a[\\\\-]"'),
        conf=_regexp_conf)

def test_rlike_multi_line():
    gen = mk_str_gen('[abc]\n[def]')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "^a"',
                'a rlike "^d"',
                'a rlike "c\\z"',
                'a rlike "e\\z"'),
        conf=_regexp_conf)

def test_rlike_missing_escape():
    gen = mk_str_gen('a[\\-\\+]')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a[-]"',
                'a rlike "a[+-]"',
                'a rlike "a[a-b-]"'),
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RLike')
def test_rlike_fallback_possessive_quantifier():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')
    assert_gpu_fallback_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a*+"'),
                'RLike',
        conf=_regexp_conf)
