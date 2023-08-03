# Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import locale
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, \
    assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_and_cpu_error, \
    assert_gpu_sql_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import is_before_spark_320, is_jvm_charset_utf8

if not is_jvm_charset_utf8():
    pytestmark = [pytest.mark.regexp, pytest.mark.skip(reason=str("Current locale doesn't support UTF-8, regexp support is disabled"))]
else:
    pytestmark = pytest.mark.regexp

_regexp_conf = { 'spark.rapids.sql.regexp.enabled': True }

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

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
            'split(a, "b[o]+", -1)',
            'split(a, "b[o]*", -1)',
            'split(a, "b[o]?", -1)',
            'split(a, "[o]", -2)'),
            conf=_regexp_conf)

def test_split_re_zero_limit():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]", 0)',
            'split(a, "[o:]", 0)',
            'split(a, "[^:]", 0)',
            'split(a, "[^o]", 0)',
            'split(a, "[o]{1,2}", 0)',
            'split(a, "[bf]", 0)',
            'split(a, "f[o]+", 0)',
            'split(a, "f[o]*", 0)',
            'split(a, "f[o]?", 0)',
            'split(a, "[o]", 0)'),
        conf=_regexp_conf)

def test_split_re_one_limit():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[:]", 1)',
            'split(a, "[o:]", 1)',
            'split(a, "[^:]", 1)',
            'split(a, "[^o]", 1)',
            'split(a, "[o]{1,2}", 1)',
            'split(a, "[bf]", 1)',
            'split(a, "b[o]+", 1)',
            'split(a, "b[o]*", 1)',
            'split(a, "b[o]?", 1)',
            'split(a, "[o]", 1)'),
        conf=_regexp_conf)

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
            'split(a, "f[o]+", 2)',
            'split(a, "f[o]*", 9)',
            'split(a, "f[o]?", 5)',
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
            'split(a, "[o]")',
            'split(a, "^(boo|foo):$")',
            'split(a, "(bo+|fo{2}):$")',
            'split(a, "[bf]$:")',
            'split(a, "b[o]+")',
            'split(a, "b[o]*")',
            'split(a, "b[o]?")',
            'split(a, "b^")',
            'split(a, "^[o]")'),
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
            'split(a, "\\\\%")',
            'split(a, "\\\\;")',
            'split(a, "\\\\/")',
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

# See https://github.com/NVIDIA/spark-rapids/issues/6958 for issue with zero-width match
@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_unsupported_fallback():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_sql_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen),
        'StringSplit',
        'string_split_table',
        'select ' +
        'split(a, "o*"),' +
        'split(a, "o?") from string_split_table')

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

def test_split_escaped_chars_in_character_class():
    data_gen = mk_str_gen(r'([0-9][\\\.\[\]\^\-\+]){1,4}')
    assert_gpu_and_cpu_are_equal_collect(
        # note that regexp patterns are double-escaped to support
        # passing from Python to Java
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            r'split(a, "[\\.]", 2)',
            r'split(a, "[\\[]", 2)',
            r'split(a, "[\\]]", 2)',
            r'split(a, "[\\^]", 2)',
            r'split(a, "[\\-]", 2)',
            r'split(a, "[\\+]", 2)',
            r'split(a, "[\\\\]", 2)',
        ))


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

# We have shims to support empty strings for zero-repetition patterns
# See https://github.com/NVIDIA/spark-rapids/issues/5456
def test_re_replace_repetition():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REGEXP_REPLACE(a, "[E]+", "PROD")',
                'REGEXP_REPLACE(a, "[A]+", "PROD")',
                'REGEXP_REPLACE(a, "A{0,}", "PROD")',
                'REGEXP_REPLACE(a, "T?E?", "PROD")',
                'REGEXP_REPLACE(a, "A*", "PROD")',
                'REGEXP_REPLACE(a, "A{0,5}", "PROD")',
                'REGEXP_REPLACE(a, "(A*)", "PROD")',
                'REGEXP_REPLACE(a, "(((A*)))", "PROD")',
                'REGEXP_REPLACE(a, "((A*)E?)", "PROD")',
                'REGEXP_REPLACE(a, "[A-Z]?", "PROD")'
            ),
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

def test_re_replace_escaped_chars():
    # https://github.com/NVIDIA/spark-rapids/issues/7892
    gen = mk_str_gen('.{0,5}TEST[\n\r\t\f\a\b\u001b]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "\\\\t", " ")',
            'REGEXP_REPLACE(a, "\\\\n", " ")',
            'REGEXP_REPLACE(a, "TEST\\\\n", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\r", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\f", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\a", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\b", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\e", "PROD")',
            'REGEXP_REPLACE(a, "TEST[\\\\r\\\\n]", "PROD")'),
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
            'REGEXP_REPLACE(a, "^TEST$", "PROD")',
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
                'REGEXP_REPLACE(a, "TE\u0000\u0000ST", "PROD")',
                'REGEXP_REPLACE(a, "[\x00TEST]", "PROD")',
                'REGEXP_REPLACE(a, "[TE\00ST]", "PROD")',
                'REGEXP_REPLACE(a, "[\u0000-z]", "PROD")'),
        conf=_regexp_conf)

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
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}/?[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "([0-9]+)", 1)',
                'regexp_extract(a, "([0-9])([abcd]+)", 1)',
                'regexp_extract(a, "([0-9])([abcd]+)", 2)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)$", 1)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)$", 2)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)([a-d]*)$", 3)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)\\\\/([a-d]*)", 3)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)\\\\/([a-d]*)$", 3)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)(\\\\/[a-d]*)", 3)',
                'regexp_extract(a, "^([a-d]*)([0-9]*)(\\\\/[a-d]*)$", 3)'),
        conf=_regexp_conf)

def test_regexp_extract_no_match():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)$", 0)',
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)$", 1)',
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)$", 2)',
                'regexp_extract(a, "^([0-9]+)([a-z]+)([0-9]+)$", 3)'),
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

def test_word_boundaries():
    gen = StringGen('([abc]{1,3}[\r\n\t \f]{0,2}[123]){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\b")',
                'rlike(a, "\\\\B")',
                'rlike(a, "\\\\b\\\\B")',
                'regexp_extract(a, "([a-d]+)\\\\b([e-h]+)", 1)',
                'regexp_extract(a, "([a-d]+)\\\\B", 1)',
                'regexp_replace(a, "\\\\b", "#")',
                'regexp_replace(a, "\\\\B", "#")',
            ),
        conf=_regexp_conf)
        
def test_character_classes():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}[ \n\t\r]{0,2}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "[abcd]")',
                'rlike(a, "[^\n\r]")',
                'rlike(a, "[\n-\\]")',
                'rlike(a, "[+--]")',
                'regexp_extract(a, "[123]", 0)',
                'regexp_replace(a, "[\\\\0101-\\\\0132]", "@")',
                'regexp_replace(a, "[\\\\x41-\\\\x5a]", "@")',
            ),
        conf=_regexp_conf)

def test_regexp_choice():
    gen = mk_str_gen('[abcd]{1,3}[0-9]{1,3}[abcd]{1,3}[ \n\t\r]{0,2}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "[abcd]|[123]")',
                'rlike(a, "[^\n\r]|abcd")',
                'rlike(a, "abd1a$|^ab2a")',
                'rlike(a, "[a-c]*|[\n]")',
                'rlike(a, "[a-c]+|[\n]")',
                'regexp_extract(a, "(abc1a$|^ab2ab|a3abc)", 1)',
                'regexp_extract(a, "(abc1a$|ab2ab$)", 1)',
                'regexp_extract(a, "(ab+|^ab)", 1)',
                'regexp_extract(a, "(ab*|^ab)", 1)',
                'regexp_replace(a, "[abcd]$|^abc", "@")',
                'regexp_replace(a, "[ab]$|[cd]$", "@")',
                'regexp_replace(a, "[ab]+|^cd1", "@")'
            ),
        conf=_regexp_conf)

def test_regexp_hexadecimal_digits():
    gen = mk_str_gen(
        '[abcd]\\\\x00\\\\x7f\\\\x80\\\\xff\\\\x{10ffff}\\\\x{00eeee}[\\\\xa0-\\\\xb0][abcd]')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\x7f")',
                'rlike(a, "\\\\x80")',
                'rlike(a, "[\\\\xa0-\\\\xf0]")',
                'rlike(a, "\\\\x{00eeee}")',
                'regexp_extract(a, "([a-d]+)\\\\xa0([a-d]+)", 1)',
                'regexp_extract(a, "([a-d]+)[\\\\xa0\nabcd]([a-d]+)", 1)',
                'regexp_replace(a, "\\\\xff", "@")',
                'regexp_replace(a, "[\\\\xa0-\\\\xb0]", "@")',
                'regexp_replace(a, "\\\\x{10ffff}", "@")',
            ),
        conf=_regexp_conf)

def test_regexp_whitespace():
    gen = mk_str_gen('\u001e[abcd]\t\n{1,3} [0-9]\n {1,3}\x0b\t[abcd]\r\f[0-9]{0,10}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'rlike(a, "\\\\s")',
                'rlike(a, "\\\\s{3}")',
                'rlike(a, "[abcd]+\\\\s+[0-9]+")',
                'rlike(a, "\\\\S{3}")',
                'rlike(a, "[abcd]+\\\\s+\\\\S{2,3}")',
                'regexp_extract(a, "([a-d]+)(\\\\s[0-9]+)([a-d]+)", 2)',
                'regexp_extract(a, "([a-d]+)(\\\\S+)([0-9]+)", 2)',
                'regexp_extract(a, "([a-d]+)(\\\\S+)([0-9]+)", 3)',
                'regexp_replace(a, "(\\\\s+)", "@")',
                'regexp_replace(a, "(\\\\S+)", "#")',
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
                'rlike(a, "[\\\\0240-\\\\0377]")',
                'regexp_extract(a, "([a-d]+)\\\\0240([a-d]+)", 1)',
                'regexp_extract(a, "([a-d]+)[\\\\0141-\\\\0172]([a-d]+)", 0)',
                'regexp_replace(a, "\\\\0377", "")',
                'regexp_replace(a, "\\\\0260", "")',
            ),
        conf=_regexp_conf)

def test_regexp_replace_digit():
    gen = mk_str_gen('[a-z]{0,2}[0-9]{0,2}') \
        .with_special_case('䤫畍킱곂⬡❽ࢅ獰᳌蛫青') \
        .with_special_case('a\n2\r\n3')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'regexp_replace(a, "\\\\d", "x")',
            'regexp_replace(a, "\\\\D", "x")',
            'regexp_replace(a, "[0-9]", "x")',
            'regexp_replace(a, "[^0-9]", "x")',
            'regexp_replace(a, "[\\\\d]", "x")',
            'regexp_replace(a, "[a\\\\d]{0,2}", "x")',
        ),
        conf=_regexp_conf)

def test_regexp_replace_word():
    gen = mk_str_gen('[a-z]{0,2}[_]{0,1}[0-9]{0,2}') \
        .with_special_case('䤫畍킱곂⬡❽ࢅ獰᳌蛫青') \
        .with_special_case('a\n2\r\n3')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'regexp_replace(a, "\\\\w", "x")',
            'regexp_replace(a, "\\\\W", "x")',
            'regexp_replace(a, "[a-zA-Z_0-9]", "x")',
            'regexp_replace(a, "[^a-zA-Z_0-9]", "x")',
        ),
        conf=_regexp_conf)

def test_predefined_character_classes():
    gen = mk_str_gen('[a-zA-Z]{0,2}[\r\n!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]{0,2}[0-9]{0,2}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'regexp_replace(a, "\\\\p{Lower}", "x")',
            'regexp_replace(a, "\\\\p{Upper}", "x")',
            'regexp_replace(a, "\\\\p{ASCII}", "x")',
            'regexp_replace(a, "\\\\p{Alpha}", "x")',
            'regexp_replace(a, "\\\\p{Digit}", "x")',
            'regexp_replace(a, "\\\\p{Alnum}", "x")',
            'regexp_replace(a, "\\\\p{Punct}", "x")',
            'regexp_replace(a, "\\\\p{Graph}", "x")',
            'regexp_replace(a, "\\\\p{Print}", "x")',
            'regexp_replace(a, "\\\\p{Blank}", "x")',
            'regexp_replace(a, "\\\\p{Cntrl}", "x")',
            'regexp_replace(a, "\\\\p{XDigit}", "x")',
            'regexp_replace(a, "\\\\p{Space}", "x")',
            'regexp_replace(a, "\\\\P{Lower}", "x")',
            'regexp_replace(a, "\\\\P{Upper}", "x")',
            'regexp_replace(a, "\\\\P{ASCII}", "x")',
            'regexp_replace(a, "\\\\P{Alpha}", "x")',
            'regexp_replace(a, "\\\\P{Digit}", "x")',
            'regexp_replace(a, "\\\\P{Alnum}", "x")',
            'regexp_replace(a, "\\\\P{Punct}", "x")',
            'regexp_replace(a, "\\\\P{Graph}", "x")',
            'regexp_replace(a, "\\\\P{Print}", "x")',
            'regexp_replace(a, "\\\\P{Blank}", "x")',
            'regexp_replace(a, "\\\\P{Cntrl}", "x")',
            'regexp_replace(a, "\\\\P{XDigit}", "x")',
            'regexp_replace(a, "\\\\P{Space}", "x")',
        ),
        conf=_regexp_conf)

def test_rlike():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a{2}"',
                'a rlike "a{1,3}"',
                'a rlike "a{1,}"',
                'a rlike "a[bc]d"',
                'a rlike "a[bc]d"',
                'a rlike "^[a-d]*$"'),
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
def test_rlike_fallback_empty_group():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_fallback_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a()?"'),
            'RLike',
        conf=_regexp_conf)

def test_rlike_escape():
    gen = mk_str_gen('[ab]{0,2};?[\\-\\+]{0,2}/?')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a[\\\\-]"',
                'a rlike "a\\\\;[\\\\-]"',
                'a rlike "a[\\\\-]\\\\/"',
                'a rlike "b\\\\;[\\\\-]\\\\/"'),
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

def test_regexp_extract_all_idx_zero():
    gen = mk_str_gen('[abcd]{0,3}[0-9]{0,3}-[0-9]{0,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract_all(a, "([a-d]+).*([0-9])", 0)',
                'regexp_extract_all(a, "(a)(b)", 0)',
                'regexp_extract_all(a, "([a-z0-9]([abcd]))", 0)',
                'regexp_extract_all(a, "(\\\\d+)-(\\\\d+)", 0)',
            ),
        conf=_regexp_conf)

def test_regexp_extract_all_idx_positive():
    gen = mk_str_gen('[abcd]{0,3}[0-9]{0,3}-[0-9]{0,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract_all(a, "([a-d]+).*([0-9])", 1)',
                'regexp_extract_all(a, "(a)(b)", 2)',
                'regexp_extract_all(a, "([a-z0-9]((([abcd](\\\\d?)))))", 3)',
                'regexp_extract_all(a, "(\\\\d+)-(\\\\d+)", 2)',
            ),
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RegExpExtractAll')
def test_regexp_extract_all_idx_negative():
    gen = mk_str_gen('[abcd]{0,3}')
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract_all(a, "(a)", -1)'
            ).collect(),
        error_message="The specified group index cannot be less than zero",
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RegExpExtractAll')
def test_regexp_extract_all_idx_out_of_bounds():
    gen = mk_str_gen('[a-d]{1,2}.{0,1}[0-9]{1,2}')
    assert_gpu_and_cpu_error(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract_all(a, "([a-d]+).*([0-9])", 3)'
            ).collect(),
        error_message="Regex group count is 2, but the specified group index is 3",
        conf=_regexp_conf)

def test_rlike_unicode_support():
    gen = mk_str_gen('a[\ud720\ud800\ud900]')\
        .with_special_case('a䤫畍킱곂⬡❽ࢅ獰᳌蛫青')

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'a rlike "a*"',
            'a rlike "a\ud720"',
            'a rlike "a\ud720.+$"'),
        conf=_regexp_conf)

def test_regexp_replace_unicode_support():
    gen = mk_str_gen('TEST[85\ud720\ud800\ud900]')\
        .with_special_case('TEST䤫畍킱곂⬡❽ࢅ獰᳌蛫青')

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "TEST\ud720", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\b", "PROD")',
            'REGEXP_REPLACE(a, "TEST\\\\B", "PROD")',
            'REGEXP_REPLACE(a, "TEST䤫", "PROD")',
            'REGEXP_REPLACE(a, "TEST[䤫]", "PROD")',
            'REGEXP_REPLACE(a, "TEST.*\\\\d", "PROD")',
            'REGEXP_REPLACE(a, "TEST[85]*$", "PROD")',
            'REGEXP_REPLACE(a, "TEST.+$", "PROD")',
            'REGEXP_REPLACE("TEST䤫", "TEST.+$", "PROD")',
        ),
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RegExpReplace')
def test_regexp_replace_fallback_configured_off():
    gen = mk_str_gen('[abcdef]{0,2}')

    conf = { 'spark.rapids.sql.regexp.enabled': 'false' }

    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "[a-z]+", "PROD")',
            'REGEXP_REPLACE(a, "aa", "PROD")',
        ),
        cpu_fallback_class_name='RegExpReplace',
        conf=conf
    )


@allow_non_gpu('ProjectExec')
def test_unsupported_fallback_regexp_extract():
    gen = mk_str_gen('[abcdef]{0,2}')
    regex_gen = StringGen('\[a-z\]\+')
    num_gen = IntegerGen(min_val=0, max_val=0, special_cases=[])

    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [
                ("a", gen),
                ("reg_ex", regex_gen),
                ("num", num_gen)], length=10).selectExpr(sql_text),
        'RegExpExtract')

    assert_gpu_did_fallback('REGEXP_EXTRACT(a, "[a-z]+", num)')
    assert_gpu_did_fallback('REGEXP_EXTRACT(a, reg_ex, 0)')
    assert_gpu_did_fallback('REGEXP_EXTRACT(a, reg_ex, num)')
    assert_gpu_did_fallback('REGEXP_EXTRACT("PROD", "[a-z]+", num)')
    assert_gpu_did_fallback('REGEXP_EXTRACT("PROD", reg_ex, 0)')
    assert_gpu_did_fallback('REGEXP_EXTRACT("PROD", reg_ex, num)')


@allow_non_gpu('ProjectExec')
def test_unsupported_fallback_regexp_extract_all():
    gen = mk_str_gen('[abcdef]{0,2}')
    regex_gen = StringGen('\[a-z\]\+')
    num_gen = IntegerGen(min_val=0, max_val=0, special_cases=[])
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [
                ("a", gen),
                ("reg_ex", regex_gen),
                ("num", num_gen)], length=10).selectExpr(sql_text),
            'RegExpExtractAll')

    assert_gpu_did_fallback('REGEXP_EXTRACT_ALL(a, "[a-z]+", num)')
    assert_gpu_did_fallback('REGEXP_EXTRACT_ALL(a, reg_ex, 0)')
    assert_gpu_did_fallback('REGEXP_EXTRACT_ALL(a, reg_ex, num)')
    assert_gpu_did_fallback('REGEXP_EXTRACT_ALL("PROD", "[a-z]+", num)')
    assert_gpu_did_fallback('REGEXP_EXTRACT_ALL("PROD", reg_ex, 0)')
    assert_gpu_did_fallback('REGEXP_EXTRACT_ALL("PROD", reg_ex, num)')


@allow_non_gpu('ProjectExec', 'RegExpReplace')
def test_unsupported_fallback_regexp_replace():
    gen = mk_str_gen('[abcdef]{0,2}')
    regex_gen = StringGen('\[a-z\]\+')
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [
                ("a", gen),
                ("reg_ex", regex_gen)], length=10).selectExpr(sql_text),
            'RegExpReplace')

    assert_gpu_did_fallback('REGEXP_REPLACE(a, "[a-z]+", a)')
    assert_gpu_did_fallback('REGEXP_REPLACE(a, reg_ex, "PROD")')
    assert_gpu_did_fallback('REGEXP_REPLACE(a, reg_ex, a)')
    assert_gpu_did_fallback('REGEXP_REPLACE("PROD", "[a-z]+", a)')
    assert_gpu_did_fallback('REGEXP_REPLACE("PROD", reg_ex, "PROD")')
    assert_gpu_did_fallback('REGEXP_REPLACE("PROD", reg_ex, a)')


@pytest.mark.parametrize("regexp_enabled", ['true', 'false'])
def test_regexp_replace_simple(regexp_enabled):
    gen = mk_str_gen('[abcdef]{0,2}')

    conf = { 'spark.rapids.sql.regexp.enabled': regexp_enabled }

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "aa", "PROD")',
            'REGEXP_REPLACE(a, "ab", "PROD")',
            'REGEXP_REPLACE(a, "ae", "PROD")',
            'REGEXP_REPLACE(a, "bc", "PROD")',
            'REGEXP_REPLACE(a, "fa", "PROD")'
        ),
        conf=conf
    )

@pytest.mark.parametrize("regexp_enabled", ['true', 'false'])
def test_regexp_replace_multi_optimization(regexp_enabled):
    gen = mk_str_gen('[abcdef]{0,2}')

    conf = { 'spark.rapids.sql.regexp.enabled': regexp_enabled }

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "aa|bb", "PROD")',
            'REGEXP_REPLACE(a, "(aa)|(bb)", "PROD")',
            'REGEXP_REPLACE(a, "aa|bb|cc", "PROD")',
            'REGEXP_REPLACE(a, "(aa)|(bb)|(cc)", "PROD")',
            'REGEXP_REPLACE(a, "aa|bb|cc|dd", "PROD")',
            'REGEXP_REPLACE(a, "(aa|bb)|(cc|dd)", "PROD")',
            'REGEXP_REPLACE(a, "aa|bb|cc|dd|ee", "PROD")',
            'REGEXP_REPLACE(a, "aa|bb|cc|dd|ee|ff", "PROD")'
        ),
        conf=conf
    )


def test_regexp_split_unicode_support():
    data_gen = mk_str_gen('([bf]o{0,2}青){1,7}') \
        .with_special_case('boo青and青foo')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[青]", -1)',
            'split(a, "[o青]", -1)',
            'split(a, "[^青]", -1)',
            'split(a, "[^o]", -1)',
            'split(a, "[o]{1,2}", -1)',
            'split(a, "[bf]", -1)',
            'split(a, "[o]", -2)'),
            conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RLike')
def test_regexp_memory_fallback():
    gen = StringGen('test')
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'a rlike "a{6}"',
            'a rlike "a{6,}"',
            'a rlike "(?:ab){0,3}"',
            'a rlike "(?:12345)?"',
            'a rlike "(?:12345)+"',
            'a rlike "(?:123456)*"',
            'a rlike "a{1,6}"',
            'a rlike "abcdef"',
            'a rlike "(1)(2)(3)"',
            'a rlike "1|2|3|4|5|6"'
        ),
        cpu_fallback_class_name='RLike',
        conf={ 
            'spark.rapids.sql.regexp.enabled': True,
            'spark.rapids.sql.regexp.maxStateMemoryBytes': '10',
            'spark.rapids.sql.batchSizeBytes': '20' # 1 row in the batch
        }
    )

def test_regexp_memory_ok():
    gen = StringGen('test')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'a rlike "a{6}"',
            'a rlike "a{6,}"',
            'a rlike "(?:ab){0,3}"',
            'a rlike "(?:12345)?"',
            'a rlike "(?:12345)+"',
            'a rlike "(?:123456)*"',
            'a rlike "a{1,6}"',
            'a rlike "abcdef"',
            'a rlike "(1)(2)(3)"',
            'a rlike "1|2|3|4|5|6"'
        ),
        conf={ 
            'spark.rapids.sql.regexp.enabled': True,
            'spark.rapids.sql.regexp.maxStateMemoryBytes': '12',
            'spark.rapids.sql.batchSizeBytes': '20' # 1 row in the batch
        }
    )

def test_re_replace_all():
    """
    regression test for https://github.com/NVIDIA/spark-rapids/issues/8323
    """
    gen = mk_str_gen('[a-z]{0,2}\n{0,2}[a-z]{0,2}\n{0,2}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, ".*$", "PROD", 1)'),
        conf=_regexp_conf)
