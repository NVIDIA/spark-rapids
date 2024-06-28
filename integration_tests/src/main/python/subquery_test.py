# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error
from data_gen import *
from marks import *

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_basic_gens, ids=idfn)
def test_scalar_subquery_basics(data_gen):
    # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, [('a', data_gen)], num_slices=1),
        'table',
        '''select a, (select last(a) from table)
        from table
        where a > (select first(a) from table)
        ''')


@ignore_order(local=True)
@pytest.mark.parametrize('basic_gen', all_basic_gens, ids=idfn)
def test_scalar_subquery_struct(basic_gen):
    # single-level struct
    gen = [('ss', StructGen([['a', basic_gen], ['b', basic_gen]]))]
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, gen, num_slices=1),
        'table',
        '''select ss, (select last(ss) from table)
        from table
        where (select first(ss) from table).b > ss.a
        ''')
    # nested struct
    gen = [('ss', StructGen([['child', StructGen([['c0', basic_gen]])]]))]
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, gen, num_slices=1),
        'table',
        '''select ss, (select last(ss) from table)
        from table
        where (select first(ss) from table)['child']['c0'] > ss.child.c0
        ''')
    # struct of array
    # Note: The test query accesses the first two elements of the array.  The datagen is set up
    #       to generate arrays of a minimum of two elements.  Otherwise, the test will fail in ANSI mode.
    #       No meaningful test coverage is lost.  Accessing invalid indices of arrays is already tested
    #       as part of array_test.py::test_array_item_ansi_fail_invalid_index.
    gen = [('ss', StructGen([['arr', ArrayGen(basic_gen, min_length=2)]]))]
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, gen, length=100, num_slices=1),
        'table',
        '''select sort_array(ss.arr), sort_array((select last(ss) from table)['arr'])
        from table
        where (select first(ss) from table).arr[0] > ss.arr[1]
        ''')


@ignore_order(local=True)
@pytest.mark.parametrize('is_ansi_enabled', [False, True])
@pytest.mark.parametrize('basic_gen', all_basic_gens, ids=idfn)
def test_scalar_subquery_array(is_ansi_enabled, basic_gen):
    """
    For this test, all the array inputs are sized so that ArrayIndexOutOfBounds conditions are
    avoided.  This is to ensure that the tests don't fail with exceptions in ANSI mode.
    Note that no meaningful test coverage is lost here.  ArrayIndexOutOfBounds exceptions are
    already tested as part of array_test.py::test_array_item_ansi_fail_invalid_index.
    """
    conf = {'spark.sql.ansi.enabled': is_ansi_enabled}

    # single-level array
    test_array_gen = ArrayGen(basic_gen, min_length=1 if is_ansi_enabled else 0)
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, [('arr', test_array_gen)], num_slices=1),
        'table',
        '''select sort_array(arr),
                  sort_array((select last(arr) from table))
        from table
        where (select first(arr) from table)[0] > arr[0]
        ''',
        conf=conf)

    # nested array
    test_array_gen = ArrayGen(ArrayGen(basic_gen, min_length=2 if is_ansi_enabled else 0),
                              min_length=11 if is_ansi_enabled else 0)
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, [('arr', test_array_gen)], length=100, num_slices=1),
        'table',
        '''select sort_array(arr[10]),
                  sort_array((select last(arr) from table)[10])
        from table
        where (select first(arr) from table)[0][1] > arr[0][1]
        ''',
        conf=conf)

    # array of struct
    test_array_gen = ArrayGen(StructGen([['a', basic_gen]]), min_length=11 if is_ansi_enabled else 0)
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, [('arr', test_array_gen)], length=100, num_slices=1),
        'table',
        '''select arr[10].a, (select last(arr) from table)[10].a
        from table
        where (select first(arr) from table)[0].a > arr[0].a
        ''',
        conf=conf)


@ignore_order(local=True)
def test_scalar_subquery_array_ansi_mode_failures():
    """
    This tests the case where the array scalar returned from a subquery might be indexed into
    with an out-of-range index value. With ANSI mode enabled, an exception is expected.

    A more thorough test for invalid indices is done in array_test.py::test_array_item_ansi_fail_invalid_index,
    and is out of the scope of this test.
    """

    def test_function(spark):
        table_name = 'scalar_subquery_array_input'
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        df = gen_df(spark, [('arr', ArrayGen(long_gen))], num_slices=1)
        df.createOrReplaceTempView(table_name)
        query = '''
          SELECT SORT_ARRAY(arr),
                 SORT_ARRAY((SELECT LAST(arr) FROM {}))
          FROM {}
          WHERE (SELECT FIRST(arr) FROM {})[0] > arr[0]
        '''.format(table_name, table_name, table_name)
        return spark.sql(query)

    assert_gpu_and_cpu_error(
        lambda spark: test_function(spark).collect(),
        conf=ansi_enabled_conf,
        error_message='ArrayIndexOutOfBoundsException')


@ignore_order(local=True)
def test_scalar_subquery_map():
    # Note: For this test, all the array inputs are sized so that ArrayIndexOutOfBounds conditions are
    #       avoided.  This is to ensure that the tests don't fail with exceptions in ANSI mode.
    #       Note that no meaningful test coverage is lost here.  ArrayIndexOutOfBounds exceptions are
    #       already tested as part of array_test.py::test_array_item_ansi_fail_invalid_index.
    map_gen = map_string_string_gen[0]
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, [('kv', map_gen)], length=100, num_slices=1),
        'table',
        '''select kv['key_0'],
                  (select first(kv) from table)['key_1'],
                  (select last(kv) from table)['key_2']
        from table
        ''')
    # array of map
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, [('arr', ArrayGen(map_gen, min_length=1))], length=100, num_slices=1),
        'table',
        '''select arr[0]['key_0'],
                  (select first(arr) from table)[0]['key_1'],
                  (select last(arr[0]) from table)['key_2']
        from table
        ''')
    # struct of map
    assert_gpu_and_cpu_are_equal_sql(
        # Fix num_slices at 1 to make sure that first/last returns same results under CPU and GPU.
        lambda spark: gen_df(spark, [('ss', StructGen([['kv', map_gen]]))], length=100, num_slices=1),
        'table',
        '''select ss['kv']['key_0'],
                  (select first(ss) from table)['kv']['key_1'],
                  (select last(ss.kv) from table)['key_2']
        from table
        ''')
