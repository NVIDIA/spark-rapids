# Copyright (c) 2020, NVIDIA CORPORATION.
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
from pyspark.sql.functions import broadcast
from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import ignore_order, allow_non_gpu, incompat

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           BooleanGen(), DateGen(), TimestampGen(),
           pytest.param(FloatGen(), marks=[incompat]),
           pytest.param(DoubleGen(), marks=[incompat])]

double_gen = [pytest.param(DoubleGen(), marks=[incompat])]

_sortmerge_join_conf = {'spark.sql.autoBroadcastJoinThreshold': '-1',
                        'spark.sql.join.preferSortMergeJoin': 'True',
                        'spark.sql.shuffle.partitions': '2'
                       }

def create_df(spark, data_gen, left_length, right_length):
    left = binary_op_df(spark, data_gen, length=left_length)
    right = binary_op_df(spark, data_gen, length=right_length).withColumnRenamed("a", "r_a")\
            .withColumnRenamed("b", "r_b")
    return left, right

@ignore_order
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_sortmerge_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 50)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)


@ignore_order
@pytest.mark.parametrize('data_gen', double_gen, ids=idfn)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/156')
@pytest.mark.parametrize('join_type', ['Inner', 'LeftSemi'], ids=idfn)
def test_sortmerge_join_fail(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 100, 100)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

# For tests which include broadcast joins, right table is broadcasted and hence it is
# made smaller than left table.
@ignore_order
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_broadcast_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 60, 30)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)


@ignore_order
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner'], ids=idfn)
def test_broadcast_join_with_conditionals(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 60, 30)
        return left.join(broadcast(right),
                   (left.a == right.r_a) & (left.b >= right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)


_mixed_df1_with_nulls = [('a', RepeatSeqGen(LongGen(nullable=(True, 20.0)), length= 10)),
                         ('b', IntegerGen()), ('c', LongGen())]
_mixed_df2_with_nulls = [('a', RepeatSeqGen(LongGen(nullable=(True, 20.0)), length= 10)),
                         ('b', StringGen()), ('c', BooleanGen())]

@ignore_order
@pytest.mark.parametrize('join_type', ['Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_broadcast_join_mixed(join_type):
    def do_join(spark):
        left = gen_df(spark, _mixed_df1_with_nulls, length=60)
        right = gen_df(spark, _mixed_df2_with_nulls, length=30).withColumnRenamed("a", "r_a")\
                .withColumnRenamed("b", "r_b").withColumnRenamed("c", "r_c")
        return left.join(broadcast(right), left.a.eqNullSafe(right.r_a), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)
