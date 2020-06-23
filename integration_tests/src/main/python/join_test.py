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

all_gen_no_nulls = [StringGen(nullable=False), ByteGen(nullable=False),
        ShortGen(nullable=False), IntegerGen(nullable=False), LongGen(nullable=False),
        BooleanGen(nullable=False), DateGen(nullable=False), TimestampGen(nullable=False),
        pytest.param(FloatGen(nullable=False), marks=[incompat]),
        pytest.param(DoubleGen(nullable=False), marks=[incompat])]

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

# Once https://github.com/NVIDIA/spark-rapids/issues/280 is fixed this test should be deleted
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen_no_nulls, ids=idfn)
@pytest.mark.parametrize('join_type', ['FullOuter'], ids=idfn)
def test_sortmerge_join_no_nulls(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

# local sort becasue of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti',
    pytest.param('FullOuter', marks=[pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/280')])], ids=idfn)
def test_sortmerge_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)


# Once https://github.com/NVIDIA/spark-rapids/issues/280 is fixed this test should be deleted
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen_no_nulls, ids=idfn)
@pytest.mark.parametrize('join_type', ['FullOuter'], ids=idfn)
def test_broadcast_join_no_nulls(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# For tests which include broadcast joins, right table is broadcasted and hence it is
# made smaller than left table.
# local sort becasue of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti',
    pytest.param('FullOuter', marks=[pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/280')])], ids=idfn)
def test_broadcast_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort becasue of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner'], ids=idfn)
def test_broadcast_join_with_conditionals(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right),
                   (left.a == right.r_a) & (left.b >= right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)


_mixed_df1_with_nulls = [('a', RepeatSeqGen(LongGen(nullable=(True, 20.0)), length= 10)),
                         ('b', IntegerGen()), ('c', LongGen())]
_mixed_df2_with_nulls = [('a', RepeatSeqGen(LongGen(nullable=(True, 20.0)), length= 10)),
                         ('b', StringGen()), ('c', BooleanGen())]

@ignore_order
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti', 'FullOuter'], ids=idfn)
def test_broadcast_join_mixed(join_type):
    def do_join(spark):
        left = gen_df(spark, _mixed_df1_with_nulls, length=500)
        right = gen_df(spark, _mixed_df2_with_nulls, length=500).withColumnRenamed("a", "r_a")\
                .withColumnRenamed("b", "r_b").withColumnRenamed("c", "r_c")
        return left.join(broadcast(right), left.a.eqNullSafe(right.r_a), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)
