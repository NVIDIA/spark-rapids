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
from conftest import is_databricks_runtime
from data_gen import *
from marks import ignore_order, allow_non_gpu, incompat
from spark_session import with_spark_session, is_before_spark_310

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

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti', 'Cross', 'FullOuter'], ids=idfn)
def test_sortmerge_join(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)


# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
# Not all join types can be translated to a broadcast join, but this tests them to be sure we
# can handle what spark is doing
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti', 'Cross', 'FullOuter'], ids=idfn)
def test_broadcast_join_right_table(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_cartesean_join(data_gen):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.crossJoin(right)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.exec.CartesianProductExec': 'true'})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.xfail(condition=is_databricks_runtime(),
    reason='https://github.com/NVIDIA/spark-rapids/issues/334')
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_cartesean_join_special_case(data_gen):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.crossJoin(right).selectExpr('COUNT(*)')
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.exec.CartesianProductExec': 'true'})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_broadcast_nested_loop_join(data_gen):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.crossJoin(broadcast(right))
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.exec.BroadcastNestedLoopJoinExec': 'true'})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_broadcast_nested_loop_join_special_case(data_gen):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.crossJoin(broadcast(right)).selectExpr('COUNT(*)')
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.exec.BroadcastNestedLoopJoinExec': 'true'})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Cross'], ids=idfn)
def test_broadcast_nested_loop_join_with_conditionals(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294 
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        return left.join(broadcast(right),
                (left.b >= right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.exec.BroadcastNestedLoopJoinExec': 'true'})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
# Not all join types can be translated to a broadcast join, but this tests them to be sure we
# can handle what spark is doing
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti', 'Cross', 'FullOuter'], ids=idfn)
def test_broadcast_join_left_table(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 250, 500)
        return broadcast(left).join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Cross'], ids=idfn)
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
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti',
    pytest.param('FullOuter', marks=pytest.mark.xfail(
        condition=not(is_before_spark_310()),
        reason='https://github.com/NVIDIA/spark-rapids/issues/575')),
    'Cross'], ids=idfn)
def test_broadcast_join_mixed(join_type):
    def do_join(spark):
        left = gen_df(spark, _mixed_df1_with_nulls, length=500)
        right = gen_df(spark, _mixed_df2_with_nulls, length=500).withColumnRenamed("a", "r_a")\
                .withColumnRenamed("b", "r_b").withColumnRenamed("c", "r_c")
        return left.join(broadcast(right), left.a.eqNullSafe(right.r_a), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.parametrize('repartition', ["true", "false"], ids=idfn)
def test_join_bucketed_table(repartition):
    def do_join(spark):
        data = [("http://fooblog.com/blog-entry-116.html", "https://fooblog.com/blog-entry-116.html"),
                ("http://fooblog.com/blog-entry-116.html", "http://fooblog.com/blog-entry-116.html")]
        resolved = spark.sparkContext.parallelize(data).toDF(['Url','ResolvedUrl'])
        feature_data = [("http://fooblog.com/blog-entry-116.html", "21")]
        feature = spark.sparkContext.parallelize(feature_data).toDF(['Url','Count'])
        feature.write.bucketBy(400, 'Url').sortBy('Url').format('parquet').mode('overwrite')\
                 .saveAsTable('featuretable')
        testurls = spark.sql("SELECT Url, Count FROM featuretable")
        if (repartition == "true"):
                return testurls.repartition(20).join(resolved, "Url", "inner")
        else:
                return testurls.join(resolved, "Url", "inner")
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.sql.autoBroadcastJoinThreshold': '-1'})

