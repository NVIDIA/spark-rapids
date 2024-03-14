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
from _pytest.mark.structures import ParameterSet
from pyspark.sql.functions import array_contains, broadcast, col
from pyspark.sql.types import *
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import is_databricks_runtime, is_emr_runtime, is_not_utc
from data_gen import *
from marks import ignore_order, allow_non_gpu, incompat, validate_execs_in_gpu_plan
from spark_session import with_cpu_session, is_before_spark_330, is_databricks_runtime

pytestmark = [pytest.mark.nightly_resource_consuming_test]

all_non_symmetric_join_types = ['Left', 'Right', 'LeftSemi', 'LeftAnti', 'Cross']
all_symmetric_join_types = ['Inner', 'FullOuter']
all_join_types = all_non_symmetric_join_types + all_symmetric_join_types

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           BooleanGen(), DateGen(), TimestampGen(), null_gen,
           pytest.param(FloatGen(), marks=[incompat]),
           pytest.param(DoubleGen(), marks=[incompat])] + orderable_decimal_gens

all_gen_no_nulls = [StringGen(nullable=False), ByteGen(nullable=False),
        ShortGen(nullable=False), IntegerGen(nullable=False), LongGen(nullable=False),
        BooleanGen(nullable=False), DateGen(nullable=False), TimestampGen(nullable=False),
        pytest.param(FloatGen(nullable=False), marks=[incompat]),
        pytest.param(DoubleGen(nullable=False), marks=[incompat])]

basic_struct_gen = StructGen([
    ['child' + str(ind), sub_gen]
    for ind, sub_gen in enumerate([StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
                                   BooleanGen(), DateGen(), TimestampGen(), null_gen, decimal_gen_64bit])],
    nullable=True)

basic_struct_gen_with_no_null_child = StructGen([
    ['child' + str(ind), sub_gen]
    for ind, sub_gen in enumerate([StringGen(nullable=False), ByteGen(nullable=False),
                                   ShortGen(nullable=False), IntegerGen(nullable=False), LongGen(nullable=False),
                                   BooleanGen(nullable=False), DateGen(nullable=False), TimestampGen(nullable=False)])],
    nullable=True)

basic_struct_gen_with_floats = StructGen([['child0', FloatGen()], ['child1', DoubleGen()]], nullable=False)

nested_2d_struct_gens = StructGen([['child0', basic_struct_gen]], nullable=False)
nested_3d_struct_gens = StructGen([['child0', nested_2d_struct_gens]], nullable=False)
struct_gens = [basic_struct_gen, basic_struct_gen_with_no_null_child, nested_2d_struct_gens, nested_3d_struct_gens]

basic_nested_gens = single_level_array_gens + map_string_string_gen + [all_basic_struct_gen, binary_gen]

# data types supported by AST expressions in joins
join_ast_gen = [
    boolean_gen, byte_gen, short_gen, int_gen, long_gen, date_gen, timestamp_gen, string_gen
]

# data types not supported by AST expressions in joins
join_no_ast_gen = [
    pytest.param(FloatGen(), marks=[incompat]), pytest.param(DoubleGen(), marks=[incompat]),
    null_gen, decimal_gen_64bit
]

# Types to use when running joins on small batches. Small batch joins can take a long time
# to run and are mostly redundant with the normal batch size test, so we only run these on a
# set of representative types rather than all types.

join_small_batch_gens = [ StringGen(), IntegerGen(), orderable_decimal_gen_128bit ]
cartesian_join_small_batch_gens = join_small_batch_gens + [basic_struct_gen, ArrayGen(string_gen)]

_sortmerge_join_conf = {'spark.sql.autoBroadcastJoinThreshold': '-1',
                        'spark.sql.join.preferSortMergeJoin': 'True',
                        'spark.sql.shuffle.partitions': '2',
                        }

# For spark to insert a shuffled hash join it has to be enabled with
# "spark.sql.join.preferSortMergeJoin" = "false" and both sides have to
# be larger than a broadcast hash join would want
# "spark.sql.autoBroadcastJoinThreshold", but one side has to be smaller
# than the number of splits * broadcast threshold and also be at least
# 3 times smaller than the other side.  So it is not likely to happen
# unless we can give it some help.
_hash_join_conf = {'spark.sql.autoBroadcastJoinThreshold': '160',
                   'spark.sql.join.preferSortMergeJoin': 'false',
                   'spark.sql.shuffle.partitions': '2',
                  }

def create_df(spark, data_gen, left_length, right_length):
    left = binary_op_df(spark, data_gen, length=left_length)
    right = binary_op_df(spark, data_gen, length=right_length).withColumnRenamed("a", "r_a")\
            .withColumnRenamed("b", "r_b")
    return left, right

# create a dataframe with 2 columns where one is a nested type to be passed
# along but not used as key and the other can be used as join key
def create_ridealong_df(spark, key_data_gen, data_gen, left_length, right_length):
    left = two_col_df(spark, key_data_gen, data_gen, length=left_length).withColumnRenamed("a", "key")
    right = two_col_df(spark, key_data_gen, data_gen, length=right_length).withColumnRenamed("a", "r_key")\
            .withColumnRenamed("b", "r_b")
    return left, right

# Takes a sequence of list-of-generator and batch size string pairs and returns the
# test parameters, using the batch size setting for each corresponding data generator.
def join_batch_size_test_params(*args):
    params = []
    for (data_gens, batch_size) in args:
        for obj in data_gens:
            if isinstance(obj, ParameterSet):
                params += [ pytest.param(v, batch_size, marks=obj.marks) for v in obj.values ]
            else:
                params += [ pytest.param(obj, batch_size) ]
    return params

@ignore_order(local=True)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", ["true", "false"], ids=idfn)
def test_right_broadcast_nested_loop_join_without_condition_empty(join_type, aqe_enabled):
    def do_join(spark):
        left, right = create_df(spark, long_gen, 50, 0)
        return left.join(broadcast(right), how=join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={ "spark.sql.adaptive.enabled": aqe_enabled })

@ignore_order(local=True)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", ["true", "false"], ids=idfn)
def test_left_broadcast_nested_loop_join_without_condition_empty(join_type, aqe_enabled):
    def do_join(spark):
        left, right = create_df(spark, long_gen, 0, 50)
        return left.join(broadcast(right), how=join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={ "spark.sql.adaptive.enabled": aqe_enabled })

@ignore_order(local=True)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", ["true", "false"], ids=idfn)
def test_broadcast_nested_loop_join_without_condition_empty(join_type, aqe_enabled):
    def do_join(spark):
        left, right = create_df(spark, long_gen, 0, 0)
        return left.join(broadcast(right), how=join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={ "spark.sql.adaptive.enabled": aqe_enabled })

@ignore_order(local=True)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_right_broadcast_nested_loop_join_without_condition_empty_small_batch(join_type):
    def do_join(spark):
        left, right = create_df(spark, long_gen, 50, 0)
        return left.join(broadcast(right), how=join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.sql.adaptive.enabled': 'true'})

@ignore_order(local=True)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_empty_broadcast_hash_join(join_type):
    def do_join(spark):
        left, right = create_df(spark, long_gen, 50, 0)
        return left.join(right.hint("broadcast"), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.sql.adaptive.enabled': 'true'})


# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen,batch_size', join_batch_size_test_params(
    (all_gen, '1g'),
    (join_small_batch_gens, '1000')), ids=idfn)
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
def test_sortmerge_join(data_gen, join_type, batch_size):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    conf = copy_and_update(_sortmerge_join_conf, {'spark.rapids.sql.batchSizeBytes': batch_size})
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=conf)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', basic_nested_gens + [decimal_gen_128bit], ids=idfn)
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
def test_sortmerge_join_ridealong(data_gen, join_type):
    def do_join(spark):
        left, right = create_ridealong_df(spark, short_gen, data_gen, 500, 500)
        return left.join(right, left.key == right.r_key, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

# For floating point values the normalization is done using a higher order function. We could probably work around this
# for now it falls back to the CPU
@allow_non_gpu('SortMergeJoinExec', 'SortExec', 'ArrayTransform', 'LambdaFunction',
        'NamedLambdaVariable', 'NormalizeNaNAndZero', 'ShuffleExchangeExec', 'HashPartitioning',
        *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', single_level_array_gens + [binary_gen], ids=idfn)
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
def test_sortmerge_join_wrong_key_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_fallback_collect(do_join, 'SortMergeJoinExec', conf=_sortmerge_join_conf)

# For spark to insert a shuffled hash join it has to be enabled with
# "spark.sql.join.preferSortMergeJoin" = "false" and both sides have to
# be larger than a broadcast hash join would want
# "spark.sql.autoBroadcastJoinThreshold", but one side has to be smaller
# than the number of splits * broadcast threshold and also be at least
# 3 times smaller than the other side.  So it is not likely to happen
# unless we can give it some help. Parameters are setup to try to make
# this happen, if test fails something might have changed related to that.
def hash_join_ridealong(data_gen, join_type, sub_part_enabled):
    def do_join(spark):
        left, right = create_ridealong_df(spark, short_gen, data_gen, 50, 500)
        return left.join(right, left.key == right.r_key, join_type)
    _all_conf = copy_and_update(_hash_join_conf, {
        "spark.rapids.sql.test.subPartitioning.enabled": sub_part_enabled
    })
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_all_conf)

@validate_execs_in_gpu_plan('GpuShuffledHashJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', basic_nested_gens + [decimal_gen_128bit], ids=idfn)
@pytest.mark.parametrize('join_type', all_non_symmetric_join_types, ids=idfn)
@pytest.mark.parametrize('sub_part_enabled', ['false', 'true'], ids=['SubPartition_OFF', 'SubPartition_ON'])
@allow_non_gpu(*non_utc_allow)
def test_hash_join_ridealong_non_symmetric(data_gen, join_type, sub_part_enabled):
    hash_join_ridealong(data_gen, join_type, sub_part_enabled)

@validate_execs_in_gpu_plan('GpuShuffledSymmetricHashJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', basic_nested_gens + [decimal_gen_128bit], ids=idfn)
@pytest.mark.parametrize('join_type', all_symmetric_join_types, ids=idfn)
@pytest.mark.parametrize('sub_part_enabled', ['false', 'true'], ids=['SubPartition_OFF', 'SubPartition_ON'])
@allow_non_gpu(*non_utc_allow)
def test_hash_join_ridealong_symmetric(data_gen, join_type, sub_part_enabled):
    hash_join_ridealong(data_gen, join_type, sub_part_enabled)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
# Not all join types can be translated to a broadcast join, but this tests them to be sure we
# can handle what spark is doing
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_right_table(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', basic_nested_gens + [decimal_gen_128bit], ids=idfn)
# Not all join types can be translated to a broadcast join, but this tests them to be sure we
# can handle what spark is doing
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_right_table_ridealong(data_gen, join_type):
    def do_join(spark):
        left, right = create_ridealong_df(spark, short_gen, data_gen, 500, 500)
        return left.join(broadcast(right), left.key == right.r_key, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
# Not all join types can be translated to a broadcast join, but this tests them to be sure we
# can handle what spark is doing
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_right_table_with_job_group(data_gen, join_type):
    with_cpu_session(lambda spark : spark.sparkContext.setJobGroup("testjob1", "test", False))
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen,batch_size', join_batch_size_test_params(
    (all_gen + basic_nested_gens, '1g'),
    (join_small_batch_gens + [basic_struct_gen, ArrayGen(string_gen)], '100')), ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_cartesian_join(data_gen, batch_size):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.crossJoin(right)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.xfail(condition=is_databricks_runtime(),
    reason='https://github.com/NVIDIA/spark-rapids/issues/334')
@pytest.mark.parametrize('batch_size', ['100', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
def test_cartesian_join_special_case_count(batch_size):
    def do_join(spark):
        left, right = create_df(spark, int_gen, 50, 25)
        return left.crossJoin(right).selectExpr('COUNT(*)')
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.xfail(condition=is_databricks_runtime(),
    reason='https://github.com/NVIDIA/spark-rapids/issues/334')
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
def test_cartesian_join_special_case_group_by_count(batch_size):
    def do_join(spark):
        left, right = create_df(spark, int_gen, 50, 25)
        return left.crossJoin(right).groupBy('a').count()
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen,batch_size', join_batch_size_test_params(
    (all_gen, '1g'),
    (join_small_batch_gens, '100')), ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_cartesian_join_with_condition(data_gen, batch_size):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        return left.join(right, left.b >= right.r_b, "cross")
    conf = copy_and_update(_sortmerge_join_conf, {'spark.rapids.sql.batchSizeBytes': batch_size})
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen,batch_size', join_batch_size_test_params(
    (all_gen + basic_nested_gens, '1g'),
    (join_small_batch_gens, '100')), ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_nested_loop_join(data_gen, batch_size):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.crossJoin(broadcast(right))
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('batch_size', ['100', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
def test_broadcast_nested_loop_join_special_case_count(batch_size):
    def do_join(spark):
        left, right = create_df(spark, int_gen, 50, 25)
        return left.crossJoin(broadcast(right)).selectExpr('COUNT(*)')
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.xfail(condition=is_databricks_runtime(),
    reason='https://github.com/NVIDIA/spark-rapids/issues/334')
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
def test_broadcast_nested_loop_join_special_case_group_by_count(batch_size):
    def do_join(spark):
        left, right = create_df(spark, int_gen, 50, 25)
        return left.crossJoin(broadcast(right)).groupBy('a').count()
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen,batch_size', join_batch_size_test_params(
    (join_ast_gen, '1g'),
    ([int_gen], 100)), ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Inner', 'LeftSemi', 'LeftAnti', 'Cross'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_right_broadcast_nested_loop_join_with_ast_condition(data_gen, join_type, batch_size):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        return left.join(broadcast(right), (left.b >= right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.rapids.sql.batchSizeBytes': batch_size})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', join_ast_gen, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_left_broadcast_nested_loop_join_with_ast_condition(data_gen):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        return broadcast(left).join(right, (left.b >= right.r_b), 'Right')
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [IntegerGen(), LongGen(), pytest.param(FloatGen(), marks=[incompat]), pytest.param(DoubleGen(), marks=[incompat])], ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Cross'], ids=idfn)
def test_broadcast_nested_loop_join_with_condition_post_filter(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        # AST does not support cast or logarithm yet, so this must be implemented as a post-filter
        return left.join(broadcast(right), left.a > f.log(right.r_a), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [IntegerGen(), LongGen(), pytest.param(FloatGen(), marks=[incompat]), pytest.param(DoubleGen(), marks=[incompat])], ids=idfn)
@pytest.mark.parametrize('join_type', ['Cross', 'Left', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_broadcast_nested_loop_join_with_condition(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # AST does not support cast or logarithm yet which is supposed to be extracted into child
        # nodes. And this test doesn't cover other join types due to:
        #    (1) build right are not supported for Right
        #    (2) FullOuter: currently is not supported
        # Those fallback reasons are not due to AST. Additionally, this test case changes test_broadcast_nested_loop_join_with_condition_fallback:
        #    (1) adapt double to integer since AST current doesn't support it.
        #    (2) switch to right side build to pass checks of 'Left', 'LeftSemi', 'LeftAnti' join types
        return left.join(broadcast(right), f.round(left.a).cast('integer') > f.round(f.log(right.r_a).cast('integer')), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={"spark.rapids.sql.castFloatToIntegralTypes.enabled": True})

@allow_non_gpu('BroadcastExchangeExec', 'BroadcastNestedLoopJoinExec', 'Cast', 'GreaterThan', 'Log')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [IntegerGen(), LongGen(), pytest.param(FloatGen(), marks=[incompat]), pytest.param(DoubleGen(), marks=[incompat])], ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_broadcast_nested_loop_join_with_condition_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # AST does not support double type which is not split-able into child nodes.
        return broadcast(left).join(right, left.a > f.log(right.r_a), join_type)
    assert_gpu_fallback_collect(do_join, 'BroadcastNestedLoopJoinExec')

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [byte_gen, short_gen, int_gen, long_gen,
                                      float_gen, double_gen,
                                      string_gen, boolean_gen, date_gen, timestamp_gen], ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_nested_loop_join_with_array_contains(data_gen, join_type):
    arr_gen = ArrayGen(data_gen)
    literal = with_cpu_session(lambda spark: gen_scalar(data_gen))
    def do_join(spark):
        left, right = create_df(spark, arr_gen, 50, 25)
        # Array_contains will be pushed down into project child nodes
        return broadcast(left).join(right, array_contains(left.a, literal.cast(data_gen.data_type)) < array_contains(right.r_a, literal.cast(data_gen.data_type)))
    assert_gpu_and_cpu_are_equal_collect(do_join)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_right_broadcast_nested_loop_join_condition_missing(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        # Compute the distinct of the join result to verify the join produces a proper dataframe
        # for downstream processing.
        return left.join(broadcast(right), how=join_type).distinct()
    assert_gpu_and_cpu_are_equal_collect(do_join)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Right'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_left_broadcast_nested_loop_join_condition_missing(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # This test is impacted by https://github.com/NVIDIA/spark-rapids/issues/294
        # if the sizes are large enough to have both 0.0 and -0.0 show up 500 and 250
        # but these take a long time to verify so we run with smaller numbers by default
        # that do not expose the error
        # Compute the distinct of the join result to verify the join produces a proper dataframe
        # for downstream processing.
        return broadcast(left).join(right, how=join_type).distinct()
    assert_gpu_and_cpu_are_equal_collect(do_join)

@pytest.mark.parametrize('data_gen', all_gen + single_level_array_gens + [binary_gen], ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_right_broadcast_nested_loop_join_condition_missing_count(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.join(broadcast(right), how=join_type).selectExpr('COUNT(*)')
    assert_gpu_and_cpu_are_equal_collect(do_join)

@pytest.mark.parametrize('data_gen', all_gen + single_level_array_gens + [binary_gen], ids=idfn)
@pytest.mark.parametrize('join_type', ['Right'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_left_broadcast_nested_loop_join_condition_missing_count(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return broadcast(left).join(right, how=join_type).selectExpr('COUNT(*)')
    assert_gpu_and_cpu_are_equal_collect(do_join)

@allow_non_gpu('BroadcastExchangeExec', 'BroadcastNestedLoopJoinExec', 'GreaterThanOrEqual', *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['LeftOuter', 'LeftSemi', 'LeftAnti', 'FullOuter'], ids=idfn)
def test_broadcast_nested_loop_join_with_conditionals_build_left_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return broadcast(left).join(right, (left.b >= right.r_b), join_type)
    assert_gpu_fallback_collect(do_join, 'BroadcastNestedLoopJoinExec')

@allow_non_gpu('BroadcastExchangeExec', 'BroadcastNestedLoopJoinExec', 'GreaterThanOrEqual', *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['RightOuter', 'FullOuter'], ids=idfn)
def test_broadcast_nested_loop_with_conditionals_build_right_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        return left.join(broadcast(right), (left.b >= right.r_b), join_type)
    assert_gpu_fallback_collect(do_join, 'BroadcastNestedLoopJoinExec')

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
# Not all join types can be translated to a broadcast join, but this tests them to be sure we
# can handle what spark is doing
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
# Specify 200 shuffle partitions to test cases where streaming side is empty
# as in https://github.com/NVIDIA/spark-rapids/issues/7516
@pytest.mark.parametrize('shuffle_conf', [{}, {'spark.sql.shuffle.partitions': 200}], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_left_table(data_gen, join_type, shuffle_conf):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 250, 500)
        return broadcast(left).join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=shuffle_conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', join_ast_gen, ids=idfn)
@pytest.mark.parametrize('join_type', all_join_types, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_with_conditionals(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right),
                   (left.a == right.r_a) & (left.b >= right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@allow_non_gpu('BroadcastExchangeExec', 'BroadcastHashJoinExec', 'Cast', 'GreaterThan', 'Log', 'SortMergeJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [long_gen], ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_broadcast_join_with_condition_ast_op_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # AST does not support cast or logarithm yet
        return left.join(broadcast(right),
                         (left.a == right.r_a) & (left.b > f.log(right.r_b)), join_type)
    exec = 'SortMergeJoinExec' if join_type in ['Right', 'FullOuter'] else 'BroadcastHashJoinExec'
    assert_gpu_fallback_collect(do_join, exec)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@allow_non_gpu('BroadcastExchangeExec', 'BroadcastHashJoinExec', 'Cast', 'GreaterThan', 'SortMergeJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', join_no_ast_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_broadcast_join_with_condition_ast_type_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 50, 25)
        # AST does not support cast or logarithm yet
        return left.join(broadcast(right),
                         (left.a == right.r_a) & (left.b > right.r_b), join_type)
    exec = 'SortMergeJoinExec' if join_type in ['Right', 'FullOuter'] else 'BroadcastHashJoinExec'
    assert_gpu_fallback_collect(do_join, exec)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', join_no_ast_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Cross'], ids=idfn)
def test_broadcast_join_with_condition_post_filter(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right),
                         (left.a == right.r_a) & (left.b > right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', join_ast_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_sortmerge_join_with_condition_ast(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(right, (left.a == right.r_a) & (left.b >= right.r_b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@allow_non_gpu('GreaterThan', 'Log', 'ShuffleExchangeExec', 'SortMergeJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [long_gen], ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_sortmerge_join_with_condition_ast_op_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        # AST does not support cast or logarithm yet
        return left.join(right, (left.a == right.r_a) & (left.b > f.log(right.r_b)), join_type)
    assert_gpu_fallback_collect(do_join, 'SortMergeJoinExec', conf=_sortmerge_join_conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@allow_non_gpu('GreaterThan', 'ShuffleExchangeExec', 'SortMergeJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', join_no_ast_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'FullOuter', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_sortmerge_join_with_condition_ast_type_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(right, (left.a == right.r_a) & (left.b > right.r_b), join_type)
    assert_gpu_fallback_collect(do_join, 'SortMergeJoinExec', conf=_sortmerge_join_conf)


_mixed_df1_with_nulls = [('a', RepeatSeqGen(LongGen(nullable=(True, 20.0)), length= 10)),
                         ('b', IntegerGen()), ('c', LongGen())]
_mixed_df2_with_nulls = [('a', RepeatSeqGen(LongGen(nullable=(True, 20.0)), length= 10)),
                         ('b', StringGen()), ('c', BooleanGen())]


@ignore_order
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti', 'FullOuter', 'Cross'], ids=idfn)
def test_broadcast_join_mixed(join_type):
    def do_join(spark):
        left = gen_df(spark, _mixed_df1_with_nulls, length=500)
        right = gen_df(spark, _mixed_df2_with_nulls, length=500).withColumnRenamed("a", "r_a")\
                .withColumnRenamed("b", "r_b").withColumnRenamed("c", "r_c")
        return left.join(broadcast(right), left.a.eqNullSafe(right.r_a), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

@ignore_order
@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
@pytest.mark.xfail(condition=is_emr_runtime(),
    reason='https://github.com/NVIDIA/spark-rapids/issues/821')
@pytest.mark.parametrize('repartition', ["true", "false"], ids=idfn)
def test_join_bucketed_table(repartition, spark_tmp_table_factory):
    def do_join(spark):
        table_name = spark_tmp_table_factory.get()
        data = [("http://fooblog.com/blog-entry-116.html", "https://fooblog.com/blog-entry-116.html"),
                ("http://fooblog.com/blog-entry-116.html", "http://fooblog.com/blog-entry-116.html")]
        resolved = spark.sparkContext.parallelize(data).toDF(['Url','ResolvedUrl'])
        feature_data = [("http://fooblog.com/blog-entry-116.html", "21")]
        feature = spark.sparkContext.parallelize(feature_data).toDF(['Url','Count'])
        feature.write.bucketBy(400, 'Url').sortBy('Url').format('parquet').mode('overwrite')\
                 .saveAsTable(table_name)
        testurls = spark.sql("SELECT Url, Count FROM {}".format(table_name))
        if (repartition == "true"):
                return testurls.repartition(20).join(resolved, "Url", "inner")
        else:
                return testurls.join(resolved, "Url", "inner")
    assert_gpu_and_cpu_are_equal_collect(do_join, conf={'spark.sql.autoBroadcastJoinThreshold': '-1'})

# Because we disable ShuffleExchangeExec in some cases we need to allow it to not be on the GPU
# and we do the result sorting in python to avoid that shuffle also being off the GPU
@allow_non_gpu('ShuffleExchangeExec', 'HashPartitioning')
@ignore_order(local=True)
@pytest.mark.parametrize('join_type', ['Left', 'Right', 'Inner', 'LeftSemi', 'LeftAnti'], ids=idfn)
@pytest.mark.parametrize('cache_side', ['cache_left', 'cache_right'], ids=idfn)
@pytest.mark.parametrize('cpu_side', ['cache', 'not_cache'], ids=idfn)
def test_half_cache_join(join_type, cache_side, cpu_side):
    left_gen = [('a', SetValuesGen(LongType(), range(500))), ('b', IntegerGen())]
    right_gen = [('r_a', SetValuesGen(LongType(), range(500))), ('c', LongGen())]
    def do_join(spark):
        # Try to force the shuffle to be split between CPU and GPU for the join
        # so don't let the shuffle be on the GPU/CPU depending on how the test is configured
        # when we repartition and cache the data
        spark.conf.set('spark.rapids.sql.exec.ShuffleExchangeExec', cpu_side != 'cache')
        left = gen_df(spark, left_gen, length=500)
        right = gen_df(spark, right_gen, length=500)

        if (cache_side == 'cache_left'):
            # Try to force the shuffle to be split between CPU and GPU for the join
            # by default if the operation after the shuffle is not on the GPU then
            # don't do a GPU shuffle, so do something simple after the repartition
            # to make sure that the GPU shuffle is used.
            left = left.repartition('a').selectExpr('b + 1 as b', 'a').cache()
            left.count() # populate the cache
        else:
            #cache_right
            # Try to force the shuffle to be split between CPU and GPU for the join
            # by default if the operation after the shuffle is not on the GPU then
            # don't do a GPU shuffle, so do something simple after the repartition
            # to make sure that the GPU shuffle is used.
            right = right.repartition('r_a').selectExpr('c + 1 as c', 'r_a').cache()
            right.count() # populate the cache
        # Now turn it back so the other half of the shuffle will be on the oposite side
        spark.conf.set('spark.rapids.sql.exec.ShuffleExchangeExec', cpu_side == 'cache')
        return left.join(right, left.a == right.r_a, join_type)

    # Even though Spark does not know the size of an RDD input so it will not do a broadcast join unless
    # we tell it to, this is just to be safe
    assert_gpu_and_cpu_are_equal_collect(do_join, {'spark.sql.autoBroadcastJoinThreshold': '1'})

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', struct_gens, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'Cross', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_sortmerge_join_struct_as_key(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', struct_gens, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'Cross', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_sortmerge_join_struct_mixed_key(data_gen, join_type):
    def do_join(spark):
        left = two_col_df(spark, data_gen, int_gen, length=500)
        right = two_col_df(spark, data_gen, int_gen, length=500)
        return left.join(right, (left.a == right.a) & (left.b == right.b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', struct_gens, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'Cross', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_sortmerge_join_struct_mixed_key_with_null_filter(data_gen, join_type):
    def do_join(spark):
        left = two_col_df(spark, data_gen, int_gen, length=500)
        right = two_col_df(spark, data_gen, int_gen, length=500)
        return left.join(right, (left.a == right.a) & (left.b == right.b), join_type)
    # Disable constraintPropagation to test null filter on built table with nullable structures.
    conf = {'spark.sql.constraintPropagation.enabled': 'false', **_sortmerge_join_conf}
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=conf)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', struct_gens, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'Cross', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_right_struct_as_key(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(broadcast(right), left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', struct_gens, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'Cross', 'LeftSemi', 'LeftAnti'], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_broadcast_join_right_struct_mixed_key(data_gen, join_type):
    def do_join(spark):
        left = two_col_df(spark, data_gen, int_gen, length=500)
        right = two_col_df(spark, data_gen, int_gen, length=250)
        return left.join(broadcast(right), (left.a == right.a) & (left.b == right.b), join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join)

# local sort because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2140')
@pytest.mark.parametrize('data_gen', [basic_struct_gen_with_floats], ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'Cross', 'LeftSemi', 'LeftAnti'], ids=idfn)
def test_sortmerge_join_struct_with_floats_key(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 250)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_sortmerge_join_conf)

@allow_non_gpu('SortMergeJoinExec', 'SortExec', 'NormalizeNaNAndZero', 'CreateNamedStruct',
        'GetStructField', 'Literal', 'If', 'IsNull', 'ShuffleExchangeExec', 'HashPartitioning',
        *non_utc_allow)
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', struct_gens, ids=idfn)
@pytest.mark.parametrize('join_type', ['FullOuter'], ids=idfn)
def test_sortmerge_join_struct_as_key_fallback(data_gen, join_type):
    def do_join(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.join(right, left.a == right.r_a, join_type)
    assert_gpu_fallback_collect(do_join, 'SortMergeJoinExec', conf=_sortmerge_join_conf)

# Regression test for https://github.com/NVIDIA/spark-rapids/issues/3775
@ignore_order(local=True)
def test_struct_self_join(spark_tmp_table_factory):
    def do_join(spark):
        data = [
            (("Adam ", "", "Green"), "1", "M", 1000),
            (("Bob ", "Middle", "Green"), "2", "M", 2000),
            (("Cathy ", "", "Green"), "3", "F", 3000)
        ]
        schema = (StructType()
                  .add("name", StructType()
                       .add("firstname", StringType())
                       .add("middlename", StringType())
                       .add("lastname", StringType()))
                  .add("id", StringType())
                  .add("gender", StringType())
                  .add("salary", IntegerType()))
        df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        df_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(df_name)
        resultdf = spark.sql(
            "select struct(name, struct(name.firstname, name.lastname) as newname)" +
            " as col,name from " + df_name + " union" +
            " select struct(name, struct(name.firstname, name.lastname) as newname) as col,name" +
            " from " + df_name)
        resultdf_name = spark_tmp_table_factory.get()
        resultdf.createOrReplaceTempView(resultdf_name)
        return spark.sql("select a.* from {} a, {} b where a.name=b.name".format(
            resultdf_name, resultdf_name))
    assert_gpu_and_cpu_are_equal_collect(do_join)

# ExistenceJoin occurs in the context of existential subqueries (which is rewritten to SemiJoin) if
# there is an additional condition that may qualify left records even though they don't have
# join partner records from the right.
#
# Thus a query is rewritten roughly as a LeftOuter with an additional Boolean column "exists" added.
# which feeds into a filter "exists OR someOtherPredicate"
# If the condition is something like an AND, it makes the result a subset of a SemiJoin, and
# the optimizer won't use ExistenceJoin.
@ignore_order(local=True)
@pytest.mark.parametrize('numComplementsToExists', [0, 1, 2], ids=(lambda val: f"complements:{val}"))
@pytest.mark.parametrize('aqeEnabled', [
    pytest.param(False, id='aqe:off'),
    # workaround: somehow AQE retains RDDScanExec preventing parent ShuffleExchangeExec
    # from being executed on GPU
    # pytest.param(True, marks=pytest.mark.allow_non_gpu('ShuffleExchangeExec'), id='aqe:on')
])
@pytest.mark.parametrize('conditionalJoin', [False, True], ids=['ast:off', 'ast:on'])
@pytest.mark.parametrize('forceBroadcastHashJoin', [False, True], ids=['broadcastHJ:off', 'broadcastHJ:on'])
def test_existence_join(numComplementsToExists, aqeEnabled, conditionalJoin, forceBroadcastHashJoin, spark_tmp_table_factory):
    leftTable = spark_tmp_table_factory.get()
    rightTable = spark_tmp_table_factory.get()
    def do_join(spark):
        # create non-overlapping ranges to have a mix of exists=true and exists=false

        # left-hand side rows
        lhs_upper_bound = 10
        lhs_data = list((f"left_{v}", v * 10, v * 100) for v in range(2, lhs_upper_bound))
        # duplicate without a match
        lhs_data.append(('left_1', 10, 100))
        # duplicate with a match
        lhs_data.append(('left_2', 20, 200))
        lhs_data.append(('left_null', None, None))
        df_left = spark.createDataFrame(lhs_data)
        df_left.createOrReplaceTempView(leftTable)

        rhs_data = list((f"right_{v}", v * 10, v * 100) for v in range(0, 8))
        rhs_data.append(('right_null', None, None))
        # duplicate every row in the rhs to verify it does not affect
        # the number of output rows, which should be equal to the left table row count
        rhs_data_with_dupes=[]
        for dupe in rhs_data:
            rhs_data_with_dupes.extend([dupe, dupe])

        df_right = spark.createDataFrame(rhs_data_with_dupes)
        df_right.createOrReplaceTempView(rightTable)
        cond = "<=" if conditionalJoin else "="
        res = spark.sql((
            "select * "
            "from {} as l "
            f"where l._2 >= {10 * (lhs_upper_bound - numComplementsToExists)}"
            "   or exists (select * from {} as r where r._2 = l._2 and r._3 {} l._3)"
        ).format(leftTable, rightTable, cond))
        return res
    existenceJoinRegex = r"ExistenceJoin\(exists#[0-9]+\),"
    if conditionalJoin:
        existenceJoinRegex = existenceJoinRegex + r" \(.+ <= .+\)"

    if forceBroadcastHashJoin:
        # hints don't work with ExistenceJoin
        # forcing by upping the size to the estimated right output
        bhjThreshold = "9223372036854775807b"
        existenceJoinRegex = r'BroadcastHashJoin .* ' + existenceJoinRegex
    else:
        bhjThreshold = "-1b"

    assert_cpu_and_gpu_are_equal_collect_with_capture(do_join, existenceJoinRegex,
        conf={
            "spark.sql.adaptive.enabled": aqeEnabled,
            "spark.sql.autoBroadcastJoinThreshold": bhjThreshold
        })

@ignore_order
@pytest.mark.parametrize('aqeEnabled', [True, False], ids=['aqe:on', 'aqe:off'])
def test_existence_join_in_broadcast_nested_loop_join(spark_tmp_table_factory, aqeEnabled):
    left_table_name = spark_tmp_table_factory.get()
    right_table_name = spark_tmp_table_factory.get()

    def do_join(spark):
        gen = LongGen(min_val=0, max_val=5)

        left_df = binary_op_df(spark, gen)
        left_df.createOrReplaceTempView(left_table_name)
        right_df = binary_op_df(spark, gen)
        right_df.createOrReplaceTempView(right_table_name)

        return spark.sql(("select * "
                          "from {} as l "
                          "where l.a >= 3 "
                          "   or exists (select * from {} as r where l.b < r.b)"
                          ).format(left_table_name, right_table_name))

    capture_regexp = r"GpuBroadcastNestedLoopJoin ExistenceJoin\(exists#[0-9]+\),"
    assert_cpu_and_gpu_are_equal_collect_with_capture(do_join, capture_regexp,
                                                      conf={"spark.sql.adaptive.enabled": aqeEnabled})

@ignore_order
@pytest.mark.parametrize('aqeEnabled', [True, False], ids=['aqe:on', 'aqe:off'])
def test_degenerate_broadcast_nested_loop_existence_join(spark_tmp_table_factory, aqeEnabled):
    left_table_name = spark_tmp_table_factory.get()
    right_table_name = spark_tmp_table_factory.get()

    def do_join(spark):
        gen = LongGen(min_val=0, max_val=5)

        left_df = binary_op_df(spark, gen)
        left_df.createOrReplaceTempView(left_table_name)
        right_df = binary_op_df(spark, gen)
        right_df.createOrReplaceTempView(right_table_name)

        return spark.sql(("select * "
                          "from {} as l "
                          "where l.a >= 3 "
                          "   or exists (select * from {} as r where l.b < l.a)"
                          ).format(left_table_name, right_table_name))

    capture_regexp = r"GpuBroadcastNestedLoopJoin ExistenceJoin\(exists#[0-9]+\),"
    assert_cpu_and_gpu_are_equal_collect_with_capture(do_join, capture_regexp,
                                                      conf={"spark.sql.adaptive.enabled": aqeEnabled})

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [StringGen(), IntegerGen()], ids=idfn)
@pytest.mark.parametrize("aqe_enabled", [True, False], ids=idfn)
@pytest.mark.parametrize("join_reorder_enabled", [True, False], ids=idfn)
def test_multi_table_hash_join(data_gen, aqe_enabled, join_reorder_enabled):
    def do_join(spark):
        t1 = binary_op_df(spark, data_gen, length=1000)
        t2 = binary_op_df(spark, data_gen, length=800)
        t3 = binary_op_df(spark, data_gen, length=300)
        t4 = binary_op_df(spark, data_gen, length=50)
        return t1.join(t2, t1.a == t2.a, 'Inner') \
                 .join(t3, t2.a == t3.a, 'Inner') \
                 .join(t4, t3.a == t4.a, 'Inner')
    conf = copy_and_update(_hash_join_conf, {
        'spark.sql.adaptive.enabled': aqe_enabled,
        'spark.rapids.sql.optimizer.joinReorder.enabled': join_reorder_enabled
    })
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=conf)


limited_integral_gens = [byte_gen, ShortGen(max_val=BYTE_MAX), IntegerGen(max_val=BYTE_MAX), LongGen(max_val=BYTE_MAX)]

def hash_join_different_key_integral_types(left_gen, right_gen, join_type):
    def do_join(spark):
        left = unary_op_df(spark, left_gen, length=50)
        right = unary_op_df(spark, right_gen, length=500)
        return left.join(right, left.a == right.a, join_type)
    _all_conf = copy_and_update(_hash_join_conf, {
        "spark.rapids.sql.test.subPartitioning.enabled": True
    })
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_all_conf)

@validate_execs_in_gpu_plan('GpuShuffledHashJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('left_gen', limited_integral_gens, ids=idfn)
@pytest.mark.parametrize('right_gen', limited_integral_gens, ids=idfn)
@pytest.mark.parametrize('join_type', all_non_symmetric_join_types, ids=idfn)
def test_hash_join_different_key_integral_types_non_symmetric(left_gen, right_gen, join_type):
    hash_join_different_key_integral_types(left_gen, right_gen, join_type)

@validate_execs_in_gpu_plan('GpuShuffledSymmetricHashJoinExec')
@ignore_order(local=True)
@pytest.mark.parametrize('left_gen', limited_integral_gens, ids=idfn)
@pytest.mark.parametrize('right_gen', limited_integral_gens, ids=idfn)
@pytest.mark.parametrize('join_type', all_symmetric_join_types, ids=idfn)
def test_hash_join_different_key_integral_types_symmetric(left_gen, right_gen, join_type):
    hash_join_different_key_integral_types(left_gen, right_gen, join_type)


bloom_filter_confs = {
    "spark.sql.autoBroadcastJoinThreshold": "1",
    "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold": 1,
    "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold": "100GB",
    "spark.sql.optimizer.runtime.bloomFilter.enabled": "true"
}

def check_bloom_filter_join(confs, expected_classes, is_multi_column):
    def do_join(spark):
        if is_multi_column:
            left = spark.range(100000).withColumn("second_id", col("id") % 5)
            right = spark.range(10).withColumn("id2", col("id").cast("string")).withColumn("second_id", col("id") % 5)
            return right.filter("cast(id2 as bigint) % 3 = 0").join(left, (left.id == right.id) & (left.second_id == right.second_id), "inner")
        else:
            left = spark.range(100000)
            right = spark.range(10).withColumn("id2", col("id").cast("string"))
            return right.filter("cast(id2 as bigint) % 3 = 0").join(left, left.id == right.id, "inner")
    all_confs = copy_and_update(bloom_filter_confs, confs)
    assert_cpu_and_gpu_are_equal_collect_with_capture(do_join, expected_classes, conf=all_confs)

@ignore_order(local=True)
@pytest.mark.parametrize("batch_size", ['1g', '1000'], ids=idfn)
@pytest.mark.parametrize("is_multi_column", [False, True], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
@pytest.mark.skipif(is_before_spark_330(), reason="Bloom filter joins added in Spark 3.3.0")
def test_bloom_filter_join(batch_size, is_multi_column):
    conf = {"spark.rapids.sql.batchSizeBytes": batch_size}
    check_bloom_filter_join(confs=conf,
                            expected_classes="GpuBloomFilterMightContain,GpuBloomFilterAggregate",
                            is_multi_column=is_multi_column)

@allow_non_gpu("FilterExec", "ShuffleExchangeExec")
@ignore_order(local=True)
@pytest.mark.parametrize("is_multi_column", [False, True], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
@pytest.mark.skipif(is_before_spark_330(), reason="Bloom filter joins added in Spark 3.3.0")
def test_bloom_filter_join_cpu_probe(is_multi_column):
    conf = {"spark.rapids.sql.expression.BloomFilterMightContain": "false"}
    check_bloom_filter_join(confs=conf,
                            expected_classes="BloomFilterMightContain,GpuBloomFilterAggregate",
                            is_multi_column=is_multi_column)

@allow_non_gpu("ObjectHashAggregateExec", "ShuffleExchangeExec")
@ignore_order(local=True)
@pytest.mark.parametrize("is_multi_column", [False, True], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
@pytest.mark.skipif(is_before_spark_330(), reason="Bloom filter joins added in Spark 3.3.0")
def test_bloom_filter_join_cpu_build(is_multi_column):
    conf = {"spark.rapids.sql.expression.BloomFilterAggregate": "false"}
    check_bloom_filter_join(confs=conf,
                            expected_classes="GpuBloomFilterMightContain,BloomFilterAggregate",
                            is_multi_column=is_multi_column)

@allow_non_gpu("ObjectHashAggregateExec", "ProjectExec", "ShuffleExchangeExec")
@ignore_order(local=True)
@pytest.mark.parametrize("agg_replace_mode", ["partial", "final"])
@pytest.mark.parametrize("is_multi_column", [False, True], ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
@pytest.mark.skipif(is_before_spark_330(), reason="Bloom filter joins added in Spark 3.3.0")
def test_bloom_filter_join_split_cpu_build(agg_replace_mode, is_multi_column):
    conf = {"spark.rapids.sql.hashAgg.replaceMode": agg_replace_mode}
    check_bloom_filter_join(confs=conf,
                            expected_classes="GpuBloomFilterMightContain,BloomFilterAggregate,GpuBloomFilterAggregate",
                            is_multi_column=is_multi_column)

@ignore_order(local=True)
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
@pytest.mark.skipif(is_before_spark_330(), reason="Bloom filter joins added in Spark 3.3.0")
def test_bloom_filter_join_with_merge_some_null_filters(spark_tmp_path):
    data_path1 = spark_tmp_path + "/BLOOM_JOIN_DATA1"
    data_path2 = spark_tmp_path + "/BLOOM_JOIN_DATA2"
    with_cpu_session(lambda spark: spark.range(100000).coalesce(1).write.parquet(data_path1))
    with_cpu_session(lambda spark: spark.range(100000).withColumn("id2", col("id").cast("string"))\
                     .coalesce(1).write.parquet(data_path2))
    confs = copy_and_update(bloom_filter_confs,
                            {"spark.sql.files.maxPartitionBytes": "1000"})
    def do_join(spark):
        left = spark.read.parquet(data_path1)
        right = spark.read.parquet(data_path2)
        return right.filter("cast(id2 as bigint) % 3 = 0").join(left, left.id == right.id, "inner")
    assert_gpu_and_cpu_are_equal_collect(do_join, confs)

@ignore_order(local=True)
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
@pytest.mark.skipif(is_before_spark_330(), reason="Bloom filter joins added in Spark 3.3.0")
def test_bloom_filter_join_with_merge_all_null_filters(spark_tmp_path):
    data_path1 = spark_tmp_path + "/BLOOM_JOIN_DATA1"
    data_path2 = spark_tmp_path + "/BLOOM_JOIN_DATA2"
    with_cpu_session(lambda spark: spark.range(100000).write.parquet(data_path1))
    with_cpu_session(lambda spark: spark.range(100000).withColumn("id2", col("id").cast("string")) \
                     .write.parquet(data_path2))
    def do_join(spark):
        left = spark.read.parquet(data_path1)
        right = spark.read.parquet(data_path2)
        return right.filter("cast(id2 as bigint) % 3 = 4").join(left, left.id == right.id, "inner")
    assert_gpu_and_cpu_are_equal_collect(do_join, bloom_filter_confs)


@ignore_order(local=True)
@allow_non_gpu("ProjectExec", "FilterExec", "BroadcastHashJoinExec", "ColumnarToRowExec", "BroadcastExchangeExec")
@pytest.mark.parametrize("disable_build", [True, False])
def test_broadcast_hash_join_fix_fallback_by_inputfile(spark_tmp_path, disable_build):
    data_path_parquet = spark_tmp_path + "/parquet"
    data_path_orc = spark_tmp_path + "/orc"
    # The smaller one (orc) will be the build side (a broadcast)
    with_cpu_session(lambda spark: spark.range(100).write.orc(data_path_orc))
    with_cpu_session(lambda spark: spark.range(10000).withColumn("id2", col("id") + 10)
                     .write.parquet(data_path_parquet))
    def do_join(spark):
        left = spark.read.parquet(data_path_parquet)
        right = spark.read.orc(data_path_orc)
        return left.join(broadcast(right), "id", "inner")\
            .selectExpr("*", "input_file_block_length()")

    if disable_build:
        # To reproduce the error
        # '''
        # java.lang.IllegalStateException: the broadcast must be on the GPU too
        #  	 at com.nvidia.spark.rapids.shims.GpuBroadcastJoinMeta.verifyBuildSideWasReplaced...
        # '''
        scan_name = 'OrcScan'
    else:
        # An additional case that the exec contains the input file expression is not disabled
        # by InputFileBlockRule mistakenly. When the stream side scan runs on CPU, but the
        # build side scan runs on GPU, the InputFileBlockRule will not put the exec on
        # CPU, leading to wrong output.
        scan_name = 'ParquetScan'
    assert_gpu_and_cpu_are_equal_collect(
        do_join,
        conf={"spark.sql.autoBroadcastJoinThreshold": "10M",
              "spark.sql.sources.useV1SourceList": "",
              "spark.rapids.sql.input." + scan_name: False})


@ignore_order(local=True)
@allow_non_gpu("ProjectExec", "BroadcastNestedLoopJoinExec", "ColumnarToRowExec", "BroadcastExchangeExec")
@pytest.mark.parametrize("disable_build", [True, False])
def test_broadcast_nested_join_fix_fallback_by_inputfile(spark_tmp_path, disable_build):
    data_path_parquet = spark_tmp_path + "/parquet"
    data_path_orc = spark_tmp_path + "/orc"
    # The smaller one (orc) will be the build side (a broadcast)
    with_cpu_session(lambda spark: spark.range(50).write.orc(data_path_orc))
    with_cpu_session(lambda spark: spark.range(500).withColumn("id2", col("id") + 10)
                     .write.parquet(data_path_parquet))
    def do_join(spark):
        left = spark.read.parquet(data_path_parquet)
        right = spark.read.orc(data_path_orc)
        return left.crossJoin(broadcast(right)).selectExpr("*", "input_file_block_length()")

    if disable_build:
        # To reproduce the error
        # '''
        # java.lang.IllegalStateException: the broadcast must be on the GPU too
        #  	 at com.nvidia.spark.rapids.shims.GpuBroadcastJoinMeta.verifyBuildSideWasReplaced...
        # '''
        scan_name = 'OrcScan'
    else:
        # An additional case that the exec contains the input file expression is not disabled
        # by InputFileBlockRule mistakenly. When the stream side scan runs on CPU, but the
        # build side scan runs on GPU, the InputFileBlockRule will not put the exec on
        # CPU, leading to wrong output.
        scan_name = 'ParquetScan'
    assert_gpu_and_cpu_are_equal_collect(
        do_join,
        conf={"spark.sql.autoBroadcastJoinThreshold": "-1",
              "spark.sql.sources.useV1SourceList": "",
              "spark.rapids.sql.input." + scan_name: False})

@ignore_order(local=True)
@pytest.mark.parametrize("join_type", ["Inner", "LeftOuter"], ids=idfn)
@pytest.mark.parametrize("batch_size", ["500", "1g"], ids=idfn)
def test_distinct_join(join_type, batch_size):
    join_conf = {
        "spark.rapids.sql.batchSizeBytes": batch_size
    }
    def do_join(spark):
        left_df = spark.range(1024).withColumn("x", f.col("id") + 1)
        right_df = spark.range(768).withColumn("x", f.col("id") * 2)
        return left_df.join(right_df, ["x"], join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=join_conf)

@ignore_order(local=True)
@pytest.mark.parametrize("join_type", ["Inner", "FullOuter"], ids=idfn)
@pytest.mark.parametrize("is_left_host_shuffle", [False, True], ids=idfn)
@pytest.mark.parametrize("is_right_host_shuffle", [False, True], ids=idfn)
@pytest.mark.parametrize("is_left_smaller", [False, True], ids=idfn)
@pytest.mark.parametrize("batch_size", ["1024", "1g"], ids=idfn)
def test_new_symmetric_join(join_type, is_left_host_shuffle, is_right_host_shuffle,
                            is_left_smaller, batch_size):
    join_conf = {
        "spark.rapids.sql.join.useShuffledSymmetricHashJoin": "true",
        "spark.sql.autoBroadcastJoinThreshold": "1",
        "spark.rapids.sql.batchSizeBytes": batch_size
    }
    left_size, right_size = (2048, 1024) if is_left_smaller else (1024, 2048)
    def do_join(spark):
        left_df = gen_df(spark, [
            ("key1", RepeatSeqGen([1, 2, 3, 4, None], data_type=IntegerType())),
            ("ints", int_gen),
            ("key2", RepeatSeqGen([5, 6, 7, None], data_type=LongType())),
            ("floats", float_gen)], left_size)
        right_df = gen_df(spark, [
            ("doubles", double_gen),
            ("key2", RepeatSeqGen([5, 7, None, 8], data_type=LongType())),
            ("shorts", short_gen),
            ("key1", RepeatSeqGen([1, 2, 3, 5, 7, None], data_type=IntegerType()))], right_size)
        # The symmetric join code handles inputs differently based on whether they are coming from
        # host memory or GPU memory. Simple joins produce inputs directly from a shuffle which
        # covers the host memory case. For GPU memory cases, we insert an aggregation to force the
        # respective join input to be from a prior GPU operation in the same stage.
        if not is_left_host_shuffle:
            left_df = left_df.groupBy("key1", "key2").max("ints", "floats")
        if not is_right_host_shuffle:
            right_df = right_df.groupBy("key1", "key2").max("doubles", "shorts")
        return left_df.join(right_df, ["key1", "key2"], join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=join_conf)

@ignore_order(local=True)
@pytest.mark.parametrize("join_type", ["Inner", "FullOuter"], ids=idfn)
@pytest.mark.parametrize("is_left_smaller", [False, True], ids=idfn)
@pytest.mark.parametrize("is_ast_supported", [False, True], ids=idfn)
@pytest.mark.parametrize("batch_size", ["1024", "1g"], ids=idfn)
def test_new_symmetric_join_conditional(join_type, is_ast_supported, is_left_smaller, batch_size):
    if join_type == "FullOuter" and not is_ast_supported:
        pytest.skip("Full outer joins do not support a non-AST condition")
    join_conf = {
        "spark.rapids.sql.join.useShuffledSymmetricHashJoin": "true",
        "spark.sql.autoBroadcastJoinThreshold": "1",
        "spark.rapids.sql.batchSizeBytes": batch_size
    }
    left_size, right_size = (2048, 1024) if is_left_smaller else (1024, 2048)
    def do_join(spark):
        left_df = gen_df(spark, [
            ("key1", RepeatSeqGen([1, 2, 3, 4, None], data_type=IntegerType())),
            ("ints", RepeatSeqGen(IntegerGen(), length = 5)),
            ("key2", RepeatSeqGen([5, 6, 7, None], data_type=LongType())),
            ("floats", float_gen)], left_size)
        right_df = gen_df(spark, [
            ("key2", RepeatSeqGen([5, 7, None, 8], data_type=LongType())),
            ("ints", RepeatSeqGen(IntegerGen(), length = 3)),
            ("key1", RepeatSeqGen([1, 2, 3, 5, 7, None], data_type=IntegerType()))], right_size)
        cond = [left_df.key1 == right_df.key1, left_df.key2 == right_df.key2]
        if is_ast_supported:
            cond.append(left_df.ints >= right_df.ints)
        else:
            # AST does not support logarithm yet
            cond.append(left_df.ints >= f.log(right_df.ints))
        return left_df.join(right_df, cond, join_type)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=join_conf)
