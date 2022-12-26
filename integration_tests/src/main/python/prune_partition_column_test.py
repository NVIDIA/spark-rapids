# Copyright (c) 2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from spark_session import with_cpu_session


@pytest.mark.parametrize('prune_part_enabled', [False, True])
def test_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled):
    data_path = spark_tmp_path + '/PARQUET_DATA/'
    for part_path in ['b=0/c=s1', 'b=0/c=s2', 'b=1/c=s1', 'b=1/c=s2']:
        with_cpu_session(
            lambda spark: unary_op_df(spark, int_gen).write.parquet(data_path + part_path))

    all_confs = {
        'spark.sql.sources.useV1SourceList': "parquet",
        'spark.rapids.sql.fileScanPrunePartition.enabled': prune_part_enabled}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).select('a', 'c'),
        conf=all_confs)


@pytest.mark.parametrize('prune_part_enabled', [False, True])
def test_prune_partition_column_when_project_filter(spark_tmp_path, prune_part_enabled):
    data_path = spark_tmp_path + '/PARQUET_DATA/'
    for part_path in ['b=0/c=s1', 'b=0/c=s2', 'b=1/c=s1', 'b=1/c=s2']:
        with_cpu_session(
            lambda spark: unary_op_df(spark, int_gen).write.parquet(data_path + part_path))

    all_confs = {
        'spark.sql.sources.useV1SourceList': "parquet",
        'spark.rapids.sql.fileScanPrunePartition.enabled': prune_part_enabled}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path) \
            .filter('a > 0') \
            .select('a', 'c'),
        conf=all_confs)
