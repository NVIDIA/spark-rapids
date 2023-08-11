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

import os
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from spark_session import with_cpu_session

# Several values to avoid generating too many folders for partitions.
part1_gen = SetValuesGen(IntegerType(), [-10, -1, 0, 1, 10])
part2_gen = SetValuesGen(LongType(), [-100, 0, 100])

file_formats = ['parquet', 'orc', 'csv',
    pytest.param('json', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/7446'))]
if os.environ.get('INCLUDE_SPARK_AVRO_JAR', 'false') == 'true':
    file_formats = file_formats + ['avro']

_enable_read_confs = {
    'spark.rapids.sql.format.avro.enabled': 'true',
    'spark.rapids.sql.format.avro.read.enabled': 'true',
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true',
}


def do_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format,
                                           gpu_project_enabled=True):
    data_path = spark_tmp_path + '/PARTED_DATA/'
    with_cpu_session(
        lambda spark: three_col_df(spark, int_gen, part1_gen, part2_gen).write \
            .partitionBy('b', 'c').format(file_format).save(data_path))

    all_confs = copy_and_update(_enable_read_confs, {
        'spark.rapids.sql.exec.ProjectExec': gpu_project_enabled,
        'spark.sql.sources.useV1SourceList': file_format,
        'spark.rapids.sql.fileScanPrunePartition.enabled': prune_part_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format(file_format).schema('a int, b int, c long') \
            .load(data_path).select('a', 'c'),
        conf=all_confs)


@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
def test_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format):
    do_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format)


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
def test_prune_partition_column_when_fallback_project(spark_tmp_path, prune_part_enabled,
                                                      file_format):
    do_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format,
                                           gpu_project_enabled=False)


def do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_project_enabled=True,
                                                  gpu_filter_enabled=True):
    data_path = spark_tmp_path + '/PARTED_DATA/'
    with_cpu_session(
        lambda spark: three_col_df(spark, int_gen, part1_gen, part2_gen).write \
            .partitionBy('b', 'c').format(file_format).save(data_path))

    all_confs = copy_and_update(_enable_read_confs, {
        'spark.rapids.sql.exec.ProjectExec': gpu_project_enabled,
        'spark.rapids.sql.exec.FilterExec': gpu_filter_enabled,
        'spark.sql.sources.useV1SourceList': file_format,
        'spark.rapids.sql.fileScanPrunePartition.enabled': prune_part_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format(file_format).schema('a int, b int, c long').load(data_path) \
            .filter('{} > 0'.format(filter_col)) \
            .select('a', 'c'),
        conf=all_confs)


@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, filter_col,
                                                    file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col)


@allow_non_gpu('ProjectExec', 'FilterExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_fallback_filter_and_project(spark_tmp_path, prune_part_enabled,
                                                                 filter_col, file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_project_enabled=False,
                                                  gpu_filter_enabled=False)


@allow_non_gpu('FilterExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_fallback_filter_project(spark_tmp_path, prune_part_enabled,
                                                             filter_col, file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_filter_enabled=False)


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_filter_fallback_project(spark_tmp_path, prune_part_enabled,
                                                             filter_col, file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_project_enabled=False)
