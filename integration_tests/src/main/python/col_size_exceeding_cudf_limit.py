# Copyright (c) 2023, NVIDIA CORPORATION.
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
from spark_session import with_cpu_session
from marks import inject_oom, nightly_gpu_mem_consuming_case

# Define test cases
gen_list_dict = {
    'single_partition_single_value': [
        ('v0', LongGen()),
        ('k0', StringGen(pattern='a{100}', nullable=False))
    ],
    'single_partition_multiple_value': [
        ('v0', LongGen()),
        ('k0', RepeatSeqGen(StringGen(pattern='a{80,100}', nullable=False), length=2))
    ],
    'multiple_partition_single_value': [
        ('v0', LongGen()),
        ('k0', StringGen(pattern='a{100}', nullable=False)),
        ('k1', StringGen(pattern='b{100}', nullable=False))
    ],
    'multiple_partition_multiple_value_wider_first_col': [
        ('v0', LongGen()),
        ('k0', RepeatSeqGen(StringGen(pattern='a{80,100}', nullable=False), length=2)),
        ('k1', RepeatSeqGen(StringGen(pattern='b{30,50}', nullable=False), length=2))
    ],
    'multiple_partition_multiple_value_narrow_first_col': [
        ('v0', LongGen()),
        ('k0', RepeatSeqGen(StringGen(pattern='a{80,100}', nullable=False), length=2)),
        ('k1', RepeatSeqGen(StringGen(pattern='b{60,80}', nullable=False), length=2)),
    ]
}


def extract_partition_cols(gen_list):
    partition_cols = [item[0] for item in gen_list if item[0].startswith('k')]
    return partition_cols


@inject_oom
@pytest.mark.parametrize("key", gen_list_dict.keys())
def test_col_size_exceeding_cudf_limit(spark_tmp_path, key):
    conf = {'spark.rapids.cudfColumnSizeLimit': 1000}
    gen_list = gen_list_dict[key]
    partition_cols = extract_partition_cols(gen_list)
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PARQUET_DATA/' + key
    with_cpu_session(
        lambda spark: gen_df(spark, gen, length=2000).write.partitionBy(partition_cols).format('parquet').save(
            data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format('parquet').load(data_path).selectExpr('*'),
        conf)


@inject_oom
@nightly_gpu_mem_consuming_case
@pytest.mark.parametrize("key", gen_list_dict.keys())
def test_col_size_exceeding_cudf_limit_nightly(spark_tmp_path, key):
    gen_list = gen_list_dict[key]
    partition_cols = extract_partition_cols(gen_list)
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PARQUET_DATA/' + key
    with_cpu_session(
        lambda spark: gen_df(spark, gen, length=100000000).write.partitionBy(partition_cols).format('parquet').save(
            data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format('parquet').load(data_path).selectExpr('*'))
