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

from spark_session import with_cpu_session
from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from pyspark.sql.types import *


@pytest.mark.parametrize('col_name', ['v0'], ids=idfn)
def test_col_size_exceeding_cudf_limit(spark_tmp_path, col_name):
    conf = {'spark.rapids.cudfColumnSizeLimit': 20000}

    gen_list = [
        ('v0', LongGen()),
        ('v1', LongGen()),
        ('k0', RepeatSeqGen(StringGen(pattern='[0-9]{0,50}', nullable=False), length=2)),
        ('k1', RepeatSeqGen(StringGen(pattern='[0-9]{0,100}', nullable=False), length=2)),
        ('k2', RepeatSeqGen(StringGen(pattern='[0-9]{0,70}', nullable=False), length=2)),
    ]

    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen, length=1000).write.partitionBy('k0', 'k1', 'k2').format('parquet').save(
            data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format('parquet').load(data_path).selectExpr(col_name),
        conf)
