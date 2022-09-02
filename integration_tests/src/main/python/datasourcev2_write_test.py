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

from asserts import assert_gpu_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import is_hive_available, is_spark_330_or_later, with_cpu_session

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.skipif(not (is_hive_available() and is_spark_330_or_later()), reason="Must have Hive on Spark 3.3+")
@pytest.mark.parametrize('fileFormat', ['parquet', 'orc'])
def test_write_hive_bucketed_table_fallback(spark_tmp_table_factory, fileFormat):
    """
    fallback because GPU does not support Hive hash partition
    """
    table = spark_tmp_table_factory.get()

    def create_hive_table(spark):
        spark.sql("""create table {0} (a bigint, b bigint, c bigint)
                  stored as {1}
                  clustered by (b) into 3 buckets""".format(table, fileFormat))
        return None

    conf = {"hive.enforce.bucketing": "true",
            "hive.exec.dynamic.partition": "true",
            "hive.exec.dynamic.partition.mode": "nonstrict"}
    with_cpu_session(create_hive_table, conf = conf)

    assert_gpu_fallback_collect(
        lambda spark: spark.sql("insert into {} values (1, 2, 3)".format(table)),
        'DataWritingCommandExec',
        conf = conf)
