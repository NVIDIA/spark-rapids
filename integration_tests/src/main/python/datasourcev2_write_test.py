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

from asserts import assert_gpu_fallback_write
from data_gen import *
from marks import *
from pyspark.sql.types import *

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.parametrize('fileFormat', ['parquet', 'orc'])
def test_write_hive_bucketed_table_fallback(spark_tmp_path, spark_tmp_table_factory, fileFormat):
    """
    fallback because GPU does not support Hive hash partition
    """
    src = spark_tmp_table_factory.get()
    table = spark_tmp_table_factory.get()

    def write_hive_table(spark):
        
        data = map(lambda i: (i % 13, str(i), i % 5), range(50))
        df = spark.createDataFrame(data, ["i", "j", "k"])
        df.write.mode("overwrite").partitionBy("k").bucketBy(8, "i", "j").format(fileFormat).saveAsTable(src)

        spark.sql("""
            create table if not exists {0} 
            using hive options(fileFormat \"{1}\")
            as select * from {2} 
            """.format(table, fileFormat, src))

    data_path = spark_tmp_path + '/HIVE_DATA'

    assert_gpu_fallback_write(
        lambda spark, _: write_hive_table(spark),
        lambda spark, _: spark.sql("SELECT * FROM {}".format(table)),
        data_path,
        'DataWritingCommandExec',
        conf = {"hive.exec.dynamic.partition": "true",
                "hive.exec.dynamic.partition.mode": "nonstrict"})