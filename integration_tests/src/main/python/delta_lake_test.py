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
from pyspark.sql import Row
from asserts import assert_gpu_fallback_collect
from marks import allow_non_gpu, delta_lake
from spark_session import with_cpu_session, is_databricks91_or_later, spark_version
from spark_session import with_cpu_session, with_gpu_session, is_databricks91_or_later, is_databricks104_or_later, \
    spark_version

_conf = {'spark.rapids.sql.explain': 'ALL'}

@delta_lake
@allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(not (is_databricks91_or_later() or spark_version().startswith("3.2.")), \
    reason="Delta Lake is already configured on Databricks and CI supports Delta Lake OSS with Spark 3.2.x so far")
def test_delta_metadata_query_fallback(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_delta_table(spark):
        df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ["id", "data"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table))
    with_cpu_session(setup_delta_table)
    # note that this is just testing that any reads against a delta log json file fall back to CPU and does
    # not test the actual metadata queries that the delta lake plugin generates so does not fully test the
    # plugin code
    assert_gpu_fallback_collect(
        lambda spark : spark.read.json("/tmp/delta-table/{}/_delta_log/00000000000000000000.json".format(table)),
        "FileSourceScanExec", conf = _conf)

@delta_lake
@pytest.mark.skipif(not is_databricks104_or_later(), \
    reason="This test is specific to Databricks because we only fall back to CPU for merges on Databricks")
@allow_non_gpu(any = True)
def test_delta_merge_query(spark_tmp_table_factory):
    table_name_1 = spark_tmp_table_factory.get()
    table_name_2 = spark_tmp_table_factory.get()
    def setup_delta_table1(spark):
        df = spark.createDataFrame([('a', 10), ('b', 20)], ["c0", "c1"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table_name_1))
    def setup_delta_table2(spark):
        df = spark.createDataFrame([('a', 30), ('c', 30)], ["c0", "c1"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table_name_2))
    with_cpu_session(setup_delta_table1)
    with_cpu_session(setup_delta_table2)
    def merge(spark):
        spark.read.format("delta").load("/tmp/delta-table/{}".format(table_name_1)).createOrReplaceTempView("t1")
        spark.read.format("delta").load("/tmp/delta-table/{}".format(table_name_2)).createOrReplaceTempView("t2")
        return spark.sql("MERGE INTO t1 USING t2 ON t1.c0 = t2.c0 \
            WHEN MATCHED THEN UPDATE SET c1 = t1.c1 + t2.c1 \
            WHEN NOT MATCHED THEN INSERT (c0, c1) VALUES (t2.c0, t2.c1)").collect()
    # run the MERGE on GPU
    with_gpu_session(lambda spark : merge(spark), conf = _conf)
    # check the results on CPU
    result = with_cpu_session(lambda spark: spark.sql("SELECT * FROM t1 ORDER BY c0").collect(), conf=_conf)
    assert [Row(c0='a', c1=40), Row(c0='b', c1=20), Row(c0='c', c1=30)] == result