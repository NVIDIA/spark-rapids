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
from marks import allow_non_gpu, delta_lake
from spark_session import with_cpu_session, is_databricks91_or_later

_conf = {'spark.rapids.sql.explain': 'ALL'}

@delta_lake
@allow_non_gpu('FileSourceScanExec')
# @pytest.mark.skipif(not is_databricks91_or_later(), reason="Delta Lake is already configured on Databricks so we just run these tests there for now")
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
@allow_non_gpu('FileSourceScanExec')
# @pytest.mark.skipif(not is_databricks91_or_later(), reason="Delta Lake is already configured on Databricks so we just run these tests there for now")
def test_delta_merge_fallback(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_delta_table(spark):
        df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ["id", "data"])
        df.write.format("delta").save("/tmp/delta-table/{}".format(table))
    with_cpu_session(setup_delta_table)

import org.apache.spark.sql._
import spark.implicits._

spark.conf.set("spark.rapids.sql.debug.logTransformations", "true")

val df = Seq(("a", 1), ("b", 2)).toDF("c0", "c1")
val df2 = Seq(("b", 8), ("c", 3)).toDF("c0", "c1")

df.write.mode(SaveMode.Overwrite).format("delta").save("/tmp/t1")
df2.write.mode(SaveMode.Overwrite).format("delta").save("/tmp/t2")

spark.read.format("delta").load("/tmp/t1").createTempView("t1")
spark.read.format("delta").load("/tmp/t2").createTempView("t2")

spark.sql("""MERGE INTO t1
USING t2
ON t1.c0 = t2.c0
WHEN MATCHED THEN
  UPDATE SET
    c1 = t2.c1
WHEN NOT MATCHED
  THEN INSERT (
    c0,
    c1
  )
  VALUES (
    t2.c0,
    t2.c1
  )""")
