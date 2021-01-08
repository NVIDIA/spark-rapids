# Copyright (c) 2021, NVIDIA CORPORATION.
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
from pyspark.sql.types import *
from spark_session import with_cpu_session

def read_parquet_df(data_path):
    return lambda spark : spark.read.parquet(data_path)

def read_parquet_sql(data_path):
    return lambda spark : spark.sql('select * from parquet.`{}`'.format(data_path))

def createPeopleCSVDf(spark, peopleCSVLocation):
    return spark.read.format("csv")\
        .option("header", "false")\
        .option("inferSchema", "true")\
        .load(peopleCSVLocation)\
        .withColumnRenamed("_c0", "name")\
        .withColumnRenamed("_c1", "age")\
        .withColumnRenamed("_c2", "job")


catalogName = "columnar"
tableName = "people"
columnarTableName = catalogName + "." + tableName

def setupInMemoryTableWithPartitioning(spark, csv):
    spark.sql("create database IF NOT EXISTS " + catalogName)
    peopleCSVDf = createPeopleCSVDf(spark, csv)
    peopleCSVDf.createOrReplaceTempView("people_csv")
    spark.table("people_csv").write.partitionBy("job").saveAsTable(columnarTableName)

def setupInMemoryTableNoPartitioning(spark, csv):
    spark.sql("create database IF NOT EXISTS " + catalogName)
    peopleCSVDf = createPeopleCSVDf(spark, csv)
    peopleCSVDf.createOrReplaceTempView("people_csv")
    spark.table("people_csv").write.saveAsTable(columnarTableName)

def readTable(spark):
    spark.table(columnarTableName)\
        .orderBy("name", "age")

@pytest.mark.parametrize('csv', ['people.csv'])
def test_read_round_trip_partitioned(std_input_path, csv):
    with_cpu_session(lambda spark : setupInMemoryTableWithPartitioning(spark, std_input_path + csv))
    assert_gpu_and_cpu_are_equal_collect(readTable)

