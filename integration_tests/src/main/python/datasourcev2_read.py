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
tableNameNoPart = "peoplenopart"
columnarTableName = catalogName + "." + tableName
columnarTableNameNoPart = catalogName + "." + tableNameNoPart
columnarClass = 'org.apache.spark.sql.connector.InMemoryTableCatalog'

def setupInMemoryTableWithPartitioning(spark, csv):
    peopleCSVDf = createPeopleCSVDf(spark, csv)
    peopleCSVDf.createOrReplaceTempView("people_csv")
    spark.table("people_csv").write.partitionBy("job").saveAsTable(columnarTableName)

def setupInMemoryTableNoPartitioning(spark, csv):
    peopleCSVDf = createPeopleCSVDf(spark, csv)
    peopleCSVDf.createOrReplaceTempView("people_csv")
    spark.table("people_csv").write.saveAsTable(columnarTableNameNoPart)

def readTable(csvPath, tableToRead):
    return lambda spark: spark.table(tableToRead)\
        .orderBy("name", "age")

def createDatabase(spark):
    spark.sql("create database IF NOT EXISTS " + catalogName)
    spark.sql("use " + catalogName)

def cleanupDatabase(spark):
    spark.sql("drop table IF EXISTS " + tableName)
    spark.sql("drop table IF EXISTS " + tableNameNoPart)
    spark.sql("drop database IF EXISTS " + catalogName)

@pytest.fixture(autouse=True)
def setupAndCleanUp():
    with_cpu_session(lambda spark : createDatabase(spark),
            conf={'spark.sql.catalog.columnar': columnarClass})
    yield
    with_cpu_session(lambda spark : cleanupDatabase(spark),
            conf={'spark.sql.catalog.columnar': columnarClass})

@pytest.mark.parametrize('csv', ['people.csv'])
def test_read_round_trip_partitioned(std_input_path, csv):
    csvPath = std_input_path + "/" + csv
    with_cpu_session(lambda spark : setupInMemoryTableWithPartitioning(spark, csvPath),
            conf={'spark.sql.catalog.columnar': columnarClass})
    assert_gpu_and_cpu_are_equal_collect(readTable(csvPath, columnarTableName),
            conf={'spark.sql.catalog.columnar': columnarClass})

@pytest.mark.parametrize('csv', ['people.csv'])
def test_read_round_trip_no_partitioned(std_input_path, csv):
    csvPath = std_input_path + "/" + csv
    with_cpu_session(lambda spark : setupInMemoryTableNoPartitioning(spark, csvPath),
            conf={'spark.sql.catalog.columnar': columnarClass})
    assert_gpu_and_cpu_are_equal_collect(readTable(csvPath, columnarTableNameNoPart),
            conf={'spark.sql.catalog.columnar': columnarClass})
