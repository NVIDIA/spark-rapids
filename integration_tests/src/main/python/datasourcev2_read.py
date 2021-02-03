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
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session

# This test requires a datasource v2 jar containing the class
# org.apache.spark.sql.connector.InMemoryTableCatalog
# which returns ArrowColumnVectors be specified in order for it to run.
# If that class is not present it skips the tests.

catalogName = "columnar"
columnarClass = 'org.apache.spark.sql.connector.InMemoryTableCatalog'

def createPeopleCSVDf(spark, peopleCSVLocation):
    return spark.read.format("csv")\
        .option("header", "false")\
        .option("inferSchema", "true")\
        .load(peopleCSVLocation)\
        .withColumnRenamed("_c0", "name")\
        .withColumnRenamed("_c1", "age")\
        .withColumnRenamed("_c2", "job")

def setupInMemoryTableWithPartitioning(spark, csv, tname, column_and_table):
    peopleCSVDf = createPeopleCSVDf(spark, csv)
    peopleCSVDf.createOrReplaceTempView(tname)
    spark.table(tname).write.partitionBy("job").saveAsTable(column_and_table)

def setupInMemoryTableNoPartitioning(spark, csv, tname, column_and_table):
    peopleCSVDf = createPeopleCSVDf(spark, csv)
    peopleCSVDf.createOrReplaceTempView(tname)
    spark.table(tname).write.saveAsTable(column_and_table)

def readTable(csvPath, tableToRead):
    return lambda spark: spark.table(tableToRead)\
        .orderBy("name", "age")

def createDatabase(spark):
    try:
        spark.sql("create database IF NOT EXISTS " + catalogName)
        spark.sql("use " + catalogName)
    except Exception:
        pytest.skip("Failed to load catalog for datasource v2 {}, jar is probably missing".format(columnarClass))

def cleanupDatabase(spark):
    spark.sql("drop database IF EXISTS " + catalogName)

@pytest.fixture(autouse=True)
def setupAndCleanUp():
    with_cpu_session(lambda spark : createDatabase(spark),
            conf={'spark.sql.catalog.columnar': columnarClass})
    yield
    with_cpu_session(lambda spark : cleanupDatabase(spark),
            conf={'spark.sql.catalog.columnar': columnarClass})

@allow_non_gpu('ShowTablesExec', 'DropTableExec')
@pytest.mark.parametrize('csv', ['people.csv'])
def test_read_round_trip_partitioned(std_input_path, csv, spark_tmp_table_factory):
    csvPath = std_input_path + "/" + csv
    tableName = spark_tmp_table_factory.get()
    columnarTableName = catalogName + "." + tableName
    with_cpu_session(lambda spark : setupInMemoryTableWithPartitioning(spark, csvPath, tableName, columnarTableName),
            conf={'spark.sql.catalog.columnar': columnarClass})
    assert_gpu_and_cpu_are_equal_collect(readTable(csvPath, columnarTableName),
            conf={'spark.sql.catalog.columnar': columnarClass})

@allow_non_gpu('ShowTablesExec', 'DropTableExec')
@pytest.mark.parametrize('csv', ['people.csv'])
def test_read_round_trip_no_partitioned(std_input_path, csv, spark_tmp_table_factory):
    csvPath = std_input_path + "/" + csv
    tableNameNoPart = spark_tmp_table_factory.get()
    columnarTableNameNoPart = catalogName + "." + tableNameNoPart
    with_cpu_session(lambda spark : setupInMemoryTableNoPartitioning(spark, csvPath, tableNameNoPart, columnarTableNameNoPart),
            conf={'spark.sql.catalog.columnar': columnarClass})
    assert_gpu_and_cpu_are_equal_collect(readTable(csvPath, columnarTableNameNoPart),
            conf={'spark.sql.catalog.columnar': columnarClass})

@allow_non_gpu('ShowTablesExec', 'DropTableExec')
@pytest.mark.parametrize('csv', ['people.csv'])
def test_read_round_trip_no_partitioned_arrow_off(std_input_path, csv, spark_tmp_table_factory):
    csvPath = std_input_path + "/" + csv
    tableNameNoPart = spark_tmp_table_factory.get()
    columnarTableNameNoPart = catalogName + "." + tableNameNoPart
    with_cpu_session(lambda spark : setupInMemoryTableNoPartitioning(spark, csvPath, tableNameNoPart, columnarTableNameNoPart),
            conf={'spark.sql.catalog.columnar': columnarClass,
                  'spark.rapids.arrowCopyOptimizationEnabled': 'false'})
    assert_gpu_and_cpu_are_equal_collect(readTable(csvPath, columnarTableNameNoPart),
            conf={'spark.sql.catalog.columnar': columnarClass,
                  'spark.rapids.arrowCopyOptimizationEnabled': 'false'})
