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

import json
import os.path
import pyspark.sql.functions as f
import pytest
import re
import sys

from asserts import *
from data_gen import *
from conftest import is_databricks_runtime
from marks import *
from parquet_write_test import parquet_part_write_gens, parquet_write_gens_list, writer_confs
from pyspark.sql.types import *
from spark_session import is_before_spark_320, is_before_spark_330, with_cpu_session

delta_meta_allow = [
    "DeserializeToObjectExec",
    "ShuffleExchangeExec",
    "FileSourceScanExec",
    "FilterExec",
    "MapPartitionsExec",
    "MapElementsExec",
    "ObjectHashAggregateExec",
    "ProjectExec",
    "SerializeFromObjectExec",
    "SortExec"
]

delta_writes_enabled_conf = {"spark.rapids.sql.format.delta.write.enabled": "true"}

def fixup_path(d):
    """Modify the 'path' value to remove random IDs in the pathname"""
    parts = d["path"].split("-")
    d["path"] = "-".join(parts[0:1]) + ".".join(parts[-1].split(".")[-2:])

def del_keys(key_list, c_val, g_val):
    for key in key_list:
        c_val.pop(key, None)
        g_val.pop(key, None)

def fixup_operation_metrics(opm):
    """Update the specified operationMetrics node to facilitate log comparisons"""
    for k in "executionTimeMs", "numOutputBytes", "rewriteTimeMs", "scanTimeMs":
        opm.pop(k, None)

TMP_TABLE_PATTERN=re.compile("tmp_table_\w+")

def fixup_operation_parameters(opp):
    """Update the specified operationParameters node to facilitate log comparisons"""
    for key in ("predicate", "matchedPredicates", "notMatchedPredicates"):
        pred = opp.get(key)
        if pred:
            opp[key] = TMP_TABLE_PATTERN.sub("tmp_table", pred)

def assert_delta_log_json_equivalent(filename, c_json, g_json):
    assert c_json.keys() == g_json.keys(), "Delta log {} has mismatched keys:\nCPU: {}\nGPU: {}".format(filename, c_json, g_json)
    for key, c_val in c_json.items():
        g_val = g_json[key]
        # Strip out the values that are expected to be different
        c_tags = c_val.get("tags", {})
        g_tags = g_val.get("tags", {})
        del_keys(["INSERTION_TIME", "MAX_INSERTION_TIME", "MIN_INSERTION_TIME"], c_tags, g_tags)
        if key == "metaData":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'metaData' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("createdTime", "id"), c_val, g_val)
        elif key == "add":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'add' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("modificationTime", "size"), c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        elif key == "cdc":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'cdc' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("size",), c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        elif key == "commitInfo":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'commitInfo' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("timestamp", "txnId"), c_val, g_val)
            for v in c_val, g_val:
                fixup_operation_metrics(v.get("operationMetrics", {}))
                fixup_operation_parameters(v.get("operationParameters", {}))
        elif key == "remove":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'remove' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("deletionTimestamp", "size"), c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        assert c_val == g_val, "Delta log {} is different at key '{}':\nCPU: {}\nGPU: {}".format(filename, key, c_val, g_val)

def decode_jsons(json_data):
    """Decode the JSON records in a string"""
    jsons = []
    idx = 0
    decoder = json.JSONDecoder()
    while idx < len(json_data):
        js, idx = decoder.raw_decode(json_data, idx=idx)
        jsons.append(js)
        # Skip whitespace between records
        while idx < len(json_data) and json_data[idx].isspace():
            idx += 1
    # reorder to produce a consistent output for comparison
    def json_to_sort_key(j):
        keys = sorted(j.keys())
        paths = sorted([ v.get("path", "") for v in j.values() ])
        return ','.join(keys + paths)
    jsons.sort(key=json_to_sort_key)
    return jsons

def assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path):
    cpu_log_data = spark.sparkContext.wholeTextFiles(data_path + "/CPU/_delta_log/*").collect()
    gpu_log_data = spark.sparkContext.wholeTextFiles(data_path + "/GPU/_delta_log/*").collect()
    assert len(cpu_log_data) == len(gpu_log_data), "Different number of Delta log files:\nCPU: {}\nGPU: {}".format(cpu_log_data, gpu_log_data)
    cpu_logs_data = [ (os.path.basename(x), y) for x, y in cpu_log_data if x.endswith(".json") ]
    gpu_logs_dict = dict([ (os.path.basename(x), y) for x, y in gpu_log_data if x.endswith(".json") ])
    for file, cpu_json_data in cpu_logs_data:
        gpu_json_data = gpu_logs_dict.get(file)
        assert gpu_json_data, "CPU Delta log file {} is missing from GPU Delta logs".format(file)
        cpu_jsons = decode_jsons(cpu_json_data)
        gpu_jsons = decode_jsons(gpu_json_data)
        assert len(cpu_jsons) == len(gpu_jsons), "Different line counts in {}:\nCPU: {}\nGPU: {}".format(file, cpu_json_data, gpu_json_data)
        for cpu_json, gpu_json in zip(cpu_jsons, gpu_jsons):
            assert_delta_log_json_equivalent(file, cpu_json, gpu_json)

@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf",
                         [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                          {"spark.rapids.sql.format.parquet.enabled": "false"},
                          {"spark.rapids.sql.format.parquet.write.enabled": "false"}], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_disabled_fallback(spark_tmp_path, disable_conf):
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=copy_and_update(writer_confs, disable_conf))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("gens", parquet_write_gens_list, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_round_trip_unmanaged(spark_tmp_path, gens):
    gen_list = [("c" + str(i), gen) for i, gen in enumerate(gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("gens", parquet_part_write_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_part_write_round_trip_unmanaged(spark_tmp_path, gens):
    gen_list = [("a", RepeatSeqGen(gens, 10)), ("b", gens)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .partitionBy("a")
            .save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("gens", parquet_part_write_gens, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_multi_part_write_round_trip_unmanaged(spark_tmp_path, gens):
    gen_list = [("a", RepeatSeqGen(gens, 10)), ("b", gens), ("c", SetValuesGen(StringType(), ["x", "y", "z"]))]
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
        .partitionBy("a", "c")
        .save(path),
        lambda spark, path: spark.read.format("delta").load(path).filter("c='x'"),
        data_path,
        conf=copy_and_update(writer_confs, delta_writes_enabled_conf))
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

def do_update_round_trip_managed(spark_tmp_path, mode):
    gen_list = [("x", int_gen), ("y", binary_gen), ("z", string_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.mode(mode).format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    # Verify time travel still works
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("delta").option("versionAsOf", "0").load(data_path + "/GPU"),
        conf=confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_overwrite_round_trip_unmanaged(spark_tmp_path):
    do_update_round_trip_managed(spark_tmp_path, "overwrite")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_append_round_trip_unmanaged(spark_tmp_path):
    do_update_round_trip_managed(spark_tmp_path, "append")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(is_databricks_runtime() and is_before_spark_330(),
                    reason="Databricks 10.4 does not properly handle options passed during DataFrame API write")
def test_delta_write_round_trip_cdf_write_opt(spark_tmp_path):
    gen_list = [("ints", int_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    # drop the _commit_timestamp column when comparing since it will always be different
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .mode("overwrite")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_round_trip_cdf_table_prop(spark_tmp_path):
    gen_list = [("ints", int_gen)]
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(writer_confs, delta_writes_enabled_conf)
    def setup_tables(spark):
        for name in ["CPU", "GPU"]:
            spark.sql("CREATE TABLE delta.`{}/{}` (ints INT) ".format(data_path, name) +
                      "USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    with_cpu_session(setup_tables)
    # drop the _commit_timestamp column when comparing since it will always be different
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .mode("append")
            .option("delta.enableChangeDataFeed", "true")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("delta")
            .mode("overwrite")
            .save(path),
        lambda spark, path: spark.read.format("delta")
            .option("readChangeDataFeed", "true")
            .option("startingVersion", 0)
            .load(path)
            .drop("_commit_timestamp"),
        data_path,
        conf=confs)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("ts_write", ["INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_legacy_timestamp_fallback(spark_tmp_path, ts_write):
    gen = TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))
    data_path = spark_tmp_path + "/DELTA_DATA"
    all_confs = copy_and_update(delta_writes_enabled_conf, {
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",
        "spark.sql.legacy.parquet.outputTimestampType": ts_write
    })
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=all_confs)

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_encryption_option_fallback(spark_tmp_path, write_options):
    def write_func(spark, path):
        writer = unary_op_df(spark, int_gen).coalesce(1).write.format("delta")
        for key, value in write_options.items():
            writer.option(key , value)
        writer.save(path)
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        write_func,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=delta_writes_enabled_conf)

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_encryption_runtimeconfig_fallback(spark_tmp_path, write_options):
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=copy_and_update(write_options, delta_writes_enabled_conf))

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("write_options", [{"parquet.encryption.footer.key": "k1"},
                                           {"parquet.encryption.column.keys": "k2:a"},
                                           {"parquet.encryption.footer.key": "k1", "parquet.encryption.column.keys": "k2:a"}])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_encryption_hadoopconfig_fallback(spark_tmp_path, write_options):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_hadoop_confs(spark):
        for k, v in write_options.items():
            spark.sparkContext._jsc.hadoopConfiguration().set(k, v)
    def reset_hadoop_confs(spark):
        for k in write_options.keys():
            spark.sparkContext._jsc.hadoopConfiguration().unset(k)
    try:
        with_cpu_session(setup_hadoop_confs)
        assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
            lambda spark, path: spark.read.format("delta").load(path),
            data_path,
            "ExecutedCommandExec",
            conf=delta_writes_enabled_conf)
    finally:
        with_cpu_session(reset_hadoop_confs)

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize('codec', ['gzip'])
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_compression_fallback(spark_tmp_path, codec):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {"spark.sql.parquet.compression.codec": codec})
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=confs)

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_legacy_format_fallback(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {"spark.sql.parquet.writeLegacyFormat": "true"})
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_append_only(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    gen = int_gen
    # setup initial table
    with_gpu_session(lambda spark: unary_op_df(spark, gen).coalesce(1).write.format("delta")
                     .option("delta.appendOnly", "true")
                     .save(data_path),
                     conf=delta_writes_enabled_conf)
    # verify overwrite fails
    assert_py4j_exception(
        lambda: with_gpu_session(
            lambda spark: unary_op_df(spark, gen).write.format("delta").mode("overwrite").save(data_path),
            conf=delta_writes_enabled_conf),
        "This table is configured to only allow appends")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_constraint_not_null(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    not_null_gen = StringGen(nullable=False)
    null_gen = SetValuesGen(StringType(), [None])

    # create table with not null constraint
    def setup_table(spark):
        spark.sql("CREATE TABLE delta.`{}` (a string NOT NULL) USING DELTA".format(data_path))

    with_cpu_session(setup_table)

    # verify write of non-null values does not throw
    with_gpu_session(lambda spark: unary_op_df(spark, not_null_gen).write.format("delta").mode("append").save(data_path),
                     conf=delta_writes_enabled_conf)

    # verify write of null value throws
    assert_py4j_exception(
        lambda: with_gpu_session(
            lambda spark: unary_op_df(spark, null_gen).write.format("delta").mode("append").save(data_path),
            conf=delta_writes_enabled_conf),
        "NOT NULL constraint violated for column: a")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_constraint_check(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"

    # create table with check constraint
    def setup_table(spark):
        spark.sql("CREATE TABLE delta.`{}` (id long, x long) USING DELTA".format(data_path))
        spark.sql("ALTER TABLE delta.`{}` ADD CONSTRAINT customcheck CHECK (id < x)".format(data_path))

    with_cpu_session(setup_table)

    # verify write of dataframe that passes constraint check does not fail
    def gen_good_data(spark):
        return spark.range(1024).withColumn("x", f.col("id") + 1)

    with_gpu_session(lambda spark: gen_good_data(spark).write.format("delta").mode("append").save(data_path),
                     conf=delta_writes_enabled_conf)

    # verify write of values that violate the constraint throws
    def gen_bad_data(spark):
        return gen_good_data(spark).union(spark.range(1).withColumn("x", f.col("id")))

    assert_py4j_exception(
        lambda: with_gpu_session(
            lambda spark: gen_bad_data(spark).write.format("delta").mode("append").save(data_path),
            conf=delta_writes_enabled_conf),
        "CHECK constraint customcheck (id < x) violated")

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_constraint_check_fallback(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    # create table with check constraint
    def setup_table(spark):
        spark.sql("CREATE TABLE delta.`{}` (id long, x long) USING DELTA".format(data_path))
        spark.sql("ALTER TABLE delta.`{}` ADD CONSTRAINT mycheck CHECK (id + x < 1000)".format(data_path))
    with_cpu_session(setup_table)
    # create a conf that will force constraint check to fallback to CPU
    add_disable_conf = copy_and_update(delta_writes_enabled_conf, {"spark.rapids.sql.expression.Add": "false"})
    # verify write of dataframe that passes constraint check does not fail
    def gen_good_data(spark):
        return spark.range(100).withColumn("x", f.col("id") + 1)
    # TODO: Find a way to capture plan with DeltaInvariantCheckerExec
    with_gpu_session(lambda spark: gen_good_data(spark).write.format("delta").mode("append").save(data_path),
                     conf=add_disable_conf)
    # verify write of values that violate the constraint throws
    def gen_bad_data(spark):
        return spark.range(1000).withColumn("x", f.col("id") + 1)
    assert_py4j_exception(
        lambda: with_gpu_session(
            lambda spark: gen_bad_data(spark).write.format("delta").mode("append").save(data_path),
            conf=add_disable_conf),
        "CHECK constraint mycheck ((id + x) < 1000) violated",)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("num_cols", [-1, 0, 1, 2, 3 ], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_stat_column_limits(num_cols, spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs = copy_and_update(delta_writes_enabled_conf, {"spark.databricks.io.skipping.stringPrefixLength": 8})
    strgen = StringGen() \
        .with_special_case((chr(sys.maxunicode) * 7) + "abc") \
        .with_special_case((chr(sys.maxunicode) * 8) + "abc") \
        .with_special_case((chr(sys.maxunicode) * 16) + "abc") \
        .with_special_case(('\U0000FFFD' * 7) + "abc") \
        .with_special_case(('\U0000FFFD' * 8) + "abc") \
        .with_special_case(('\U0000FFFD' * 16) + "abc")
    gens = [("a", StructGen([("x", strgen), ("y", StructGen([("z", strgen)]))])),
            ("b", binary_gen),
            ("c", strgen)]
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: gen_df(spark, gens).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_generated_columns(spark_tmp_table_factory, spark_tmp_path):
    from delta.tables import DeltaTable
    def write_data(spark, path):
        DeltaTable.create(spark) \
            .tableName(spark_tmp_table_factory.get()) \
            .location(path) \
            .addColumn("id", "LONG", comment="IDs") \
            .addColumn("x", "LONG", comment="some other column") \
            .addColumn("z", "STRING", comment="a generated column",
                       generatedAlwaysAs="CONCAT('sum(id,x)=', CAST((id + x) AS STRING))") \
            .execute()
        df = spark.range(2048).withColumn("x", f.col("id") * 2)
        df.write.format("delta").mode("append").save(path)

    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_and_cpu_writes_are_equal_collect(
        write_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320() or not is_databricks_runtime(),
                    reason="Delta Lake identity columns are currently only supported on Databricks")
def test_delta_write_identity_columns(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def create_data(spark, path):
        spark.sql("CREATE TABLE delta.`{}` (x BIGINT, id BIGINT GENERATED ALWAYS AS IDENTITY) USING DELTA".format(path))
        spark.range(2048).selectExpr("id * id AS x").write.format("delta").mode("append").save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        create_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    def append_data(spark, path):
        spark.range(2048).selectExpr("id + 10 as x").write.format("delta").mode("append").save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        append_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))


@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320() or not is_databricks_runtime(),
                    reason="Delta Lake identity columns are currently only supported on Databricks")
def test_delta_write_multiple_identity_columns(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def create_data(spark, path):
        spark.sql("CREATE TABLE delta.`{}` (".format(path) +
                  "id1 BIGINT GENERATED ALWAYS AS IDENTITY, "
                  "x BIGINT, "
                  "id2 BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 100 ), "
                  "id3 BIGINT GENERATED ALWAYS AS IDENTITY ( INCREMENT BY 11 ), "
                  "id4 BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH -200 INCREMENT BY 3 ), "
                  "id5 BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 12 INCREMENT BY -3 )"
                  ") USING DELTA")
        spark.range(2048).selectExpr("id * id AS x").write.format("delta").mode("append").save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        create_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    def append_data(spark, path):
        spark.range(2048).selectExpr("id + 10 as x").write.format("delta").mode("append").save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        append_data,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=delta_writes_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("confkey", ["optimizeWrite"], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(is_databricks_runtime() and is_before_spark_330(),
                    reason="Databricks 10.4 does not properly handle options passed during DataFrame API write")
def test_delta_write_auto_optimize_write_opts_fallback(confkey, spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").option(confkey, "true").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=delta_writes_enabled_conf)

@allow_non_gpu(*delta_meta_allow, "CreateTableExec", "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("confkey", [
    "delta.autoOptimize",
    "delta.autoOptimize.optimizeWrite",
    "delta.autoOptimize.autoCompact" ], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.skipif(not is_databricks_runtime(), reason="Auto optimize only supported on Databricks")
def test_delta_write_auto_optimize_table_props_fallback(confkey, spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        spark.sql("CREATE TABLE delta.`{}/CPU` (a INT) USING DELTA TBLPROPERTIES ({} = true)".format(data_path, confkey))
        spark.sql("CREATE TABLE delta.`{}/GPU` (a INT) USING DELTA TBLPROPERTIES ({} = true)".format(data_path, confkey))
    with_cpu_session(setup_tables)
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").mode("append").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=delta_writes_enabled_conf)

@allow_non_gpu(*delta_meta_allow, "ExecutedCommandExec")
@delta_lake
@ignore_order
@pytest.mark.parametrize("confkey", [
    "spark.databricks.delta.optimizeWrite.enabled",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" ], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_auto_optimize_sql_conf_fallback(confkey, spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {confkey: "true"})
    assert_gpu_fallback_write(
        lambda spark, path: unary_op_df(spark, int_gen).coalesce(1).write.format("delta").save(path),
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        "ExecutedCommandExec",
        conf=confs)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_write_aqe_join(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    confs=copy_and_update(delta_writes_enabled_conf, {"spark.sql.adaptive.enabled": "true"})
    def do_join(spark, path):
        df = unary_op_df(spark, int_gen)
        df.join(df, ["a"], "inner").write.format("delta").save(path)
    assert_gpu_and_cpu_writes_are_equal_collect(
        do_join,
        lambda spark, path: spark.read.format("delta").load(path),
        data_path,
        conf=confs)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
