# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
import pytest
import re

from spark_session import is_databricks122_or_later, supports_delta_lake_deletion_vectors, is_databricks143_or_later, \
    with_cpu_session
from asserts import assert_equal

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

# Disable Deletion Vectors except for Databricks 14.3
def deletion_vector_values_with_350DB143_xfail_reasons(enabled_xfail_reason=None, disabled_xfail_reason=None):
    # We will always set the deletion vectors to False
    # in case of DB 14.3, if there is no reason provided it's False otherwise False with xfail reason
    if not is_databricks143_or_later() or disabled_xfail_reason is None:
        enable_deletion_vector = [False]
    elif disabled_xfail_reason is not None: 
        enable_deletion_vector = [pytest.param(False, marks=pytest.mark.xfail(reason=disabled_xfail_reason))]

    # We only set the deletion vectors to true for DB 14.3
    # If there is an xfail reason provided then that is included as part of the parameter.
    if is_databricks143_or_later():
        if enabled_xfail_reason is None:
            enable_deletion_vector.append(True)
        else:
            enable_deletion_vector.append(pytest.param(True, marks=pytest.mark.xfail(reason=enabled_xfail_reason)))

    return enable_deletion_vector

deletion_vector_values = deletion_vector_values_with_350DB143_xfail_reasons()

delta_writes_enabled_conf = {"spark.rapids.sql.format.delta.write.enabled": "true"}

delta_write_fallback_allow = "ExecutedCommandExec,DataWritingCommandExec,WriteFilesExec,DeltaInvariantCheckerExec" if is_databricks122_or_later() else "ExecutedCommandExec"
delta_write_fallback_check = "DataWritingCommandExec" if is_databricks122_or_later() else "ExecutedCommandExec"

delta_optimized_write_fallback_allow = "ExecutedCommandExec,DataWritingCommandExec,DeltaOptimizedWriterExec,WriteFilesExec" if is_databricks122_or_later() else "ExecutedCommandExec"

def _fixup_operation_metrics(opm):
    """Update the specified operationMetrics node to facilitate log comparisons"""
    # note that we remove many byte metrics because number of bytes can vary
    # between CPU and GPU.
    metrics_to_remove = ["executionTimeMs", "numOutputBytes", "rewriteTimeMs", "scanTimeMs",
                         "numRemovedBytes", "numAddedBytes", "numTargetBytesAdded", "numTargetBytesInserted",
                         "numTargetBytesUpdated", "numTargetBytesRemoved", "materializeSourceTimeMs"]
    for k in metrics_to_remove:
        opm.pop(k, None)

TMP_TABLE_PATTERN=re.compile(r"tmp_table_\w+")
TMP_TABLE_PATH_PATTERN=re.compile(r"delta.`[^`]*`")
REF_ID_PATTERN=re.compile(r"#[0-9]+")

def _fixup_operation_parameters(opp):
    """Update the specified operationParameters node to facilitate log comparisons"""
    for key in ("predicate", "matchedPredicates", "notMatchedPredicates"):
        pred = opp.get(key)
        if pred:
            subbed = TMP_TABLE_PATTERN.sub("tmp_table", pred)
            subbed = TMP_TABLE_PATH_PATTERN.sub("tmp_table", subbed)
            opp[key] = REF_ID_PATTERN.sub("#refid", subbed)

def assert_delta_history_equal(conf, cpu_table, gpu_table):
    # Project all columns except for the `timestamp` column, which won't match between CPU and GPU.
    cols = ["version", "userId", "userName", "operation", "operationParameters", "job", "notebook",
            "clusterId", "readVersion", "isolationLevel", "isBlindAppend", "operationMetrics", "userMetadata"]
    cpu_history = with_cpu_session(lambda spark: spark.sql("DESCRIBE HISTORY {}".format(cpu_table))
                                   .select(cols).collect(), conf=conf)
    gpu_history = with_cpu_session(lambda spark: spark.sql("DESCRIBE HISTORY {}".format(gpu_table))
                                   .select(cols).collect(), conf=conf)
    assert_equal(cpu_history, gpu_history)


def assert_delta_log_json_equivalent(filename, c_json, g_json):
    assert c_json.keys() == g_json.keys(), "Delta log {} has mismatched keys:\nCPU: {}\nGPU: {}".format(filename, c_json, g_json)
    def fixup_path(d):
        """Modify the 'path' value to remove random IDs in the pathname"""
        parts = d["path"].split("-")
        d["path"] = "-".join(parts[0:1]) + ".".join(parts[-1].split(".")[-2:])
    def del_keys(key_list, c_val, g_val):
        for key in key_list:
            c_val.pop(key, None)
            g_val.pop(key, None)
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
                _fixup_operation_metrics(v.get("operationMetrics", {}))
                _fixup_operation_parameters(v.get("operationParameters", {}))
        elif key == "remove":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'remove' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("deletionTimestamp", "size"), c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        assert c_val == g_val, "Delta log {} is different at key '{}':\nCPU: {}\nGPU: {}".format(filename, key, c_val, g_val)

def _decode_jsons(json_data):
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
        stats = sorted([ v.get("stats", "") for v in j.values() ])
        paths = sorted([ v.get("path", "") for v in j.values() ])
        return ','.join(keys + stats + paths)
    jsons.sort(key=json_to_sort_key)
    return jsons

def read_delta_logs(spark, path):
    log_data = spark.sparkContext.wholeTextFiles(path).collect()
    return dict([(os.path.basename(x), _decode_jsons(y)) for x, y in log_data])

def assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path):
    cpu_log_data = spark.sparkContext.wholeTextFiles(data_path + "/CPU/_delta_log/*").collect()
    gpu_log_data = spark.sparkContext.wholeTextFiles(data_path + "/GPU/_delta_log/*").collect()
    assert len(cpu_log_data) == len(gpu_log_data), "Different number of Delta log files:\nCPU: {}\nGPU: {}".format(cpu_log_data, gpu_log_data)
    cpu_logs_data = [ (os.path.basename(x), y) for x, y in cpu_log_data if x.endswith(".json") ]
    gpu_logs_dict = dict([ (os.path.basename(x), y) for x, y in gpu_log_data if x.endswith(".json") ])
    for file, cpu_json_data in cpu_logs_data:
        gpu_json_data = gpu_logs_dict.get(file)
        assert gpu_json_data, "CPU Delta log file {} is missing from GPU Delta logs".format(file)
        cpu_jsons = _decode_jsons(cpu_json_data)
        gpu_jsons = _decode_jsons(gpu_json_data)
        assert len(cpu_jsons) == len(gpu_jsons), "Different line counts in {}:\nCPU: {}\nGPU: {}".format(file, cpu_json_data, gpu_json_data)
        for cpu_json, gpu_json in zip(cpu_jsons, gpu_jsons):
            assert_delta_log_json_equivalent(file, cpu_json, gpu_json)

def read_delta_path(spark, path):
    return spark.read.format("delta").load(path)

def read_delta_path_with_cdf(spark, path):
    return spark.read.format("delta") \
        .option("readChangeDataFeed", "true").option("startingVersion", 0) \
        .load(path).drop("_commit_timestamp")

def schema_to_ddl(spark, schema):
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json()).toDDL()

def setup_delta_dest_table(spark, path, dest_table_func, use_cdf, partition_columns=None, enable_deletion_vectors=False):
    dest_df = dest_table_func(spark)
    # append to SQL-created table
    writer = dest_df.write.format("delta").mode("append")
    ddl = schema_to_ddl(spark, dest_df.schema)
    table_properties = {}
    table_properties['delta.enableChangeDataFeed'] = str(use_cdf).lower()
    if supports_delta_lake_deletion_vectors():
        table_properties['delta.enableDeletionVectors'] = str(enable_deletion_vectors).lower()
    # if any table properties are specified then we need to use SQL to define the table
    sql_text = "CREATE TABLE delta.`{path}` ({ddl}) USING DELTA".format(path=path, ddl=ddl)
    if partition_columns:
        sql_text += " PARTITIONED BY ({})".format(",".join(partition_columns))
        writer = writer.partitionBy(*partition_columns)
    properties = ', '.join(key + ' = ' + value for key, value in table_properties.items())
    sql_text += " TBLPROPERTIES ({})".format(properties)
    spark.sql(sql_text)
    writer.save(path)

def setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, enable_deletion_vectors, partition_columns=None):
    for name in ["CPU", "GPU"]:
        path = "{}/{}".format(data_path, name)
        setup_delta_dest_table(spark, path, dest_table_func, use_cdf, partition_columns, enable_deletion_vectors)
