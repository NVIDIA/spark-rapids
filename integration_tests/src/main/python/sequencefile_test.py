# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""
Integration tests for SequenceFile RDD reads with RAPIDS plugin enabled.
"""

import pytest
import os
import struct

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, is_databricks_runtime

# Reader types supported by SequenceFile (COALESCING is not supported)
# AUTO is accepted for compatibility and resolves to MULTITHREADED.
sequencefile_reader_types = ['AUTO', 'MULTITHREADED']


def write_sequencefile_with_rdd(spark, data_path, payloads):
    """
    Write an uncompressed SequenceFile using Spark's RDD saveAsNewAPIHadoopFile method.
    payloads: list of byte arrays to be written as values (keys will be incrementing integers).
    
    This writes actual BytesWritable key/value pairs.
    """
    sc = spark.sparkContext
    
    # Create (key, value) pairs where key is 4-byte big-endian integer
    # Convert to bytearray for proper BytesWritable serialization
    records = [(bytearray(struct.pack('>I', idx)), bytearray(payload)) 
               for idx, payload in enumerate(payloads)]
    
    # Create RDD and save as SequenceFile using Hadoop API
    rdd = sc.parallelize(records, 1)
    
    # Use saveAsNewAPIHadoopFile with BytesWritable key/value classes
    rdd.saveAsNewAPIHadoopFile(
        data_path,
        "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.io.BytesWritable"
    )


def read_sequencefile_via_rdd(spark, data_path):
    """
    Read a SequenceFile using the RDD path.
    Reads data through the RDD SequenceFile path.
    """
    sc = spark.sparkContext
    rdd = sc.newAPIHadoopFile(
        data_path,
        "org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.io.BytesWritable"
    )
    
    # Map to extract raw bytes (BytesWritable has length prefix)
    def extract_bytes(kv):
        k, v = kv
        # BytesWritable stores data after a 4-byte length prefix
        return (bytes(k[4:]) if len(k) > 4 else bytes(k),
                bytes(v[4:]) if len(v) > 4 else bytes(v))
    
    mapped_rdd = rdd.map(extract_bytes)
    # Use explicit schema to avoid schema inference failure on empty RDD
    schema = StructType([
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True)
    ])
    return spark.createDataFrame(mapped_rdd, schema)


def read_sequencefile_value_only(spark, data_path):
    """
    Read only the value column from a SequenceFile (common pattern for protobuf payloads).
    """
    sc = spark.sparkContext
    rdd = sc.newAPIHadoopFile(
        data_path,
        "org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.io.BytesWritable"
    )
    
    def extract_value(kv):
        _, v = kv
        # BytesWritable stores data after a 4-byte length prefix
        return (bytes(v[4:]) if len(v) > 4 else bytes(v),)
    
    mapped_rdd = rdd.map(extract_value)
    # Use explicit schema to avoid schema inference failure on empty RDD
    schema = StructType([
        StructField("value", BinaryType(), True)
    ])
    return spark.createDataFrame(mapped_rdd, schema)


def write_corrupt_file(path, payload=b'not-a-sequence-file'):
    """Write a non-SequenceFile payload to simulate a corrupt input file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(payload)


# ============================================================================
# Basic Read Tests
# ============================================================================

@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_basic_read(spark_tmp_path, reader_type):
    """Test basic SequenceFile reading via RDD conversion."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Write test data using CPU
    payloads = [
        b'\x01\x02\x03',
        b'hello world',
        b'\xff' * 10
    ]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_read_value_only(spark_tmp_path, reader_type):
    """Test reading only the value column."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    payloads = [b'value1', b'value2', b'value3']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_value_only(spark, data_path),
        conf=all_confs)


# ============================================================================
# Empty File Tests
# ============================================================================

@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_empty_file(spark_tmp_path, reader_type):
    """Test reading an empty SequenceFile."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Write empty file
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, []))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


# ============================================================================
# Large Data Tests
# ============================================================================

@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_large_batch(spark_tmp_path, reader_type):
    """Test reading many records to verify batch handling."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create many records
    num_records = 1000
    payloads = [f'record_{i}_with_some_data'.encode() for i in range(num_records)]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_large_records(spark_tmp_path, reader_type):
    """Test reading records with large values."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create records with varying sizes, including some large ones
    payloads = [b'x' * (1024 * i) for i in range(1, 11)]  # 1KB to 10KB
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


def test_multithreaded_reader_combine_mode_correctness(spark_tmp_path):
    """Test MULTITHREADED reader combine mode with many small files."""
    base_path = spark_tmp_path + '/SEQFILE_COMBINE_DATA'
    payload_sets = [
        [b'a1', b'a2', b'a3'],
        [b'b1', b'b2'],
        [b'c1', b'c2', b'c3', b'c4']
    ]

    def write_all_files(spark):
        for idx, payloads in enumerate(payload_sets):
            write_sequencefile_with_rdd(spark, f'{base_path}/part_{idx}', payloads)

    with_cpu_session(write_all_files)

    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': 'MULTITHREADED',
        # Force combine behavior in multithreaded reader.
        'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '1',
        'spark.rapids.sql.reader.multithreaded.combine.waitTime': '1',
        'spark.rapids.sql.files.maxPartitionBytes': str(1 << 20),
    }

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_value_only(spark, base_path + '/*'),
        conf=all_confs)


# ============================================================================
# Configuration Tests
# ============================================================================

def test_rdd_path_when_physical_replacement_disabled(spark_tmp_path):
    """Test that the original RDD path still works when physical replacement is disabled."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    payloads = [b'test']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled': 'false'
    }
    
    # This should work via the original RDD path with physical replacement explicitly disabled.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


# ============================================================================
# Binary Data Tests
# ============================================================================

@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_binary_data(spark_tmp_path, reader_type):
    """Test reading various binary data patterns."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    payloads = [
        bytes(range(256)),           # All byte values
        b'\x00' * 100,               # Nulls
        b'\xff' * 100,               # All 1s
        b''.join(struct.pack('<d', float(i)) for i in range(10)),  # Doubles
    ]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_sequencefile_read_with_missing_files(spark_tmp_path, reader_type):
    """PySpark newAPIHadoopFile still errors on missing inputs before physical replacement."""
    existing_path = spark_tmp_path + '/SEQFILE_MISSING_DATA/existing'
    missing_path = spark_tmp_path + '/SEQFILE_MISSING_DATA/missing'

    payloads = [b'x1', b'x2']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, existing_path, payloads))

    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type,
        'spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled': 'true',
        'spark.sql.files.ignoreMissingFiles': 'true'
    }

    assert_gpu_and_cpu_error(
        lambda spark: read_sequencefile_value_only(
            spark, f"{existing_path},{missing_path}").collect(),
        conf=all_confs,
        error_message="Input path does not exist")


@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_sequencefile_read_with_corrupt_files(spark_tmp_path, reader_type):
    """RDD path still throws when a file is not a valid SequenceFile."""
    good_path = spark_tmp_path + '/SEQFILE_CORRUPT_DATA/good'
    corrupt_path = spark_tmp_path + '/SEQFILE_CORRUPT_DATA/corrupt/part-00000'

    payloads = [b'good-a', b'good-b']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, good_path, payloads))
    write_corrupt_file(corrupt_path)

    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type,
        'spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled': 'false',
        'spark.sql.files.ignoreCorruptFiles': 'true'
    }

    assert_gpu_and_cpu_error(
        lambda spark: read_sequencefile_value_only(
            spark, f"{good_path},{corrupt_path}").collect(),
        conf=all_confs,
        error_message="not a SequenceFile")
