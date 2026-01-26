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
Integration tests for SequenceFile RDD conversion and GPU acceleration.

The SequenceFile support in spark-rapids works via the SequenceFileRDDConversionRule,
which converts RDD-based SequenceFile scans (e.g., sc.newAPIHadoopFile with
SequenceFileInputFormat) to FileFormat-based scans that can be GPU-accelerated.

This conversion is disabled by default and must be enabled via:
  spark.rapids.sql.sequenceFile.rddConversion.enabled=true

If the conversion fails or GPU doesn't support the operation, the original RDD scan
is preserved (no fallback to CPU FileFormat).
"""

import pytest
import struct

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session

# Reader types supported by SequenceFile (COALESCING is not supported)
sequencefile_reader_types = ['PERFILE', 'MULTITHREADED']

# Base config to enable SequenceFile RDD conversion
sequencefile_conversion_conf = {
    'spark.rapids.sql.sequenceFile.rddConversion.enabled': 'true'
}


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
    When spark.rapids.sql.sequenceFile.rddConversion.enabled=true,
    this should be converted to FileFormat-based scan.
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
        **sequencefile_conversion_conf,
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
        **sequencefile_conversion_conf,
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
        **sequencefile_conversion_conf,
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
        **sequencefile_conversion_conf,
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_row_counts_equal(
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
        **sequencefile_conversion_conf,
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)


# ============================================================================
# Configuration Tests
# ============================================================================

def test_conversion_disabled_by_default(spark_tmp_path):
    """Test that RDD conversion is disabled by default."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    payloads = [b'test']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    # Without enabling conversion, this should still work via the original RDD path
    # (no conversion happens, just regular RDD execution)
    all_confs = {
        # Note: NOT enabling sequencefile.rddConversion
    }
    
    # This should work - the RDD path still functions, just without conversion
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
        **sequencefile_conversion_conf,
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_sequencefile_via_rdd(spark, data_path),
        conf=all_confs)
