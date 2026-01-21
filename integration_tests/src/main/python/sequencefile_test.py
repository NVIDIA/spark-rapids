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

import pytest
import struct

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session

# Reader types supported by SequenceFile (COALESCING is not supported)
sequencefile_reader_types = ['PERFILE', 'MULTITHREADED']


def read_sequencefile_df(data_path):
    """Helper function to read SequenceFile using DataFrame API."""
    return lambda spark: spark.read.format("sequencefilebinary").load(data_path)


def write_sequencefile_with_rdd(spark, data_path, payloads):
    """
    Write an uncompressed SequenceFile using Spark's RDD saveAsNewAPIHadoopFile method.
    payloads: list of byte arrays to be written as values (keys will be incrementing integers).
    
    This writes actual BytesWritable key/value pairs that can be read by the 
    sequencefilebinary format.
    """
    sc = spark.sparkContext
    
    # Create (key, value) pairs where key is 4-byte big-endian integer
    # Convert to bytearray for proper BytesWritable serialization
    records = [(bytearray(struct.pack('>I', idx)), bytearray(payload)) 
               for idx, payload in enumerate(payloads)]
    
    # Create RDD and save as SequenceFile using Hadoop API
    rdd = sc.parallelize(records, 1)
    
    # Use saveAsNewAPIHadoopFile with BytesWritable key/value classes
    # and SequenceFileOutputFormat
    rdd.saveAsNewAPIHadoopFile(
        data_path,
        "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.io.BytesWritable"
    )


# ============================================================================
# Basic Read Tests
# ============================================================================

@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_basic_read(spark_tmp_path, reader_type):
    """Test basic SequenceFile reading with different reader types."""
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
        read_sequencefile_df(data_path),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_read_key_only(spark_tmp_path, reader_type):
    """Test reading only the key column (column pruning)."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    payloads = [b'value1', b'value2', b'value3']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("sequencefilebinary").load(data_path).select("key"),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_read_value_only(spark_tmp_path, reader_type):
    """Test reading only the value column (column pruning)."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    payloads = [b'value1', b'value2', b'value3']
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("sequencefilebinary").load(data_path).select("value"),
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
        read_sequencefile_df(data_path),
        conf=all_confs)


# ============================================================================
# Multi-file Tests
# ============================================================================

@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_multi_file_read(spark_tmp_path, reader_type):
    """Test reading multiple SequenceFiles from a directory."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Write multiple files
    for i in range(3):
        file_path = data_path + f'/file{i}'
        payloads = [f'file{i}_record{j}'.encode() for j in range(5)]
        with_cpu_session(lambda spark, p=payloads, fp=file_path: 
                        write_sequencefile_with_rdd(spark, fp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
        conf=all_confs)


# ============================================================================
# Partitioned Read Tests
# ============================================================================

@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_partitioned_read(spark_tmp_path, reader_type):
    """Test reading SequenceFiles with Hive-style partitioning."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create partitioned directory structure
    for part_val in ['a', 'b', 'c']:
        part_path = data_path + f'/part={part_val}'
        payloads = [f'{part_val}_record{i}'.encode() for i in range(3)]
        with_cpu_session(lambda spark, p=payloads, pp=part_path: 
                        write_sequencefile_with_rdd(spark, pp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    # Read and verify both data columns and partition column
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("sequencefilebinary").load(data_path)
            .select("key", "value", "part"),
        conf=all_confs)


@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_partitioned_read_just_partitions(spark_tmp_path, reader_type):
    """Test reading only partition columns from SequenceFiles."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create partitioned directory structure - use 'pkey' to avoid collision with 'key' data column
    for part_val in [0, 1, 2]:
        part_path = data_path + f'/pkey={part_val}'
        payloads = [f'record{i}'.encode() for i in range(2)]
        with_cpu_session(lambda spark, p=payloads, pp=part_path: 
                        write_sequencefile_with_rdd(spark, pp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    # Select only the partition column
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("sequencefilebinary").load(data_path).select("pkey"),
        conf=all_confs)


@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_nested_partitions(spark_tmp_path, reader_type):
    """Test reading SequenceFiles with nested partitioning."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create nested partitioned directory structure - use 'pkey' to avoid collision with 'key' data column
    for pkey in [0, 1]:
        for pkey2 in [20, 21]:
            part_path = data_path + f'/pkey={pkey}/pkey2={pkey2}'
            payloads = [f'key{pkey}_key2{pkey2}_rec{i}'.encode() for i in range(2)]
            with_cpu_session(lambda spark, p=payloads, pp=part_path: 
                            write_sequencefile_with_rdd(spark, pp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
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
    payloads = [f'record-{i}-payload-data'.encode() for i in range(num_records)]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_read_count(spark_tmp_path, reader_type):
    """Test row count operation on SequenceFiles."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    num_records = 500
    payloads = [f'record-{i}'.encode() for i in range(num_records)]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_row_counts_equal(
        read_sequencefile_df(data_path),
        conf=all_confs)


# ============================================================================
# Varied Record Sizes Tests
# ============================================================================

@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_varied_record_sizes(spark_tmp_path, reader_type):
    """Test reading SequenceFiles with varied record sizes."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create records with varying sizes
    payloads = [
        b'',                           # Empty
        b'x',                          # 1 byte
        b'small',                      # Small
        b'medium-sized-record' * 10,   # Medium
        b'large-record' * 1000,        # Large (~13KB)
    ]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
        conf=all_confs)


@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_binary_data(spark_tmp_path, reader_type):
    """Test reading SequenceFiles with binary data (all byte values)."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create records with various binary patterns
    payloads = [
        bytes(range(256)),             # All byte values 0-255
        bytes([0] * 100),              # All zeros
        bytes([255] * 100),            # All ones
        bytes([0xDE, 0xAD, 0xBE, 0xEF] * 25),  # Pattern
    ]
    with_cpu_session(lambda spark: write_sequencefile_with_rdd(spark, data_path, payloads))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
        conf=all_confs)


# ============================================================================
# Filter Tests  
# ============================================================================

@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_filter_on_partition(spark_tmp_path, reader_type):
    """Test filtering on partition column."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create partitioned data
    for part_val in ['a', 'b', 'c']:
        part_path = data_path + f'/part={part_val}'
        payloads = [f'{part_val}_record{i}'.encode() for i in range(5)]
        with_cpu_session(lambda spark, p=payloads, pp=part_path: 
                        write_sequencefile_with_rdd(spark, pp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    # Filter on partition column
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("sequencefilebinary").load(data_path)
            .filter(f.col('part') == 'a'),
        conf=all_confs)


# ============================================================================
# Input File Metadata Tests
# ============================================================================

@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', sequencefile_reader_types)
def test_input_file_meta(spark_tmp_path, reader_type):
    """Test reading input file metadata."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create multiple files in partitioned structure - use 'pkey' to avoid collision with 'key' data column
    for pkey in [0, 1]:
        part_path = data_path + f'/pkey={pkey}'
        payloads = [f'key{pkey}_record{i}'.encode() for i in range(3)]
        with_cpu_session(lambda spark, p=payloads, pp=part_path: 
                        write_sequencefile_with_rdd(spark, pp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': reader_type
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("sequencefilebinary").load(data_path)
            .selectExpr('value',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
        conf=all_confs)


# ============================================================================
# Multithreaded Reader Tests
# ============================================================================

@ignore_order(local=True)
def test_multithreaded_max_files_parallel(spark_tmp_path):
    """Test multithreaded reader with limited parallel file count."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create multiple small files
    for i in range(10):
        file_path = data_path + f'/file{i}'
        payloads = [f'file{i}_record{j}'.encode() for j in range(5)]
        with_cpu_session(lambda spark, p=payloads, fp=file_path: 
                        write_sequencefile_with_rdd(spark, fp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': 'MULTITHREADED',
        'spark.rapids.sql.format.sequencefile.multiThreadedRead.maxNumFilesParallel': '3'
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
        conf=all_confs)


# ============================================================================
# AUTO Reader Type Tests
# ============================================================================

@ignore_order(local=True)
def test_auto_reader_type(spark_tmp_path):
    """Test AUTO reader type selection."""
    data_path = spark_tmp_path + '/SEQFILE_DATA'
    
    # Create test files
    for i in range(3):
        file_path = data_path + f'/file{i}'
        payloads = [f'file{i}_record{j}'.encode() for j in range(5)]
        with_cpu_session(lambda spark, p=payloads, fp=file_path: 
                        write_sequencefile_with_rdd(spark, fp, p))
    
    all_confs = {
        'spark.rapids.sql.format.sequencefile.reader.type': 'AUTO'
    }
    
    assert_gpu_and_cpu_are_equal_collect(
        read_sequencefile_df(data_path),
        conf=all_confs)
