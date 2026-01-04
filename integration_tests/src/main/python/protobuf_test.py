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

import inspect

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import (
    BooleanGen, IntegerGen, LongGen, FloatGen, DoubleGen, StringGen,
    ProtobufSimpleMessageRowGen, gen_df
)
from marks import ignore_order
from spark_session import with_cpu_session, is_before_spark_340
import pyspark.sql.functions as f

pytestmark = [pytest.mark.premerge_ci_1]


def _try_import_from_protobuf():
    try:
        from pyspark.sql.protobuf.functions import from_protobuf
        return from_protobuf
    except Exception:
        return None


def _spark_protobuf_jvm_available(spark) -> bool:
    """
    `spark-protobuf` is an optional external module. PySpark may have the Python wrappers
    even when the JVM side isn't present on the classpath, which manifests as:
      TypeError: 'JavaPackage' object is not callable
    when calling into `sc._jvm.org.apache.spark.sql.protobuf.functions.from_protobuf`.
    """
    jvm = spark.sparkContext._jvm
    candidates = [
        # Scala object `functions` compiles to `functions$`
        "org.apache.spark.sql.protobuf.functions$",
        # Some environments may expose it differently
        "org.apache.spark.sql.protobuf.functions",
    ]
    for cls in candidates:
        try:
            jvm.java.lang.Class.forName(cls)
            return True
        except Exception:
            continue
    return False


def _build_simple_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for:
      package test;
      syntax = "proto2";
      message Simple {
        optional bool   b   = 1;
        optional int32  i32 = 2;
        optional int64  i64 = 3;
        optional float  f32 = 4;
        optional double f64 = 5;
        optional string s   = 6;
      }
    """
    jvm = spark.sparkContext._jvm
    D = jvm.com.google.protobuf.DescriptorProtos

    fd = D.FileDescriptorProto.newBuilder() \
        .setName("simple.proto") \
        .setPackage("test")
    # Some Spark distributions bring an older protobuf-java where FileDescriptorProto.Builder
    # does not expose setSyntax(String). For this test we only need proto2 semantics, and
    # leaving syntax unset is sufficient/compatible.
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        # If setSyntax is unavailable (older protobuf-java), we intentionally leave syntax unset.
        pass

    msg = D.DescriptorProto.newBuilder().setName("Simple")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    def add_field(name, number, ftype):
        msg.addField(
            D.FieldDescriptorProto.newBuilder()
              .setName(name)
              .setNumber(number)
              .setLabel(label_opt)
              .setType(ftype)
              .build()
        )

    add_field("b", 1, D.FieldDescriptorProto.Type.TYPE_BOOL)
    add_field("i32", 2, D.FieldDescriptorProto.Type.TYPE_INT32)
    add_field("i64", 3, D.FieldDescriptorProto.Type.TYPE_INT64)
    add_field("f32", 4, D.FieldDescriptorProto.Type.TYPE_FLOAT)
    add_field("f64", 5, D.FieldDescriptorProto.Type.TYPE_DOUBLE)
    add_field("s", 6, D.FieldDescriptorProto.Type.TYPE_STRING)

    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    # py4j converts Java byte[] to a Python bytes-like object
    return bytes(fds.toByteArray())


def _write_bytes_to_hadoop_path(spark, path_str, data_bytes):
    sc = spark.sparkContext
    config = sc._jsc.hadoopConfiguration()
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path_str)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    out = fs.create(jpath, True)
    try:
        out.write(bytearray(data_bytes))
    finally:
        out.close()


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_simple_parquet_binary_round_trip(spark_tmp_path):
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    data_path = spark_tmp_path + "/PROTOBUF_SIMPLE_PARQUET/"
    desc_path = spark_tmp_path + "/simple.desc"
    message_name = "test.Simple"

    # Generate descriptor bytes once using the JVM (no protoc dependency)
    desc_bytes = with_cpu_session(_build_simple_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Build a DF with scalar columns + binary protobuf column and write to parquet
    row_gen = ProtobufSimpleMessageRowGen([
        ("b", 1, BooleanGen(nullable=True)),
        ("i32", 2, IntegerGen(nullable=True, min_val=0, max_val=1 << 20)),
        ("i64", 3, LongGen(nullable=True, min_val=0, max_val=1 << 40, special_cases=[])),
        ("f32", 4, FloatGen(nullable=True, no_nans=True)),
        ("f64", 5, DoubleGen(nullable=True, no_nans=True)),
        ("s", 6, StringGen(nullable=True)),
    ], binary_col_name="bin")

    def write_parquet(spark):
        df = gen_df(spark, row_gen, length=512)
        df.write.mode("overwrite").parquet(data_path)

    with_cpu_session(write_parquet)

    # Sanity check correctness on CPU (decoded struct matches the original scalar columns)
    def cpu_correctness_check(spark):
        df = spark.read.parquet(data_path)
        expected = f.struct(
            f.col("b").alias("b"),
            f.col("i32").alias("i32"),
            f.col("i64").alias("i64"),
            f.col("f32").alias("f32"),
            f.col("f64").alias("f64"),
            f.col("s").alias("s"),
        ).alias("expected")

        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(f.col("bin"), message_name, binaryDescriptorSet=bytearray(desc_bytes)).alias("decoded")
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path).alias("decoded")

        rows = df.select(expected, decoded).collect()
        for r in rows:
            assert r["expected"] == r["decoded"]

    with_cpu_session(cpu_correctness_check)

    # Main assertion: CPU and GPU results match for from_protobuf on a binary column read from parquet
    def run_on_spark(spark):
        df = spark.read.parquet(data_path)
        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(f.col("bin"), message_name, binaryDescriptorSet=bytearray(desc_bytes))
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path)
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_simple_null_input_returns_null(spark_tmp_path):
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/simple_null_input.desc"
    message_name = "test.Simple"

    # Generate descriptor bytes once using the JVM (no protoc dependency)
    desc_bytes = with_cpu_session(_build_simple_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Spark's ProtobufDataToCatalyst is NullIntolerant (null input -> null output).
    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(None,), (bytes([0x08, 0x01, 0x10, 0x7B]),)],  # b=true, i32=123
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes),
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path)
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)
