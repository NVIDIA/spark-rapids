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


def _build_nested_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with both simple fields and nested message:
      package test;
      syntax = "proto2";
      message Nested {
        optional int32 x = 1;
      }
      message WithNested {
        optional int32  simple_int  = 1;
        optional string simple_str  = 2;
        optional Nested nested_msg  = 3;   // nested message - not supported by GPU
        optional int64  simple_long = 4;
      }
    """
    jvm = spark.sparkContext._jvm
    D = jvm.com.google.protobuf.DescriptorProtos

    fd = D.FileDescriptorProto.newBuilder() \
        .setName("nested.proto") \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Define Nested message
    nested_msg = D.DescriptorProto.newBuilder().setName("Nested")
    nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("x")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    fd.addMessageType(nested_msg.build())

    # Define WithNested message
    with_nested_msg = D.DescriptorProto.newBuilder().setName("WithNested")
    # simple_int
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("simple_int")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    # simple_str
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("simple_str")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    # nested_msg (nested message type)
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("nested_msg")
            .setNumber(3)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Nested")
            .build()
    )
    # simple_long
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("simple_long")
            .setNumber(4)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT64)
            .build()
    )
    fd.addMessageType(with_nested_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_schema_projection_simple_fields_only(spark_tmp_path):
    """
    Test schema projection: when only simple fields are selected from a protobuf message
    that also contains unsupported types (nested message), GPU should be able to decode
    just the simple fields without falling back to CPU.
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/nested.desc"
    message_name = "test.WithNested"

    desc_bytes = with_cpu_session(_build_nested_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data: protobuf binary with simple fields set
    # Field 1 (simple_int): varint 42 -> 0x08 0x2A
    # Field 2 (simple_str): length-delimited "hello" -> 0x12 0x05 h e l l o
    # Field 4 (simple_long): varint 12345 -> 0x20 0xB9 0x60
    test_data = bytes([
        0x08, 0x2A,  # simple_int = 42
        0x12, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F,  # simple_str = "hello"
        0x20, 0xB9, 0x60,  # simple_long = 12345
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data,), (None,)],
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
        # Only select simple fields, not the nested_msg field
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
            decoded.getField("simple_long").alias("simple_long")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_enum_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with enum field:
      package test;
      syntax = "proto2";
      message WithEnum {
        enum Color {
          RED = 0;
          GREEN = 1;
          BLUE = 2;
        }
        optional Color color = 1;
        optional int32 count = 2;
        optional string name = 3;
      }
    """
    jvm = spark.sparkContext._jvm
    D = jvm.com.google.protobuf.DescriptorProtos

    fd = D.FileDescriptorProto.newBuilder() \
        .setName("enum.proto") \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Define WithEnum message with nested enum
    msg = D.DescriptorProto.newBuilder().setName("WithEnum")

    # Add enum type definition
    enum_type = D.EnumDescriptorProto.newBuilder().setName("Color")
    enum_type.addValue(D.EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0).build())
    enum_type.addValue(D.EnumValueDescriptorProto.newBuilder().setName("GREEN").setNumber(1).build())
    enum_type.addValue(D.EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(2).build())
    msg.addEnumType(enum_type.build())

    # Add color field (enum type)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("color")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_ENUM)
            .setTypeName(".test.WithEnum.Color")
            .build()
    )
    # Add count field (int32)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("count")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    # Add name field (string)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(3)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_enum_as_int(spark_tmp_path):
    """
    Test enum field decoded as integer with enums.as.ints=true option.
    GPU should decode enum fields as INT32 values matching CPU behavior.
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/enum.desc"
    message_name = "test.WithEnum"

    desc_bytes = with_cpu_session(_build_enum_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data with various enum values:
    # Row 0: color=GREEN(1), count=42, name="test"
    # Row 1: color=RED(0), count=100, name missing
    # Row 2: color missing, count=200, name="hello"
    # Row 3: null input
    test_data_row0 = bytes([
        0x08, 0x01,  # color = GREEN (1)
        0x10, 0x2A,  # count = 42
        0x1A, 0x04, 0x74, 0x65, 0x73, 0x74,  # name = "test"
    ])
    test_data_row1 = bytes([
        0x08, 0x00,  # color = RED (0)
        0x10, 0x64,  # count = 100
    ])
    test_data_row2 = bytes([
        0x10, 0xC8, 0x01,  # count = 200
        0x1A, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F,  # name = "hello"
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,), (test_data_row2,), (None,)],
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        options = {"enums.as.ints": "true"}
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes),
                options=options
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path, options)
        return df.select(
            decoded.getField("color").alias("color"),
            decoded.getField("count").alias("count"),
            decoded.getField("name").alias("name")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_enum_unknown_value(spark_tmp_path):
    """
    Test that unknown enum values (not defined in enum) null the entire struct row.
    Both GPU and CPU implementations (PERMISSIVE mode) null the entire row when
    an unknown enum value is encountered.
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/enum_unknown.desc"
    message_name = "test.WithEnum"

    desc_bytes = with_cpu_session(_build_enum_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data with unknown enum value (999 is not defined in Color enum)
    # 999 encoded as varint: 0xE7 0x07
    test_data = bytes([
        0x08, 0xE7, 0x07,  # color = 999 (unknown value)
        0x10, 0x2A,  # count = 42
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data,)],
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        # Use PERMISSIVE mode to allow unknown enum values to pass through
        options = {"enums.as.ints": "true", "mode": "PERMISSIVE"}
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes),
                options=options
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path, options)
        return df.select(
            decoded.getField("color").alias("color"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_required_field_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with required fields (proto2):
      package test;
      syntax = "proto2";
      message WithRequired {
        required int64 id = 1;
        optional string name = 2;
        optional int32 count = 3;
      }
    """
    jvm = spark.sparkContext._jvm
    D = jvm.com.google.protobuf.DescriptorProtos

    fd = D.FileDescriptorProto.newBuilder() \
        .setName("required.proto") \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass

    label_required = D.FieldDescriptorProto.Label.LABEL_REQUIRED
    label_optional = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithRequired")
    
    # id field (required)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_required)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT64)
            .build()
    )
    # name field (optional)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(2)
            .setLabel(label_optional)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    # count field (optional)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("count")
            .setNumber(3)
            .setLabel(label_optional)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_present(spark_tmp_path):
    """
    Test that required fields decode correctly when present.
    GPU should produce same results as CPU.
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/required.desc"
    message_name = "test.WithRequired"

    desc_bytes = with_cpu_session(_build_required_field_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data with required field present
    # Row 0: id=100, name="test", count=42
    # Row 1: id=200, name missing, count missing
    test_data_row0 = bytes([
        0x08, 0x64,  # id = 100
        0x12, 0x04, 0x74, 0x65, 0x73, 0x74,  # name = "test"
        0x18, 0x2A,  # count = 42
    ])
    test_data_row1 = bytes([
        0x08, 0xC8, 0x01,  # id = 200
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,)],
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes)
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_default_value_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with default values (proto2):
      package test;
      syntax = "proto2";
      message WithDefaults {
        optional int32 count = 1 [default = 42];
        optional string name = 2 [default = "unknown"];
        optional bool flag = 3 [default = true];
      }
    Note: Setting explicit defaults requires using FieldOptions which may not be
    available via the simple DescriptorProtos API. For testing, we rely on proto2
    implicit behavior where hasDefaultValue() returns true for optional fields.
    """
    jvm = spark.sparkContext._jvm
    D = jvm.com.google.protobuf.DescriptorProtos

    fd = D.FileDescriptorProto.newBuilder() \
        .setName("defaults.proto") \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass

    label_optional = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithDefaults")
    
    # count field with default
    count_field = D.FieldDescriptorProto.newBuilder() \
        .setName("count") \
        .setNumber(1) \
        .setLabel(label_optional) \
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32) \
        .setDefaultValue("42")
    msg.addField(count_field.build())
    
    # name field with default
    name_field = D.FieldDescriptorProto.newBuilder() \
        .setName("name") \
        .setNumber(2) \
        .setLabel(label_optional) \
        .setType(D.FieldDescriptorProto.Type.TYPE_STRING) \
        .setDefaultValue("unknown")
    msg.addField(name_field.build())
    
    # flag field with default
    flag_field = D.FieldDescriptorProto.newBuilder() \
        .setName("flag") \
        .setNumber(3) \
        .setLabel(label_optional) \
        .setType(D.FieldDescriptorProto.Type.TYPE_BOOL) \
        .setDefaultValue("true")
    msg.addField(flag_field.build())

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_default_values_field_present(spark_tmp_path):
    """
    Test that when fields with defaults are present, the actual values are used.
    This validates the GPU correctly decodes present values (not using defaults).
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/defaults.desc"
    message_name = "test.WithDefaults"

    desc_bytes = with_cpu_session(_build_default_value_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data where all fields are present (actual values, not defaults)
    test_data = bytes([
        0x08, 0x64,  # count = 100 (not the default 42)
        0x12, 0x04, 0x74, 0x65, 0x73, 0x74,  # name = "test" (not "unknown")
        0x18, 0x00,  # flag = false (not true)
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data,)],
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes)
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path)
        return df.select(
            decoded.getField("count").alias("count"),
            decoded.getField("name").alias("name"),
            decoded.getField("flag").alias("flag")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_default_values_missing_fields(spark_tmp_path):
    """
    Test that when fields with defaults are missing, the default values are filled in.
    This validates the GPU correctly fills default values for missing fields.
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/defaults.desc"
    message_name = "test.WithDefaults"

    desc_bytes = with_cpu_session(_build_default_value_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data where all fields are MISSING (should use defaults)
    # Empty protobuf message
    test_data_empty = bytes([])
    
    # Partial message: only count field is present
    test_data_partial = bytes([
        0x08, 0x64,  # count = 100
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_empty,), (test_data_partial,)],
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes)
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path)
        return df.select(
            decoded.getField("count").alias("count"),
            decoded.getField("name").alias("name"),
            decoded.getField("flag").alias("flag")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_string_default_value(spark_tmp_path):
    """
    Test string default value specifically.
    """
    from_protobuf = _try_import_from_protobuf()
    if from_protobuf is None:
        pytest.skip("pyspark.sql.protobuf.functions.from_protobuf is not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM module is not available on the classpath")

    desc_path = spark_tmp_path + "/defaults.desc"
    message_name = "test.WithDefaults"

    desc_bytes = with_cpu_session(_build_default_value_descriptor_set_bytes)
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    # Create test data where only non-string fields are present
    # name field is missing, should use default "unknown"
    test_data = bytes([
        0x08, 0x2A,  # count = 42
        0x18, 0x01,  # flag = true
    ])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data,)],
            schema="bin binary",
        )
        sig = inspect.signature(from_protobuf)
        if "binaryDescriptorSet" in sig.parameters:
            decoded = from_protobuf(
                f.col("bin"),
                message_name,
                binaryDescriptorSet=bytearray(desc_bytes)
            )
        else:
            decoded = from_protobuf(f.col("bin"), message_name, desc_path)
        return df.select(
            decoded.getField("name").alias("name")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)
