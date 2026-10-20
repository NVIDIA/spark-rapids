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
import os

import pytest

from asserts import (
    assert_gpu_and_cpu_are_equal_collect,
    assert_gpu_and_cpu_error,
    assert_gpu_fallback_collect,
)
from data_gen import (
    ArrayGen, BinaryGen, BooleanGen, DoubleGen, FloatGen, IntegerGen, LongGen,
    SetValuesGen, StringGen, StructGen, idfn
)
from marks import allow_non_gpu, ignore_order
from protobuf_data_gen import call_protobuf_function, materialize_protobuf_data
from spark_session import (
    is_before_spark_340,
    is_spark_protobuf_available,
    with_cpu_session,
)
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

try:
    from google.protobuf import descriptor_pb2
except ModuleNotFoundError:
    descriptor_pb2 = None

pytestmark = [
    pytest.mark.premerge_ci_1,
    pytest.mark.skipif(
        not is_spark_protobuf_available(),
        reason="from_protobuf is unavailable"),
]


# Random data generation configurations for simple scalars
_random_scalar_test_configs = [
    # (test_id, data_gen_config)
    ("all_types", [
        ("b", BooleanGen()),
        ("i32", IntegerGen()),
        ("i64", LongGen()),
        ("f32", FloatGen()),
        ("f64", DoubleGen()),
        ("s", StringGen()),
    ]),
    ("integers_edge_cases", [
        ("b", BooleanGen()),
        ("i32", IntegerGen(
            min_val=-2147483648, max_val=2147483647,
            special_cases=[-2147483648, -1, 0, 1, 2147483647])),
        ("i64", LongGen(
            min_val=-9223372036854775808, max_val=9223372036854775807,
            special_cases=[-9223372036854775808, -1, 0, 1, 9223372036854775807])),
        ("f32", FloatGen()),
        ("f64", DoubleGen()),
        ("s", StringGen()),
    ]),
]


# ---------------------------------------------------------------------------
# Shared fixture and helpers to reduce per-test boilerplate
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def from_protobuf_fn():
    """Return the runtime-provided PySpark API."""
    from pyspark.sql.protobuf.functions import from_protobuf
    return from_protobuf


def _setup_protobuf_desc(local_tmp_path, desc_name, build_fn):
    """Build descriptor bytes and write them to the driver's local filesystem."""
    if descriptor_pb2 is None:
        pytest.skip("Python protobuf package is required to generate descriptors")
    desc_path = os.path.join(local_tmp_path, desc_name)
    desc_bytes = with_cpu_session(build_fn)
    with open(desc_path, "wb") as fp:
        fp.write(desc_bytes)
    return desc_path, desc_bytes


def _call_from_protobuf(from_protobuf_fn, col, message_name,
                        desc_path, desc_bytes, options=None):
    """Call from_protobuf using the right API variant."""
    return call_protobuf_function(
        from_protobuf_fn, col, message_name, desc_path, desc_bytes, options)


def _build_simple_descriptor_set_bytes(spark):
    """Build a simple scalar proto2 descriptor."""
    return _build_proto2_descriptor(spark, "simple.proto", [
        _msg("Simple", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("i64", 3, "INT64"),
            _field("f32", 4, "FLOAT"),
            _field("f64", 5, "DOUBLE"),
            _field("s", 6, "STRING"),
        ]),
    ])


def _new_proto2_file(_spark, name):
    """Create a descriptor without depending on Spark's private protobuf package."""
    return descriptor_pb2.FileDescriptorProto(
        name=name, package="test", syntax="proto2")


def _field(name, number, ftype, label="optional", default=None,
           type_name=None, packed=False, oneof_index=None):
    """Declarative field spec for `_build_proto2_descriptor`."""
    return {
        "name": name,
        "number": number,
        "type": ftype,
        "label": label,
        "default": default,
        "type_name": type_name,
        "packed": packed,
        "oneof_index": oneof_index,
    }


def _msg(name, fields, enums=None, oneofs=None):
    """Declarative message spec for `_build_proto2_descriptor`."""
    return {
        "name": name,
        "fields": fields,
        "enums": enums or [],
        "oneofs": oneofs or [],
    }


def _enum(name, values, allow_alias=False):
    """Declarative enum spec. Values are `(name, number)` tuples."""
    return {"name": name, "values": values, "allow_alias": allow_alias}


def _build_proto2_descriptor(
        spark, filename, messages, file_enums=None, java_string_check_utf8=False):
    """Build FileDescriptorSet bytes from declarative message and enum specs."""
    fd = _new_proto2_file(spark, filename)
    if java_string_check_utf8:
        fd.options.java_string_check_utf8 = True
    type_map = {
        name: getattr(descriptor_pb2.FieldDescriptorProto, f"TYPE_{name}")
        for name in [
            "BOOL", "INT32", "INT64", "UINT32", "UINT64", "SINT32", "SINT64",
            "FIXED32", "FIXED64", "SFIXED32", "SFIXED64", "FLOAT", "DOUBLE",
            "STRING", "BYTES", "MESSAGE", "ENUM",
        ]
    }
    label_map = {
        name: getattr(descriptor_pb2.FieldDescriptorProto, f"LABEL_{name.upper()}")
        for name in ["optional", "repeated", "required"]
    }

    def _default_literal(value):
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _build_enum(enum_spec):
        enum_proto = descriptor_pb2.EnumDescriptorProto(name=enum_spec["name"])
        if enum_spec.get("allow_alias", False):
            enum_proto.options.allow_alias = True
        for value_name, value_number in enum_spec["values"]:
            enum_proto.value.add(name=value_name, number=value_number)
        return enum_proto

    for enum_spec in file_enums or []:
        fd.enum_type.add().CopyFrom(_build_enum(enum_spec))

    for message_spec in messages:
        message_proto = fd.message_type.add(name=message_spec["name"])
        for oneof_name in message_spec.get("oneofs", []):
            message_proto.oneof_decl.add(name=oneof_name)
        for enum_spec in message_spec["enums"]:
            message_proto.enum_type.add().CopyFrom(_build_enum(enum_spec))
        for field_spec in message_spec["fields"]:
            field_proto = message_proto.field.add(
                name=field_spec["name"],
                number=field_spec["number"],
                label=label_map[field_spec["label"]],
                type=type_map[field_spec["type"]])
            if field_spec["type_name"] is not None:
                field_proto.type_name = field_spec["type_name"]
            if field_spec["default"] is not None:
                field_proto.default_value = _default_literal(field_spec["default"])
            if field_spec["packed"]:
                field_proto.options.packed = True
            if field_spec.get("oneof_index") is not None:
                field_proto.oneof_index = field_spec["oneof_index"]

    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.add().CopyFrom(fd)
    return fds.SerializeToString()


def _build_proto2_root_with_proto3_import(spark, reference_kind):
    root_file = _new_proto2_file(spark, f"proto3_{reference_kind}_root.proto")
    child_file = descriptor_pb2.FileDescriptorProto(
        name=f"proto3_{reference_kind}_child.proto", package="test", syntax="proto3")
    root_file.dependency.append(child_file.name)

    if reference_kind == "message":
        child_file.message_type.add(name="ImportedChild").field.add(
            name="value",
            number=1,
            label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
            type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
        type_name = ".test.ImportedChild"
        field_type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    else:
        child_enum = child_file.enum_type.add(name="ImportedStatus")
        child_enum.value.add(name="UNKNOWN", number=0)
        child_enum.value.add(name="OK", number=1)
        type_name = ".test.ImportedStatus"
        field_type = descriptor_pb2.FieldDescriptorProto.TYPE_ENUM

    root_file.message_type.add(name="Proto2Root").field.add(
        name="value",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=field_type,
        type_name=type_name)
    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.add().CopyFrom(child_file)
    fds.file.add().CopyFrom(root_file)
    return fds.SerializeToString()


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("reference_kind", ["message", "enum"], ids=idfn)
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_proto3_import_falls_back(
        local_tmp_path, from_protobuf_fn, reference_kind):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        f"proto3_{reference_kind}_import.desc",
        lambda spark: _build_proto2_root_with_proto3_import(spark, reference_kind))
    if reference_kind == "message":
        child = _encode_tag(1, 0) + _encode_varint(7)
        row = _encode_tag(1, 2) + _encode_varint(len(child)) + child
    else:
        row = _encode_tag(1, 0) + _encode_varint(1)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Proto2Root", desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _build_oneof_descriptor_set_bytes(spark):
    """Build a proto2 descriptor containing a oneof plus an independent sibling."""
    return _build_proto2_descriptor(spark, "oneof.proto", [
        _msg("Choice", [
            _field("int_value", 1, "INT32", oneof_index=0),
            _field("string_value", 2, "STRING", oneof_index=0),
            _field("sibling", 3, "INT32"),
        ], oneofs=["value"]),
    ])


def _build_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a message-local enum field."""
    return _build_proto2_descriptor(spark, "enum.proto", [
        _msg("WithEnum", [
            _field("color", 1, "ENUM", type_name=".test.WithEnum.Color"),
            _field("count", 2, "INT32"),
            _field("name", 3, "STRING"),
        ], enums=[
            _enum("Color", [("RED", 0), ("GREEN", 1), ("BLUE", 2)]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("enum_case", [
    "as_int",
    "unknown_as_int",
    "as_string",
    "unknown_as_string",
], ids=lambda x: x)
@ignore_order(local=True)
def test_from_protobuf_enum_cases(
        local_tmp_path, from_protobuf_fn, enum_case):
    """Parametrized enum decoding tests for int/string modes and unknown values."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    message_name = "test.WithEnum"
    logical_gen = StructGen([
        ("color", SetValuesGen(
            StringType(), ["RED", "GREEN", "BLUE", None])),
        ("count", IntegerGen()),
        ("name", StringGen()),
    ], nullable=False)

    if enum_case == "as_int":
        materialized, _ = materialize_protobuf_data(
            logical_gen, message_name, desc_path, desc_bytes, logical_rows=[
                {"color": "GREEN", "count": 42, "name": "test"},
                {"color": "RED", "count": 100, "name": None},
                {"color": None, "count": 200, "name": "hello"},
            ])
        rows = [(row[-1],) for row in materialized] + [(None,)]
        options = {"enums.as.ints": "true"}
        select_mode = "fields3"
    elif enum_case == "unknown_as_int":
        rows = [(_encode_varint_field(1, 999) + _encode_varint_field(2, 42),)]
        options = {"enums.as.ints": "true", "mode": "PERMISSIVE"}
        select_mode = "fields2"
    elif enum_case == "as_string":
        materialized, _ = materialize_protobuf_data(
            logical_gen, message_name, desc_path, desc_bytes, logical_rows=[
                {"color": "GREEN", "count": 42, "name": "test"},
                {"color": "RED", "count": 100, "name": None},
                {"color": "BLUE", "count": 200, "name": "hello"},
                {"color": None, "count": 300, "name": "world"},
            ])
        rows = [(row[-1],) for row in materialized] + [(None,)]
        options = None
        select_mode = "fields3"
    else:
        rows = [
            (_encode_varint_field(1, 1) + _encode_varint_field(2, 10),),
            (_encode_varint_field(1, 999) + _encode_varint_field(2, 20),),
            (_encode_varint_field(1, 2) + _encode_varint_field(2, 30),),
        ]
        options = {"mode": "PERMISSIVE"}
        select_mode = "decoded"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)
        if select_mode == "fields3":
            return df.select(
                decoded.getField("color").alias("color"),
                decoded.getField("count").alias("count"),
                decoded.getField("name").alias("name"))
        if select_mode == "fields2":
            return df.select(
                decoded.getField("color").alias("color"),
                decoded.getField("count").alias("count"))
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_out_of_order_enum_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "out_of_order_enum.proto", [
        _msg("OutOfOrderEnum", [
            _field("status", 1, "ENUM", type_name=".test.OutOfOrderEnum.Status"),
        ], enums=[
            _enum("Status", [("TEN", 10), ("ONE", 1), ("FIVE", 5)]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [None, {"enums.as.ints": "true"}],
                         ids=["string", "int"])
def test_from_protobuf_out_of_order_enum_values(
        local_tmp_path, from_protobuf_fn, options):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "out_of_order_enum.desc",
        _build_out_of_order_enum_descriptor_set_bytes)
    rows = [
        (_encode_tag(1, 0) + _encode_varint(value),)
        for value in (10, 1, 5)
    ]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.OutOfOrderEnum",
            desc_path, desc_bytes, options=options)
        return df.select(decoded.getField("status").alias("status"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_aliased_enum_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "aliased_enum.proto", [
        _msg("AliasedEnum", [
            _field("status", 1, "ENUM", type_name=".test.AliasedEnum.Status"),
        ], enums=[
            _enum("Status", [("UNKNOWN", 0), ("FIRST", 1), ("ALIAS", 1)],
                  allow_alias=True),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [None, {"enums.as.ints": "true"}],
                         ids=["string", "int"])
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_aliased_enum_falls_back(
        local_tmp_path, from_protobuf_fn, options):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "aliased_enum.desc", _build_aliased_enum_descriptor_set_bytes)
    row = _encode_tag(1, 0) + _encode_varint(1)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.AliasedEnum",
            desc_path, desc_bytes, options=options)
        return df.select(decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _build_repeated_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a repeated enum field."""
    return _build_proto2_descriptor(spark, "repeated_enum.proto", [
        _msg("WithRepeatedEnum", [
            _field("id", 1, "INT32"),
            _field("priority", 2, "ENUM", label="repeated",
                   type_name=".test.WithRepeatedEnum.Priority"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_enum_invalid_permissive(local_tmp_path, from_protobuf_fn):
    """Unknown proto2 enum occurrences are omitted from repeated fields."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_enum.desc", _build_repeated_enum_descriptor_set_bytes)
    message_name = "test.WithRepeatedEnum"

    row1 = (_encode_tag(1, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(0) +
            _encode_tag(2, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(2))
    row2 = (_encode_tag(1, 0) + _encode_varint(2) +
            _encode_tag(2, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(99))
    row3 = (_encode_tag(1, 0) + _encode_varint(3) +
            _encode_tag(2, 0) + _encode_varint(0))

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row1,), (row2,), (row3,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_enum_string_invalid_permissive_drops_unknown_values(
        local_tmp_path, from_protobuf_fn):
    """String enum mode drops unknown values without invalidating sibling fields."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_enum.desc", _build_repeated_enum_descriptor_set_bytes)
    message_name = "test.WithRepeatedEnum"

    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 0) + _encode_varint(0) +
                 _encode_tag(2, 0) + _encode_varint(2))
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 0) + _encode_varint(1) +
                   _encode_tag(2, 0) + _encode_varint(99))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("priority").alias("priority")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_required_field_descriptor_set_bytes(spark):
    """Build a descriptor for top-level proto2 required-field validation."""
    return _build_proto2_descriptor(spark, "required.proto", [
        _msg("WithRequired", [
            _field("id", 1, "INT64", label="required"),
            _field("name", 2, "STRING"),
            _field("count", 3, "INT32"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_present(
        local_tmp_path, from_protobuf_fn):
    """
    Test that required fields decode correctly when present.
    GPU should produce same results as CPU.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    logical_gen = StructGen([
        ("id", LongGen(nullable=False)),
        ("name", StringGen()),
        ("count", IntegerGen()),
    ], nullable=False)
    fixture_rows, _ = materialize_protobuf_data(
        logical_gen, "test.WithRequired", desc_path, desc_bytes, logical_rows=[
            {"id": 100, "name": "test", "count": 42},
            {"id": 200, "name": None, "count": None},
        ])
    test_data = [(row[-1],) for row in fixture_rows]

    def run_on_spark(spark):
        df = spark.createDataFrame(test_data, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithRequired",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_missing_failfast(local_tmp_path, from_protobuf_fn):
    """Required-field violations should fail consistently on CPU and GPU in FAILFAST mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    missing_required_row = _encode_tag(2, 2) + _encode_varint(4) + b"oops"

    def run_on_spark(spark):
        df = spark.createDataFrame([(missing_required_row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.alias("decoded")).collect()

    assert_gpu_and_cpu_error(run_on_spark, conf={}, error_message="Malformed")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_missing_permissive(local_tmp_path, from_protobuf_fn):
    """Required-field violations should null the whole row in PERMISSIVE mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    missing_required_row = _encode_tag(2, 2) + _encode_varint(5) + b"hello"

    def run_on_spark(spark):
        df = spark.createDataFrame([(missing_required_row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("mode", ["PERMISSIVE", "FAILFAST"], ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_visible_known_field_wrong_wire_type_matches_spark_error_policy(
        local_tmp_path, from_protobuf_fn, mode):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    row = (_encode_tag(2, 0) + _encode_varint(7) +
           _encode_tag(2, 5) + bytes([77, 0, 0, 0]) +
           _encode_tag(2, 0) + _encode_varint(42) +
           _encode_tag(6, 2) + _encode_varint(2) + b"ok")

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Simple", desc_path, desc_bytes,
            options={"mode": mode})
        result = df.select(decoded.alias("decoded"))
        return result.collect() if mode == "FAILFAST" else result

    if mode == "FAILFAST":
        assert_gpu_and_cpu_error(run_on_spark, conf={}, error_message="Malformed")
    else:
        assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_noncanonical_raw32_tag_and_length(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    overlong_i32_tag = bytes([0x90] + [0x80] * 8 + [0])
    overlong_zero = bytes([0x80] * 9 + [0])
    row = (overlong_i32_tag + _encode_varint(42) +
           _encode_tag(6, 2) + overlong_zero)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Simple", desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_proto2_invalid_utf8_surrogate_is_repaired_once(
        local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    invalid_utf8 = b"\xed\xa0\x80"
    row = _encode_tag(6, 2) + _encode_varint(len(invalid_utf8)) + invalid_utf8

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Simple", desc_path, desc_bytes)
        return df.select(decoded.getField("s").alias("s"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_strict_utf8_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(
        spark,
        "strict_utf8.proto",
        [_msg("StrictUtf8", [_field("value", 1, "STRING")])],
        java_string_check_utf8=True)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_proto2_strict_utf8_falls_back(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "strict_utf8.desc", _build_strict_utf8_descriptor_set_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame([(b"\x0a\x02ok",)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.StrictUtf8", desc_path, desc_bytes)
        return df.select(decoded.getField("value").alias("value"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _build_default_value_descriptor_set_bytes(spark):
    """Build a descriptor for proto2 scalar default-value behavior."""
    return _build_proto2_descriptor(spark, "defaults.proto", [
        _msg("WithDefaults", [
            _field("count", 1, "INT32", default=42),
            _field("name", 2, "STRING", default="unknown"),
            _field("flag", 3, "BOOL", default=True),
            _field("zero_count", 4, "INT32", default=0),
            _field("empty_name", 5, "STRING", default=""),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("default_case", [
    "field_present",
    "missing_fields",
    "string_default_only",
], ids=lambda x: x)
@ignore_order(local=True)
def test_from_protobuf_default_values_cases(local_tmp_path, from_protobuf_fn, default_case):
    """Parametrized tests for proto2 default-value behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "defaults.desc", _build_default_value_descriptor_set_bytes)
    message_name = "test.WithDefaults"

    if default_case == "field_present":
        rows = [(_encode_varint_field(1, 100) +
                 _encode_length_delimited_field(2, "test") +
                 _encode_varint_field(3, 0),)]
        select_mode = "all"
    elif default_case == "missing_fields":
        rows = [
            (b"",),
            (_encode_varint_field(1, 100),),
        ]
        select_mode = "all"
    else:
        rows = [(_encode_varint_field(1, 42) + _encode_varint_field(3, 1),)]
        select_mode = "name_only"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        if select_mode == "all":
            return df.select(
                decoded.getField("count").alias("count"),
                decoded.getField("name").alias("name"),
                decoded.getField("flag").alias("flag"),
                decoded.getField("zero_count").alias("zero_count"),
                decoded.getField("empty_name").alias("empty_name"))
        return df.select(decoded.getField("name").alias("name"))

    if default_case == "missing_fields":
        cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
        assert all(row["zero_count"] == 0 and row["empty_name"] == ""
                   for row in cpu_rows)
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_bytes_default_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "bytes_default.proto", [
        _msg("BytesDefault", [
            _field("payload", 1, "BYTES", default="abc"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_bytes_default_falls_back(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "bytes_default.desc", _build_bytes_default_descriptor_set_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame([(b"",)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.BytesDefault", desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


def _scalar_test_id(config):
    """Generate stable test ID using only the first element (test name)."""
    return config[0] if isinstance(config, tuple) else str(config)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("test_config", _random_scalar_test_configs, ids=_scalar_test_id)
@ignore_order(local=True)
def test_from_protobuf_random_scalars(
        local_tmp_path, from_protobuf_fn, test_config):
    """
    Parametrized test for from_protobuf with randomly generated scalar data.
    Covers: all types, integer edge cases, float edge cases, large datasets.
    """
    _, field_configs = test_config

    logical_gen = StructGen(field_configs, nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.Simple", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)

        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.Simple", desc_path, desc_bytes)

        # Select all decoded fields
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
            decoded.getField("i64").alias("i64"),
            decoded.getField("f32").alias("f32"),
            decoded.getField("f64").alias("f64"),
            decoded.getField("s").alias("s"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_all_scalars_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "all_scalars.proto", [
        _msg("AllScalars", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("i64", 3, "INT64"),
            _field("f32", 4, "FLOAT"),
            _field("f64", 5, "DOUBLE"),
            _field("s", 6, "STRING"),
            _field("si32", 7, "SINT32"),
            _field("si64", 8, "SINT64"),
            _field("fx32", 9, "FIXED32"),
            _field("fx64", 10, "FIXED64"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_scalar_types(
        local_tmp_path, from_protobuf_fn):
    """Decode all scalar wire encodings together in one message."""
    logical_gen = StructGen([
        ("b", BooleanGen()),
        ("i32", IntegerGen()),
        ("i64", LongGen()),
        ("f32", FloatGen()),
        ("f64", DoubleGen()),
        ("s", StringGen()),
        ("si32", IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647])),
        ("si64", LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807])),
        ("fx32", IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648])),
        ("fx64", LongGen(special_cases=[0, 1, -1])),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "all_scalars.desc", _build_all_scalars_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.AllScalars", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.AllScalars", desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
            decoded.getField("i64").alias("i64"),
            decoded.getField("f32").alias("f32"),
            decoded.getField("f64").alias("f64"),
            decoded.getField("s").alias("s"),
            decoded.getField("si32").alias("si32"),
            decoded.getField("si64").alias("si64"),
            decoded.getField("fx32").alias("fx32"),
            decoded.getField("fx64").alias("fx64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_scalar_bytes_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "scalar_bytes.proto", [
        _msg("ScalarBytes", [_field("payload", 1, "BYTES")]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_scalar_bytes(
        local_tmp_path, from_protobuf_fn):
    """Decode optional bytes scalar field, including empty bytes."""
    logical_gen = StructGen([
        ("payload", BinaryGen(min_length=0, max_length=16)),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "scalar_bytes.desc", _build_scalar_bytes_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.ScalarBytes", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.ScalarBytes", desc_path, desc_bytes)
        return df.select(decoded.getField("payload").alias("payload"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_duplicate_fields(local_tmp_path, from_protobuf_fn):
    """Duplicate non-repeated field should follow protobuf last-one-wins semantics."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # i32 (field 2) appears twice in row0 and three times in row1.
    test_data_row0 = bytes([0x10, 0x01, 0x10, 0x2A])
    test_data_row1 = bytes([0x10, 0x03, 0x10, 0x04, 0x10, 0x05])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,), (None,)],
            schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.getField("i32").alias("i32"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_int_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "repeated_int.proto", [
        _msg("WithRepeatedInt", [
            _field("id", 1, "INT32"),
            _field("values", 2, "INT32", label="repeated"),
        ]),
    ])


def _build_repeated_string_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "repeated_string.proto", [
        _msg("WithRepeatedString", [
            _field("name", 1, "STRING"),
            _field("tags", 2, "STRING", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_int32(
        local_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated int32 field (ArrayType of integers).
    """
    logical_gen = StructGen([
        ("id", IntegerGen()),
        ("values", ArrayGen(
            IntegerGen(nullable=False), min_length=0, max_length=10,
            nullable=False)),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithRepeatedInt", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithRepeatedInt",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("values").alias("values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_string(
        local_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated string field (ArrayType of strings).
    """
    logical_gen = StructGen([
        ("name", StringGen()),
        ("tags", ArrayGen(
            StringGen(nullable=False), min_length=0, max_length=5,
            nullable=False)),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_string.desc",
        _build_repeated_string_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithRepeatedString", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithRepeatedString",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("name").alias("name"),
            decoded.getField("tags").alias("tags"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_packed_repeated_descriptor_set_bytes(spark):
    """Build a descriptor for packed repeated numeric and bool fields."""
    return _build_proto2_descriptor(spark, "packed_repeated.proto", [
        _msg("WithPackedRepeated", [
            _field("id", 1, "INT32"),
            _field("int_values", 2, "INT32", label="repeated", packed=True),
            _field("double_values", 3, "DOUBLE", label="repeated", packed=True),
            _field("bool_values", 4, "BOOL", label="repeated", packed=True),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_packed_repeated(
        local_tmp_path, from_protobuf_fn):
    """
    Test packed repeated fields (int, double, bool) using DataGen.
    """
    logical_gen = StructGen([
        ("id", IntegerGen()),
        ("int_values", ArrayGen(
            IntegerGen(nullable=False), min_length=0, max_length=10,
            nullable=False)),
        ("double_values", ArrayGen(
            DoubleGen(nullable=False), min_length=0, max_length=5,
            nullable=False)),
        ("bool_values", ArrayGen(
            BooleanGen(nullable=False), min_length=0, max_length=5,
            nullable=False)),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "packed.desc", _build_packed_repeated_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithPackedRepeated", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithPackedRepeated",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("int_values").alias("int_values"),
            decoded.getField("double_values").alias("double_values"),
            decoded.getField("bool_values").alias("bool_values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_all_types_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "repeated_all_types.proto", [
        _msg("WithRepeatedAllTypes", [
            _field("id", 1, "INT32"),
            _field("long_values", 2, "INT64", label="repeated"),
            _field("float_values", 3, "FLOAT", label="repeated"),
            _field("double_values", 4, "DOUBLE", label="repeated"),
            _field("bool_values", 5, "BOOL", label="repeated"),
            _field("bytes_values", 6, "BYTES", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_all_types(
        local_tmp_path, from_protobuf_fn):
    """Test repeated fields of various types (int64, float, double, bool, bytes) using DataGen."""
    logical_gen = StructGen([
        ("id", IntegerGen()),
        ("long_values", ArrayGen(
            LongGen(nullable=False), min_length=0, max_length=5, nullable=False)),
        ("float_values", ArrayGen(
            FloatGen(nullable=False), min_length=0, max_length=5, nullable=False)),
        ("double_values", ArrayGen(
            DoubleGen(nullable=False), min_length=0, max_length=5, nullable=False)),
        ("bool_values", ArrayGen(
            BooleanGen(nullable=False), min_length=0, max_length=5, nullable=False)),
        ("bytes_values", ArrayGen(
            BinaryGen(nullable=False), min_length=0, max_length=5, nullable=False)),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_all.desc",
        _build_repeated_all_types_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithRepeatedAllTypes", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithRepeatedAllTypes",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("long_values").alias("long_values"),
            decoded.getField("float_values").alias("float_values"),
            decoded.getField("double_values").alias("double_values"),
            decoded.getField("bool_values").alias("bool_values"),
            decoded.getField("bytes_values").alias("bytes_values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_large_repeated_array(
        local_tmp_path, from_protobuf_fn):
    """
    Test decoding large repeated field (500-1000 elements).
    Stress test for array handling.
    """
    logical_gen = StructGen([
        ("id", IntegerGen()),
        ("values", ArrayGen(
            IntegerGen(nullable=False), min_length=500, max_length=1000,
            nullable=False)),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "large_repeated_int.desc",
        _build_repeated_int_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithRepeatedInt", desc_path, desc_bytes, length=128)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithRepeatedInt",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            f.size(decoded.getField("values")).alias("array_size"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_signed_integers_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "signed_integers.proto", [
        _msg("WithSignedInts", [
            _field("si32", 1, "SINT32"),
            _field("si64", 2, "SINT64"),
            _field("sf32", 3, "SFIXED32"),
            _field("sf64", 4, "SFIXED64"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_signed_integers(
        local_tmp_path, from_protobuf_fn):
    """Decode signed integers using zigzag and fixed-width encodings."""
    logical_gen = StructGen([
        ("si32", IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647])),
        ("si64", LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807])),
        ("sf32", IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648])),
        ("sf64", LongGen(special_cases=[0, 1, -1])),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "signed.desc", _build_signed_integers_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithSignedInts", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithSignedInts",
            desc_path, desc_bytes)

        return df.select(
            decoded.getField("si32").alias("si32"),
            decoded.getField("si64").alias("si64"),
            decoded.getField("sf32").alias("sf32"),
            decoded.getField("sf64").alias("sf64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_fixed_integers_descriptor_set_bytes(spark):
    return _build_proto2_descriptor(spark, "fixed_integers.proto", [
        _msg("WithFixedInts", [
            _field("fx32", 1, "FIXED32"),
            _field("fx64", 2, "FIXED64"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_fixed_integers(
        local_tmp_path, from_protobuf_fn):
    """
    Test decoding fixed-width unsigned integer types.
    """
    logical_gen = StructGen([
        ("fx32", IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648])),
        ("fx64", LongGen(special_cases=[0, 1, -1])),
    ], nullable=False)
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "fixed.desc", _build_fixed_integers_descriptor_set_bytes)
    fixture_rows, fixture_schema = materialize_protobuf_data(
        logical_gen, "test.WithFixedInts", desc_path, desc_bytes)

    def run_on_spark(spark):
        df = spark.createDataFrame(fixture_rows, fixture_schema)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.WithFixedInts",
            desc_path, desc_bytes)
        return df.select(
            decoded.getField("fx32").alias("fx32"),
            decoded.getField("fx64").alias("fx64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_many_repeated_fields_descriptor_set_bytes(spark, field_count):
    return _build_proto2_descriptor(spark, "many_repeated.proto", [
        _msg("ManyRepeated", [
            _field(f"values_{number}", number, "INT32", label="repeated")
            for number in range(1, field_count + 1)
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("select_whole_message", [True, False], ids=["whole", "pruned"])
@allow_non_gpu("ProtobufDataToCatalyst")
@ignore_order(local=True)
def test_from_protobuf_repeated_field_capacity_falls_back(
        local_tmp_path, from_protobuf_fn, select_whole_message):
    field_count = 33
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        "many_repeated_33.desc",
        lambda spark: _build_many_repeated_fields_descriptor_set_bytes(spark, field_count))
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(field_count, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.ManyRepeated", desc_path, desc_bytes)
        if select_whole_message:
            return df.select("idx", decoded.alias("decoded"))
        return df.select("idx", decoded.getField("values_1").alias("values_1"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_field_capacity_boundary(
        local_tmp_path, from_protobuf_fn):
    field_count = 32
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path,
        f"many_repeated_{field_count}.desc",
        lambda spark: _build_many_repeated_fields_descriptor_set_bytes(spark, field_count))
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(field_count, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.ManyRepeated", desc_path, desc_bytes)
        return df.select("idx", decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _encode_varint(value):
    """Encode a non-negative integer as a protobuf varint (for hand-crafting test bytes)."""
    if value < 0:
        raise ValueError("varint value must be non-negative")
    out = bytearray()
    v = int(value)
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _encode_tag(field_number, wire_type):
    return _encode_varint((field_number << 3) | wire_type)


def _encode_varint_field(field_number, value):
    return _encode_tag(field_number, 0) + _encode_varint(value)


def _encode_length_delimited_field(field_number, value):
    data = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    return _encode_tag(field_number, 2) + _encode_varint(len(data)) + data


def _encode_simple_message(i32_value, s_value):
    return (_encode_varint_field(1, i32_value & 0xFFFFFFFFFFFFFFFF) +
            _encode_length_delimited_field(2, s_value))


# This keeps the baseline API coverage independent of protobuf runtime versions.
_simple_desc_bytes = bytes.fromhex(
    "0a360a0c73696d706c652e70726f746f12047465737422200a0653696d706c65"
    "120b0a0369333218012001280512090a0173180220012809")


# Keep these cross-file contracts independent of Spark's shaded descriptor runtime.
_protobuf_semantics_desc_bytes = bytes.fromhex(
    "0a3f0a0a6c6561662e70726f746f12047465737422230a12496d706f72746564"
    "4772616e646368696c64120d0a0576616c7565180120012805620670726f746f"
    "320ad5010a0b6368696c642e70726f746f1204746573741a0a6c6561662e7072"
    "6f746f223d0a0d496d706f727465644368696c64122c0a0a6772616e64636869"
    "6c6418012001280b32182e746573742e496d706f727465644772616e64636869"
    "6c6422380a0c52657065617465644c65616612280a0676616c75657318012003"
    "280b32182e746573742e496d706f727465644772616e646368696c6422330a0d"
    "52657065617465644368696c6412220a066c656176657318012003280b32122e"
    "746573742e52657065617465644c656166620670726f746f320ad3020a0a726f"
    "6f742e70726f746f1204746573741a0b6368696c642e70726f746f22320a0c49"
    "6d706f72746564526f6f7412220a056368696c6418012001280b32132e746573"
    "742e496d706f727465644368696c64229c010a0c556e7369676e6564526f6f74"
    "121c0a0673746174757318012002280e320c2e746573742e5374617475731214"
    "0a0c72657175697265645f75363418022002280412140a0c6f7074696f6e616c"
    "5f75333218032001280d12140a0c72657065617465645f75333218042003280d"
    "12140a0c72657065617465645f75363418052003280412160a0b64656661756c"
    "745f7536341806200128043a013022350a0c5265706561746564526f6f741225"
    "0a086368696c6472656e18012003280b32132e746573742e5265706561746564"
    "4368696c642a200a06537461747573120b0a07554e4b4e4f574e100012090a05"
    "52454144591002620670726f746f32")

_oneof_smoke_desc_bytes = bytes.fromhex(
    "0a5e0a116f6e656f665f736d6f6b652e70726f746f120474657374223b0a0643"
    "686f696365120b0a01691801200128054800120b0a0173180220012809480012"
    "0d0a056f7468657218032001280542080a0663686f696365620670726f746f32")


@pytest.fixture
def simple_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "simple.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_simple_desc_bytes)
    return desc_path, _simple_desc_bytes


@pytest.fixture
def protobuf_semantics_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "protobuf_semantics.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_protobuf_semantics_desc_bytes)
    return desc_path, _protobuf_semantics_desc_bytes


@pytest.fixture
def oneof_smoke_desc(local_tmp_path):
    desc_path = os.path.join(local_tmp_path, "oneof_smoke.desc")
    with open(desc_path, "wb") as fp:
        fp.write(_oneof_smoke_desc_bytes)
    return desc_path, _oneof_smoke_desc_bytes


@pytest.mark.skipif(is_before_spark_340(), reason="descriptor compatibility shims start at Spark 3.4")
@pytest.mark.parametrize("check_utf8", [False, True])
def test_protobuf_descriptor_compat(local_tmp_path, from_protobuf_fn, check_utf8):
    # proto2 test.Compat: optional int32 id = 1 [default = 7]; Status FIRST = ALIAS = 0;
    # optional Status first = 2 [default = FIRST]; optional Status alias = 3 [default = ALIAS].
    # repeated int32 items = 4; required string required_name = 5;
    # oneof choice { int32 selected = 6; }
    # message Nested { optional string value = 1; } optional Nested nested = 7;
    # message GroupContainer { optional group Legacy = 1 { optional string value = 1; } }
    # The final byte sets the file's java_string_check_utf8 option.
    desc_bytes = bytes.fromhex(
        "0ad6030a0c636f6d7061742e70726f746f12047465737422d3020a06436f6d70617412110a0269641801200128"
        "053a01375202696412300a05666972737418022001280e32132e746573742e436f6d7061742e5374617475733a"
        "0546495253545205666972737412300a05616c69617318032001280e32132e746573742e436f6d7061742e5374"
        "617475733a05414c4941535205616c69617312140a056974656d7318042003280552056974656d7312230a0d72"
        "657175697265645f6e616d65180520022809520c72657175697265644e616d65121c0a0873656c656374656418"
        "06200128054800520873656c6563746564122b0a066e657374656418072001280b32132e746573742e436f6d70"
        "61742e4e657374656452066e65737465641a1e0a064e657374656412140a0576616c7565180120012809520576"
        "616c756522220a0653746174757312090a054649525354100012090a05414c49415310001a02100142080a0663"
        "686f69636522650a0e47726f7570436f6e7461696e657212330a066c656761637918012001280a321b2e746573"
        "742e47726f7570436f6e7461696e65722e4c656761637952066c65676163791a1e0a064c656761637912140a05"
        "76616c7565180120012809520576616c75654203d801" + ("01" if check_utf8 else "00"))
    # Add a proto3 file: message Presence { optional int32 optional_value = 1;
    # oneof choice { int32 selected = 2; } int32 plain = 3; }
    desc_bytes += bytes.fromhex(
        "0a87010a0e70726573656e63652e70726f746f12047465737422670a0850726573656e6365121b0a0e6f7074"
        "696f6e616c5f76616c7565180120012805480188010112120a0873656c65637465641802200128054800120d"
        "0a05706c61696e18032001280542080a0663686f69636542110a0f5f6f7074696f6e616c5f76616c75656206"
        "70726f746f33")
    desc_path = local_tmp_path + "/compat.desc"
    with open(desc_path, "wb") as fp:
        fp.write(desc_bytes)

    def check(spark):
        decoded = call_protobuf_function(
            from_protobuf_fn, f.col("bin"), "test.Compat", desc_path, desc_bytes,
            options={"mode": "FAILFAST"})
        # Match the plugin's planning phase: replace RuntimeReplaceable, but avoid constant folding.
        source = spark.createDataFrame([(bytearray(),)], "bin binary")
        expr = source.select(decoded.alias("d"))._jdf.queryExecution() \
            .optimizedPlan().projectList().apply(0).child()
        assert expr.getClass().getName() == "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst"
        cls = spark._jvm.com.nvidia.spark.rapids.ShimReflectionUtils.loadClass(
            "com.nvidia.spark.rapids.shims.SparkProtobufCompat$")
        compat = cls.getField("MODULE$").get(None)
        extracted = compat.extractExprInfo(expr)
        assert extracted.isRight(), str(extracted)
        info = extracted.toOption().get()
        assert info.messageName() == "test.Compat"
        assert info.options().apply("mode") == "FAILFAST"
        resolved = compat.resolveMessageDescriptor(info)
        assert resolved.isRight(), str(resolved)
        descriptor = resolved.toOption().get()
        assert descriptor.syntax() == "PROTO2"
        assert descriptor.javaStringCheckUtf8() == check_utf8
        assert descriptor.findField("missing").isEmpty()
        for name, number, proto_type, default in [
                ("id", 1, "INT32", 7), ("first", 2, "ENUM", "FIRST"),
                ("alias", 3, "ENUM", "ALIAS")]:
            field = descriptor.findField(name).get()
            assert (field.name(), field.fieldNumber(), field.protoTypeName()) == \
                (name, number, proto_type)
            assert not field.isRepeated() and not field.isRequired() and not field.isInOneof()
            assert field.messageDescriptor().isEmpty()
            result = field.explicitDefaultValue()
            assert result.isRight(), str(result)
            value = result.toOption().get().get()
            if proto_type == "ENUM":
                assert field.referencedTypeSyntax().get() == "PROTO2"
                assert (value.number(), value.name()) == (0, default)
                values = field.enumMetadata().get().values()
                assert [(values.apply(i).number(), values.apply(i).name())
                        for i in range(values.size())] == [(0, "FIRST"), (0, "ALIAS")]
            else:
                assert field.enumMetadata().isEmpty()
                assert field.referencedTypeSyntax().isEmpty()
                assert value.value() == default
        for name, number, proto_type, labels in [
                ("items", 4, "INT32", (True, False, False)),
                ("required_name", 5, "STRING", (False, True, False)),
                ("selected", 6, "INT32", (False, False, True)),
                ("nested", 7, "MESSAGE", (False, False, False))]:
            field = descriptor.findField(name).get()
            assert (field.fieldNumber(), field.protoTypeName()) == (number, proto_type)
            assert (field.isRepeated(), field.isRequired(), field.isInOneof()) == labels
            default = field.explicitDefaultValue()
            assert default.isRight() and default.toOption().get().isEmpty()
            assert field.enumMetadata().isEmpty()
            if proto_type == "MESSAGE":
                assert field.referencedTypeSyntax().get() == "PROTO2"
                nested = field.messageDescriptor().get()
                assert nested.syntax() == "PROTO2"
                assert nested.javaStringCheckUtf8() == check_utf8
                nested_field = nested.findField("value").get()
                assert (nested_field.fieldNumber(), nested_field.protoTypeName()) == (1, "STRING")
                assert nested.findField("missing").isEmpty()
            else:
                assert field.messageDescriptor().isEmpty()
                assert field.referencedTypeSyntax().isEmpty()
        # Inspect GROUP metadata without requiring Spark SQL to support group decoding.
        group_info = info.copy("test.GroupContainer", info.descriptorSource(), info.options())
        group_result = compat.resolveMessageDescriptor(group_info)
        assert group_result.isRight(), str(group_result)
        group = group_result.toOption().get().findField("legacy").get()
        assert (group.fieldNumber(), group.protoTypeName()) == (1, "GROUP")
        assert group.referencedTypeSyntax().get() == "PROTO2"
        nested = group.messageDescriptor().get()
        assert nested.syntax() == "PROTO2"
        assert nested.javaStringCheckUtf8() == check_utf8
        value = nested.findField("value").get()
        assert (value.fieldNumber(), value.protoTypeName()) == (1, "STRING")
        assert nested.findField("missing").isEmpty()
        missing = info.copy("test.Missing", info.descriptorSource(), info.options())
        assert compat.resolveMessageDescriptor(missing).isLeft()
        presence_info = info.copy("test.Presence", info.descriptorSource(), info.options())
        presence_result = compat.resolveMessageDescriptor(presence_info)
        assert presence_result.isRight(), str(presence_result)
        presence = presence_result.toOption().get()
        assert presence.syntax() == "PROTO3"
        for name, in_oneof in [("optional_value", False), ("selected", True), ("plain", False)]:
            assert presence.findField(name).get().isInOneof() == in_oneof, name

    # This exercises the CPU-side metadata API, not execution of a GPU expression.
    with_cpu_session(check)


def _make_smoke_df(spark):
    values = [(1, "a"), (-2, "bb"), (0, ""), (12345, "hello")]
    encoded = [(_encode_simple_message(i, s),) for i, s in values]
    return spark.createDataFrame(encoded, ["bin"])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_smoke_path_api(simple_desc, from_protobuf_fn):
    desc_path, _ = simple_desc

    def run_on_spark(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple", desc_path).alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_disabled_by_expression_config(simple_desc, from_protobuf_fn):
    desc_path, _ = simple_desc

    def run_on_spark(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple", desc_path).alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst",
        conf={"spark.rapids.sql.expression.ProtobufDataToCatalyst": "false"})


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_unsigned_boundaries(
        protobuf_semantics_desc, from_protobuf_fn):
    desc_path, desc_bytes = protobuf_semantics_desc
    row = (
        _encode_tag(1, 0) + _encode_varint(2) +
        _encode_tag(2, 0) + _encode_varint((1 << 64) - 1) +
        _encode_tag(3, 0) + _encode_varint(1 << 31) +
        _encode_tag(4, 0) + _encode_varint(0) +
        _encode_tag(4, 0) + _encode_varint((1 << 32) - 1) +
        _encode_tag(5, 0) + _encode_varint(1 << 63) +
        _encode_tag(5, 0) + _encode_varint((1 << 64) - 1))

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.UnsignedRoot",
            desc_path,
            desc_bytes)
        return spark.createDataFrame([(row,)], ["bin"]).select(decoded.alias("decoded"))

    cpu_rows = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert cpu_rows[0]["decoded"].asDict() == {
        "status": "READY",
        "required_u64": -1,
        "optional_u32": -(1 << 31),
        "repeated_u32": [0, -1],
        "repeated_u64": [-(1 << 63), -1],
        "default_u64": 0,
    }
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_smoke_oneof_falls_back(oneof_smoke_desc, from_protobuf_fn):
    desc_path, desc_bytes = oneof_smoke_desc
    rows = [
        (_encode_tag(1, 0) + _encode_varint(7) +
         _encode_tag(3, 0) + _encode_varint(9)),
        (_encode_tag(2, 2) + _encode_varint(1) + b"x" +
         _encode_tag(3, 0) + _encode_varint(10)),
    ]

    def run_on_spark(spark):
        decoded = _call_from_protobuf(
            from_protobuf_fn,
            f.col("bin"),
            "test.Choice",
            desc_path,
            desc_bytes)
        return spark.createDataFrame([(row,) for row in rows], ["bin"]).select(
            decoded.alias("decoded"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_smoke_binary_descriptor_api(simple_desc, from_protobuf_fn):
    if "binaryDescriptorSet" not in inspect.signature(from_protobuf_fn).parameters:
        pytest.skip("binaryDescriptorSet kwarg is Spark 3.5+ only")
    _, desc_bytes = simple_desc

    def run_on_spark(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(
                f.col("bin"),
                "test.Simple",
                binaryDescriptorSet=bytearray(desc_bytes)).alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# Regression tests for known bugs found by code review
# ===========================================================================

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_oneof_member_falls_back(local_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "oneof.desc", _build_oneof_descriptor_set_bytes)
    message_name = "test.Choice"
    row = (_encode_tag(1, 0) + _encode_varint(7) +
           _encode_tag(2, 2) + _encode_varint(1) + b"x" +
           _encode_tag(3, 0) + _encode_varint(9))

    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row), (1, None)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            "idx",
            decoded.getField("int_value").alias("int_value"),
            decoded.getField("sibling").alias("sibling"))

    assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


# ---------------------------------------------------------------------------
# Bug 1: BOOL8 truncation — non-canonical bool varint values >= 256
#
# Protobuf spec: bool is a varint; any non-zero value means true.
# CPU decoder (protobuf-java): CodedInputStream.readBool() = readRawVarint64() != 0  →  true
# GPU decoder: extract_varint_kernel writes static_cast<uint8_t>(v).
#   For v = 256, static_cast<uint8_t>(256) == 0  →  false.  BUG.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_scalar(local_tmp_path, from_protobuf_fn):
    """Regression test: scalar bool encoded as non-canonical varint (e.g. 256) must decode as true.

    Protobuf allows any non-zero varint for bool true. The GPU decoder previously
    truncated to uint8_t, causing values >= 256 to wrap to 0 (false).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "simple_bool_bug.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # varint(256) = 0x80 0x02, varint(512) = 0x80 0x04 — valid non-canonical bool true
    row_bool_256 = _encode_tag(1, 0) + _encode_varint(256) + \
                   _encode_tag(2, 0) + _encode_varint(99)

    # Control row: canonical bool true (varint 1) — should work on both
    row_bool_1 = _encode_tag(1, 0) + _encode_varint(1) + \
                 _encode_tag(2, 0) + _encode_varint(100)

    # Another non-canonical value: varint(512)
    row_bool_512 = _encode_tag(1, 0) + _encode_varint(512) + \
                   _encode_tag(2, 0) + _encode_varint(101)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_bool_256,), (row_bool_1,), (row_bool_512,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_bool_descriptor_set_bytes(spark):
    """Build a descriptor for an optional id plus repeated bool flags."""
    return _build_proto2_descriptor(spark, "repeated_bool.proto", [
        _msg("WithRepeatedBool", [
            _field("id", 1, "INT32"),
            _field("flags", 2, "BOOL", label="repeated"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_repeated(local_tmp_path, from_protobuf_fn):
    """Regression test: repeated bool with non-canonical varint values must all decode as true.

    Same uint8_t truncation issue as the scalar case, exercised with repeated fields.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "repeated_bool_bug.desc", _build_repeated_bool_descriptor_set_bytes)
    message_name = "test.WithRepeatedBool"

    # Repeated bool field 2 (wire type 0 = varint), unpacked.
    # Three elements: varint(256), varint(1), varint(512) — all should decode as true.
    row = (_encode_tag(1, 0) + _encode_varint(42) +
           _encode_tag(2, 0) + _encode_varint(256) +
           _encode_tag(2, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(512))

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("flags").alias("flags"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# FAILFAST mode tests
# ===========================================================================

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [
    {"mode": "DROPMALFORMED"},
    {"mode": "DROPMALFORMED", "emit.default.values": "true"},
], ids=["dropmalformed", "dropmalformed-with-unsupported-option"])
@pytest.mark.parametrize("malformed", [False, True], ids=["valid", "malformed"])
@allow_non_gpu("ProtobufDataToCatalyst")
def test_from_protobuf_dropmalformed_mode_falls_back(
        local_tmp_path, from_protobuf_fn, options, malformed):
    """Spark rejects DROPMALFORMED only when a malformed row reaches its error handler."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "dropmalformed.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"
    row = bytes([0x08, 0x80]) if malformed else _encode_tag(2, 0) + _encode_varint(42)

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)
        return df.select(decoded.alias("decoded"))

    if malformed:
        assert_gpu_and_cpu_error(
            lambda spark: run_on_spark(spark).collect(), {}, "DROPMALFORMED")
    else:
        assert_gpu_fallback_collect(run_on_spark, "ProtobufDataToCatalyst")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_failfast_malformed_data(local_tmp_path, from_protobuf_fn):
    """FAILFAST mode should throw on malformed protobuf data (both CPU and GPU)."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "failfast.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # Craft a valid row and a malformed row (truncated varint with continuation bit)
    valid_row = _encode_tag(1, 0) + _encode_varint(1) + \
                _encode_tag(2, 0) + _encode_varint(42)
    malformed_row = bytes([0x08, 0x80])  # field 1, varint, but only continuation byte -- no end

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(valid_row,), (malformed_row,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "FAILFAST"})
        # Must call .collect() so the exception surfaces inside with_*_session
        return df.select(decoded.getField("b").alias("b")).collect()

    assert_gpu_and_cpu_error(run_on_spark, {}, "Malformed")


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_permissive_malformed_returns_null(local_tmp_path, from_protobuf_fn):
    """PERMISSIVE mode should return null for malformed rows, not throw.

    Note: Spark's from_protobuf defaults to FAILFAST (unlike JSON/CSV which
    default to PERMISSIVE), so mode must be set explicitly.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "permissive.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    valid_row = _encode_tag(2, 0) + _encode_varint(99)
    malformed_row = bytes([0x08, 0x80])  # truncated varint

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(valid_row,), (malformed_row,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_null_input(local_tmp_path, from_protobuf_fn):
    """All rows in the input binary column are null (not empty bytes, actual nulls).
    GPU should produce all-null struct rows matching CPU behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        local_tmp_path, "allnull.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(None,), (None,), (None,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("i32").alias("i32"),
            decoded.getField("s").alias("s"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)
