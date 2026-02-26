#!/bin/bash
# Convenience script: compile nested_proto .proto files into a descriptor set.
#
# Usage:
#   ./gen_nested_proto_data.sh
#
# The generated .desc file is checked into the repository and used by
# integration tests in protobuf_test.py.  Re-run this script whenever
# the .proto definitions under nested_proto/ change.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROTO_DIR="${SCRIPT_DIR}/nested_proto"
OUTPUT_DIR="${SCRIPT_DIR}/nested_proto/generated"

echo "=== Protobuf Descriptor Compiler ==="
echo "Proto dir: ${PROTO_DIR}"
echo ""

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Compile proto files into a descriptor set (includes all imports)
DESC_FILE="${OUTPUT_DIR}/main_log.desc"
echo "Compiling proto files..."
protoc \
    --descriptor_set_out="${DESC_FILE}" \
    --include_imports \
    -I"${PROTO_DIR}" \
    "${PROTO_DIR}/main_log.proto"

echo "Generated: ${DESC_FILE}"
echo "=== Done ==="
