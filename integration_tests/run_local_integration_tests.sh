#!/bin/bash
#
# Local Integration Tests Script for RAPIDS Spark Plugin
# Based on jenkins/spark-tests.sh
#
# Usage:
#   ./run_local_integration_tests.sh [TEST_MODE]
#
# TEST_MODE options:
#   DEFAULT             - Run all basic tests (default)
#   DELTA_LAKE_ONLY     - Run only Delta Lake tests
#   ICEBERG_ONLY        - Run only Iceberg tests (default catalog)
#   ICEBERG_REST_ONLY   - Run only Iceberg REST catalog tests
#   ICEBERG_S3TABLES    - Run only Iceberg S3 Tables tests
#   AVRO_ONLY          - Run only Avro tests
#   ALL                - Run all tests including Delta Lake and Iceberg
#

# Don't exit on error - we want to run all tests even if some fail
set +e

# ==================== Configuration ====================
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RAPIDS_ROOT="${RAPIDS_ROOT:-$SCRIPT_DIR/../}"
INTEGRATION_TESTS_HOME="$RAPIDS_ROOT/integration_tests"

# Spark Configuration
export SPARK_HOME="${SPARK_HOME:-/root/spark-3.5.3-bin-hadoop3}"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export AWS_REGION="${AWS_REGION:-us-west-2}"

# Detect Spark version
SPARK_VER=$(basename $(ls $SPARK_HOME/jars/spark-core_*.jar | head -1) .jar | awk -F'-' '{print $NF}')
SCALA_BINARY_VER=$(basename $(ls $SPARK_HOME/jars/spark-core_*.jar | head -1) .jar | awk -F'_' '{print $2}' | cut -d'-' -f1)

echo "========================================="
echo "RAPIDS Spark Local Integration Tests"
echo "========================================="
echo "Spark Home: $SPARK_HOME"
echo "Spark Version: $SPARK_VER"
echo "Scala Version: $SCALA_BINARY_VER"
echo "========================================="

# ==================== CRITICAL: Set PYTHONPATH ====================
# This is the key fix for "ModuleNotFoundError: No module named 'pyspark'"
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip" | head -1)
export PYTHONPATH="$PYTHONPATH:$SPARK_HOME/python:$PY4J_FILE"

echo "PYTHONPATH: $PYTHONPATH"
echo "========================================="

# Verify Python can import pyspark
if ! python -c "import pyspark" 2>/dev/null; then
    echo "ERROR: Cannot import pyspark. Please check SPARK_HOME and PYTHONPATH."
    exit 1
fi
echo "✓ PySpark module is accessible"
echo "========================================="

# ==================== Check if tests should be skipped ====================
if [[ "${SKIP_TESTS}" == "true" ]]; then
    echo "Tests are skipped (SKIP_TESTS=true)"
    exit 0
fi

# ==================== Test Configuration ====================
cd "$INTEGRATION_TESTS_HOME"

export TEST_TYPE="${TEST_TYPE:-developer}"
export TEST_PARALLEL="${TEST_PARALLEL:-4}"

# Coverage configuration (passed from Maven)
if [ -n "${COVERAGE_SUBMIT_FLAGS}" ]; then
    echo "JaCoCo coverage enabled: ${COVERAGE_SUBMIT_FLAGS}"
fi

# Base Spark configurations
export PYSP_TEST_spark_sql_shuffle_partitions=12
export PYSP_TEST_spark_dynamicAllocation_enabled=false
export PYSP_TEST_spark_driver_extraJavaOptions="-Duser.timezone=UTC ${COVERAGE_SUBMIT_FLAGS}"
export PYSP_TEST_spark_executor_extraJavaOptions="-Duser.timezone=UTC ${COVERAGE_SUBMIT_FLAGS}"
export PYSP_TEST_spark_sql_session_timeZone=UTC
export TZ=UTC

# ==================== Helper Functions ====================

# Determine Delta Lake versions based on Spark version
get_delta_lake_versions() {
    local spark_ver=$1
    local scala_ver=$2
    
    if [[ "$spark_ver" =~ ^3\.2\. ]]; then
        echo "2.0.1"
    elif [[ "$spark_ver" =~ ^3\.3\. ]]; then
        echo "2.1.1 2.2.0 2.3.0"
    elif [[ "$spark_ver" =~ ^3\.4\. ]]; then
        echo "2.4.0"
    elif [[ "$spark_ver" =~ ^3\.5\.[3-9] ]]; then
        echo "3.3.0"
    elif [[ "$spark_ver" =~ ^4\.0\. ]]; then
        if [[ "$scala_ver" == "2.13" ]]; then
            echo "4.0.0"
        else
            echo ""
        fi
    else
        echo ""
    fi
}

# ==================== Test Functions ====================

run_default_tests() {
    echo ""
    echo ">>> Running DEFAULT tests..."
    ./run_pyspark_from_build.sh
    echo "✓ DEFAULT tests completed"
}

run_delta_lake_tests() {
    echo ""
    echo ">>> Running Delta Lake tests..."
    
    local delta_versions=$(get_delta_lake_versions "$SPARK_VER" "$SCALA_BINARY_VER")
    
    if [ -z "$delta_versions" ]; then
        echo "skipping Delta Lake tests - No compatible version for Spark $SPARK_VER"
        return 0
    fi
    
    for version in $delta_versions; do
        echo ""
        echo "--- Running Delta Lake tests for version $version ---"
        
        # Determine the correct Delta Lake artifact
        if [[ "$version" == "3.3.0" || "$version" == "4.0.0" ]]; then
            DELTA_JAR="io.delta:delta-spark_${SCALA_BINARY_VER}:$version"
        else
            DELTA_JAR="io.delta:delta-core_${SCALA_BINARY_VER}:$version"
        fi
        
        # Run tests with Delta Lake configuration
        PYSP_TEST_spark_jars_packages="$DELTA_JAR" \
        PYSP_TEST_spark_sql_extensions="io.delta.sql.DeltaSparkSessionExtension" \
        PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
            ./run_pyspark_from_build.sh -m delta_lake --delta_lake
        
        echo "✓ Delta Lake $version tests completed"
    done
}

run_iceberg_tests() {
    # Currently we only support Iceberg 1.6.1 for Spark 3.5.x
    ICEBERG_VERSION=1.6.1
    
    # Get the major/minor version of Spark
    ICEBERG_SPARK_VER=$(echo "$SPARK_VER" | cut -d. -f1,2)
    IS_SPARK_35X=0
    
    # If $SPARK_VER starts with 3.5, then set $IS_SPARK_35X to 1
    if [[ "$ICEBERG_SPARK_VER" = "3.5" ]]; then
        IS_SPARK_35X=1
    fi
    
    # RAPIDS-iceberg only supports Spark 3.5.x yet
    if [[ "$IS_SPARK_35X" -ne "1" ]]; then
        echo "!!!! Skipping Iceberg tests. GPU acceleration of Iceberg is not supported on $ICEBERG_SPARK_VER"
        return 0
    fi
    
    local test_type=${1:-'default'}
    
    if [[ "$test_type" == "default" ]]; then
        echo ""
        echo "!!! Running Iceberg tests (default catalog)..."
        
        PYSP_TEST_spark_driver_memory="6G" \
        PYSP_TEST_spark_jars_packages="org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION}" \
        PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
        PYSP_TEST_spark_sql_catalog_spark__catalog_type="hadoop" \
        PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="/tmp/spark-warehouse-$RANDOM" \
            ./run_pyspark_from_build.sh -m iceberg --iceberg
        
        echo "✓ Iceberg default catalog tests completed"
        
    elif [[ "$test_type" == "rest" ]]; then
        echo ""
        echo "!!! Running Iceberg tests with REST catalog..."
        
        ICEBERG_REST_JARS="org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION},\
org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"
        
        env \
            ICEBERG_TEST_REMOTE_CATALOG=1 \
            PYSP_TEST_spark_driver_memory=6G \
            PYSP_TEST_spark_jars_packages="${ICEBERG_REST_JARS}" \
            PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
            PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_catalog-impl="org.apache.iceberg.rest.RESTCatalog" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_uri="${ICEBERG_REST_CATALOG_URI:-http://10.172.54.77:9001/iceberg}" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_auth_type="none" \
            ./run_pyspark_from_build.sh -m iceberg --iceberg
        
        echo "✓ Iceberg REST catalog tests completed"
        
    elif [[ "$test_type" == "s3tables" ]]; then
        echo ""
        echo "!!! Running Iceberg tests with S3 Tables..."
        
        # AWS deps versions for Spark 3.5.x
        AWS_SDK_VERSION=${AWS_SDK_VERSION:-"2.29.26"}
        HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION:-"3.3.4"}
        AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION:-"1.12.709"}
        S3TABLES_CATALOG_VERSION=${S3TABLES_CATALOG_VERSION:-"0.1.6"}
        
        ICEBERG_S3TABLES_JARS="org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION},\
software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:${S3TABLES_CATALOG_VERSION},\
software.amazon.awssdk:apache-client:${AWS_SDK_VERSION},\
software.amazon.awssdk:aws-core:${AWS_SDK_VERSION},\
software.amazon.awssdk:dynamodb:${AWS_SDK_VERSION},\
software.amazon.awssdk:glue:${AWS_SDK_VERSION},\
software.amazon.awssdk:http-client-spi:${AWS_SDK_VERSION},\
software.amazon.awssdk:kms:${AWS_SDK_VERSION},\
software.amazon.awssdk:s3:${AWS_SDK_VERSION},\
software.amazon.awssdk:sdk-core:${AWS_SDK_VERSION},\
software.amazon.awssdk:sts:${AWS_SDK_VERSION},\
software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION},\
software.amazon.awssdk:s3tables:${AWS_SDK_VERSION},\
org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},\
com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_BUNDLE_VERSION}"
        
        # Check if AWS credentials are set
        if [ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
            echo "WARNING: AWS credentials not set."
            echo "   S3 Tables tests require AWS credentials. Please set:"
            echo "   - AWS_ACCESS_KEY_ID"
            echo "   - AWS_SECRET_ACCESS_KEY"
            echo "   Skipping S3 Tables tests..."
            return 0
        fi
        
        # Requires to setup s3 buckets and namespaces to run iceberg s3tables tests.
        # These steps are included in the test pipeline.
        # Please refer to integration_tests/README.md#run-apache-iceberg-s3tables-tests
        env \
            ICEBERG_TEST_REMOTE_CATALOG=1 \
            PYSP_TEST_spark_driver_memory=6G \
            PYSP_TEST_spark_jars_packages="${ICEBERG_S3TABLES_JARS}" \
            PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
            PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_catalog-impl="software.amazon.s3tables.iceberg.S3TablesCatalog" \
            PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="${S3TABLES_BUCKET_ARN:-arn:aws:s3tables:us-west-2:503960270539:bucket/spark-qa-table-bucket}" \
            ./run_pyspark_from_build.sh -s -m iceberg --iceberg
        
        echo "✓ Iceberg S3 Tables tests completed"
    fi
}

run_avro_tests() {
    echo ""
    echo ">>> Running Avro tests..."
    
    # Remove existing avro jars to avoid conflicts
    rm -vf "$INTEGRATION_TESTS_HOME"/target/dependency/spark-avro*.jar || true
    
    PYSP_TEST_spark_jars_packages="org.apache.spark:spark-avro_${SCALA_BINARY_VER}:${SPARK_VER}" \
    PYSP_TEST_spark_jars_repositories="https://repository.apache.org/snapshots" \
        ./run_pyspark_from_build.sh -k avro
    
    echo "✓ Avro tests completed"
}

# ==================== Main Execution ====================

TEST_MODE="${1:-DEFAULT}"

echo ""
echo "Running test mode: $TEST_MODE"
echo "========================================="

case "$TEST_MODE" in
    DEFAULT)
        run_default_tests
        ;;
    
    DELTA_LAKE_ONLY)
        run_delta_lake_tests
        ;;
    
    ICEBERG_ONLY)
        run_iceberg_tests
        ;;
    
    ICEBERG_REST_ONLY)
        run_iceberg_tests 'rest'
        ;;
    
    ICEBERG_S3TABLES)
        run_iceberg_tests 's3tables'
        ;;
    
    AVRO_ONLY)
        run_avro_tests
        ;;
    
    ALL)
        run_default_tests
        run_delta_lake_tests
        run_iceberg_tests
        run_iceberg_tests 'rest'
        run_iceberg_tests 's3tables'
        run_avro_tests
        ;;
    
    *)
        echo "ERROR: Unknown test mode: $TEST_MODE"
        echo ""
        echo "Valid test modes:"
        echo "  DEFAULT             - Run all basic tests"
        echo "  DELTA_LAKE_ONLY     - Run only Delta Lake tests"
        echo "  ICEBERG_ONLY        - Run only Iceberg tests"
        echo "  ICEBERG_REST_ONLY   - Run only Iceberg REST catalog tests"
        echo "  ICEBERG_S3TABLES    - Run only Iceberg S3 Tables tests"
        echo "  AVRO_ONLY          - Run only Avro tests"
        echo "  ALL                - Run all tests (including all Iceberg catalogs)"
        exit 1
        ;;
esac

echo ""
echo "========================================="
echo "✓ Test mode $TEST_MODE completed!"
echo "========================================="
