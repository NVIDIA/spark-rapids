source ~/venv/spark-rapids/bin/activate

cd ~/Workspace/issues/14253/spark-rapids/integration_tests


export ICEBERG_SPARK_VER=3.5
export SCALA_BINARY_VER=2.12
export ICEBERG_VERSION=1.6.1
export S3TABLES_BUCKET_ARN="arn:aws:s3tables:us-west-2:503960270539:bucket/iceberg-it-test-1"
# export SPARK_HOME="/home/ubuntu/Apps/spark-3.5.6-bin-hadoop3"
export SPARK_HOME="/home/rayliu/Apps/spark/spark-3.5.6-bin-hadoop3"

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

    # Requires to setup s3 buckets and namespaces to run iceberg s3tables tests.
    # These steps are included in the test pipeline.
    # Please refer to integration_tests/README.md#run-apache-iceberg-s3tables-tests
env \
    ICEBERG_TEST_REMOTE_CATALOG=1 \
    TEST_PARALLEL=2 \
    PYSP_TEST_spark_driver_memory=6G \
    PYSP_TEST_spark_executor_memory=6G \
    PYSP_TEST_spark_rapids_filecache_enabled=true \
    PYSP_TEST_spark_jars_packages="${ICEBERG_S3TABLES_JARS}" \
    PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
    PYSP_TEST_spark_sql_catalog_spark__catalog_catalog-impl="software.amazon.s3tables.iceberg.S3TablesCatalog" \
    PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="${S3TABLES_BUCKET_ARN}" \
    ./run_pyspark_from_build.sh -s -m iceberg --iceberg -k 'test_iceberg_read_with_filecache'
