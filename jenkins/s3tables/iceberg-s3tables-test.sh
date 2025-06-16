#!/bin/bash
#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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

set -ex

AWS_REGION="us-west-2"
NAMESPACE_NAME="test_namespace"

# Function to generate a unique S3 bucket name
generate_random_s3_bucket_name() {
    # S3 bucket names must be globally unique, lowercase, no underscores, 3-63 chars.
    # Current time (HH-mm-ss) and a random 8-character hex string for uniqueness.
    CURRENT_TIME=$(date +"%H-%M-%S")
    RANDOM_HEX=$(head /dev/urandom | tr -dc a-f0-9 | head -c 8)
    BUCKET_NAME="iceberg-test-${CURRENT_TIME}-${RANDOM_HEX}"
    return "$BUCKET_NAME"
}

create_s3tables_bucket() {
  bucket_name="$1"
  CMD="aws s3tables create-table-bucket \
           --region ${AWS_REGION} \
           --name ${bucket_name}"
  echo "Creating aws s3tables table bucket: ${bucket_name}"
  arn="$(CMD) | jq -r '.arn'"
  echo "ARN of bucket ${bucket_name} is $arn"
  return "$arn"
}

create_s3tables_namespace() {
  bucket_arn="$1"
  CMD="aws s3tables create-namespace \
           --table-bucket-arn ${bucket_arn} \
           --region ${AWS_REGION} \
           --namespace ${NAMESPACE_NAME}"
  echo "Creating namespace ${NAMESPACE_NAME} in bucket ${bucket_arn}"
}


run_iceberg_s3tables_test() {
 # Currently we only support Iceberg 1.6.1 for Spark 3.5.x
   ICEBERG_VERSION=1.6.1
   # get the major/minor version of Spark
   ICEBERG_SPARK_VER=$(echo "$SPARK_VER" | cut -d. -f1,2)
   IS_SPARK_35X=0
   # If $SPARK_VER starts with 3.5, then set $IS_SPARK_35X to 1
   if [[ "$ICEBERG_SPARK_VER" = "3.5" ]]; then
     IS_SPARK_35X=1
   fi

  # RAPIDS-iceberg only support Spark 3.5.x yet
  if [[ "IS_SPARK_35X" -ne "1" ]]; then
    echo "!!!! Skipping Iceberg tests. GPU acceleration of Iceberg is not supported on $ICEBERG_SPARK_VER"
  else
    echo "!!! Running iceberg test with s3table catalog"

    JARS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
    software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.6,\
    software.amazon.awssdk:apache-client:2.29.26,\
    software.amazon.awssdk:aws-core:2.29.26,\
    software.amazon.awssdk:dynamodb:2.29.26,\
    software.amazon.awssdk:glue:2.29.26,\
    software.amazon.awssdk:http-client-spi:2.29.26,\
    software.amazon.awssdk:kms:2.29.26,\
    software.amazon.awssdk:s3:2.29.26,\
    software.amazon.awssdk:sdk-core:2.29.26,\
    software.amazon.awssdk:sts:2.29.26,\
    software.amazon.awssdk:url-connection-client:2.29.26,\
    software.amazon.awssdk:s3tables:2.29.26,\
    org.apache.hadoop:hadoop-aws:3.3.4,\
    com.amazonaws:aws-java-sdk-bundle:1.12.709"

     # aws configure
     # create table bucket
     # create namespace
     ICEBERG_TEST_S3TABLES_NAMESPACE="<namespace name>" \
     env 'PYSP_TEST_spark_sql_catalog_spark__catalog_table-default_write_spark_fanout_enabled=false' \
         PYSP_TEST_spark_driver_memory="6G" \
         PYSP_TEST_spark_jars_packages="$JARS" \
         PYSP_TEST_spark_hadoop_fs_s3_impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
         PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
         PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
         PYSP_TEST_spark_sql_catalog_spark__catalog_catalog-impl="software.amazon.s3tables.iceberg.S3TablesCatalog" \
         PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="<bucket arn>" \
         ./run_pyspark_from_build.sh -m iceberg --iceberg

     # drop all tables in namespace
     # drop namespace
     # drop table bucket
  fi
}

destroy_table_bucket() {
  bucket_arn="$1"
  list_table_cmd="aws s3tables list-tables \
                      --table-bucket-arn ${bucket_arn} \
                      --region ${AWS_REGION} \
                      --namespace ${NAMESPACE_NAME} \
                      --no-paginate "
  table_names=$(list_table_cmd | jq -r '.tables.[].name')
  while IFS=$'\n' read -r table_name; do
    echo " Deleting table: ${table_name}"
    aws s3table delete-table \
      --table-bucket-arn "${bucket_arn}" \
      --namespace "${NAMESPACE_NAME}" \
      --name "${table_name}"
  done <<< "${table_names}"
}

BUCKET_NAME=$(generate_random_s3_bucket_name)
BUCKET_ARN=$(create_s3tables_bucket "$BUCKET_NAME")

create_s3tables_namespace "$BUCKET_ARN"

run_iceberg_s3tables_test

destroy_table_bucket "$BUCKET_ARN"

