/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tests.tpcxbb

import com.nvidia.spark.rapids.tests.common.BenchUtils
import com.nvidia.spark.rapids.tests.tpcxbb.TpcxbbLikeSpark.{csvToOrc, csvToParquet}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit

// DecimalType to DoubleType, bigint to LongType
object TpcxbbLikeSpark {
  private def setupWrite(
      df: DataFrame, 
      name: String,
      coalesce: Map[String, Int],
      repartition: Map[String, Int]): DataFrameWriter[Row] = {
    val repart = BenchUtils.applyCoalesceRepartition(name, df, coalesce, repartition)
    repart.write.mode("overwrite")
  }

  def csvToParquet(
      spark: SparkSession, 
      basePath: String, 
      baseOutput: String, 
      coalesce: Map[String, Int],
      repartition: Map[String, Int]): Unit = {
    setupWrite(readCustomerCSV(spark, basePath + "/customer/"), "customer", coalesce, repartition).parquet(baseOutput + "/customer/")
    setupWrite(readCustomerAddressCSV(spark, basePath + "/customer_address/"), "customer_address", coalesce, repartition).parquet(baseOutput + "/customer_address/")
    setupWrite(readItemCSV(spark, basePath + "/item/"), "item", coalesce, repartition).parquet(baseOutput + "/item/")
    setupWrite(readStoreSalesCSV(spark, basePath + "/store_sales/"), "store_sales", coalesce, repartition).parquet(baseOutput + "/store_sales/")
    setupWrite(readDateDimCSV(spark, basePath + "/date_dim/"), "date_dim", coalesce, repartition).parquet(baseOutput + "/date_dim/")
    setupWrite(readStoreCSV(spark, basePath + "/store/"), "store", coalesce, repartition).parquet(baseOutput + "/store/")
    setupWrite(readCustomerDemographicsCSV(spark, basePath + "/customer_demographics/"), "customer_demographics", coalesce, repartition).parquet(baseOutput + "/customer_demographics/")
    setupWrite(readReviewsCSV(spark, basePath + "/product_reviews/"), "product_reviews", coalesce, repartition).parquet(baseOutput + "/product_reviews/")
    setupWrite(readWebSalesCSV(spark, basePath + "/web_sales/"), "web_sales", coalesce, repartition).parquet(baseOutput + "/web_sales/")
    setupWrite(readWebClickStreamsCSV(spark, basePath + "/web_clickstreams/"), "web_clickstreams", coalesce, repartition).parquet(baseOutput + "/web_clickstreams/")
    setupWrite(readHouseholdDemographicsCSV(spark, basePath + "/household_demographics/"), "household_demographics", coalesce, repartition).parquet(baseOutput + "/household_demographics/")
    setupWrite(readWebPageCSV(spark, basePath + "/web_page/"), "web_page", coalesce, repartition).parquet(baseOutput + "/web_page/")
    setupWrite(readTimeDimCSV(spark, basePath + "/time_dim/"), "time_dim", coalesce, repartition).parquet(baseOutput + "/time_dim/")
    setupWrite(readWebReturnsCSV(spark, basePath + "/web_returns/"), "web_returns", coalesce, repartition).parquet(baseOutput + "/web_returns/")
    setupWrite(readWarehouseCSV(spark, basePath + "/warehouse/"), "warehouse", coalesce, repartition).parquet(baseOutput + "/warehouse/")
    setupWrite(readPromotionCSV(spark, basePath + "/promotion/"), "promotion", coalesce, repartition).parquet(baseOutput + "/promotion/")
    setupWrite(readStoreReturnsCSV(spark, basePath + "/store_returns/"), "store_returns", coalesce, repartition).parquet(baseOutput + "/store_returns/")
    setupWrite(readInventoryCSV(spark, basePath + "/inventory/"), "inventory", coalesce, repartition).parquet(baseOutput + "/inventory/")
    setupWrite(readMarketPricesCSV(spark, basePath + "/item_marketprices/"), "item_marketprices", coalesce, repartition).parquet(baseOutput + "/item_marketprices/")
  }

  def csvToOrc(
      spark: SparkSession, 
      basePath: String, 
      baseOutput: String, 
      coalesce: Map[String, Int],
      repartition: Map[String, Int]): Unit = {
    setupWrite(readCustomerCSV(spark, basePath + "/customer/"), "customer", coalesce, repartition).orc(baseOutput + "/customer/")
    setupWrite(readCustomerAddressCSV(spark, basePath + "/customer_address/"), "customer_address", coalesce, repartition).orc(baseOutput + "/customer_address/")
    setupWrite(readItemCSV(spark, basePath + "/item/"), "item", coalesce, repartition).orc(baseOutput + "/item/")
    setupWrite(readStoreSalesCSV(spark, basePath + "/store_sales/"), "store_sales", coalesce, repartition).orc(baseOutput + "/store_sales/")
    setupWrite(readDateDimCSV(spark, basePath + "/date_dim/"), "date_dim", coalesce, repartition).orc(baseOutput + "/date_dim/")
    setupWrite(readStoreCSV(spark, basePath + "/store/"), "store", coalesce, repartition).orc(baseOutput + "/store/")
    setupWrite(readCustomerDemographicsCSV(spark, basePath + "/customer_demographics/"), "customer_demographics", coalesce, repartition).orc(baseOutput + "/customer_demographics/")
    setupWrite(readReviewsCSV(spark, basePath + "/product_reviews/"), "product_reviews", coalesce, repartition).orc(baseOutput + "/product_reviews/")
    setupWrite(readWebSalesCSV(spark, basePath + "/web_sales/"), "web_sales", coalesce, repartition).orc(baseOutput + "/web_sales/")
    setupWrite(readWebClickStreamsCSV(spark, basePath + "/web_clickstreams/"), "web_clickstreams", coalesce, repartition).orc(baseOutput + "/web_clickstreams/")
    setupWrite(readHouseholdDemographicsCSV(spark, basePath + "/household_demographics/"), "household_demographics", coalesce, repartition).orc(baseOutput + "/household_demographics/")
    setupWrite(readWebPageCSV(spark, basePath + "/web_page/"), "web_page", coalesce, repartition).orc(baseOutput + "/web_page/")
    setupWrite(readTimeDimCSV(spark, basePath + "/time_dim/"), "time_dim", coalesce, repartition).orc(baseOutput + "/time_dim/")
    setupWrite(readWebReturnsCSV(spark, basePath + "/web_returns/"), "web_returns", coalesce, repartition).orc(baseOutput + "/web_returns/")
    setupWrite(readWarehouseCSV(spark, basePath + "/warehouse/"), "warehouse", coalesce, repartition).orc(baseOutput + "/warehouse/")
    setupWrite(readPromotionCSV(spark, basePath + "/promotion/"), "promotion", coalesce, repartition).orc(baseOutput + "/promotion/")
    setupWrite(readStoreReturnsCSV(spark, basePath + "/store_returns/"), "store_returns", coalesce, repartition).orc(baseOutput + "/store_returns/")
    setupWrite(readInventoryCSV(spark, basePath + "/inventory/"), "inventory", coalesce, repartition).orc(baseOutput + "/inventory/")
    setupWrite(readMarketPricesCSV(spark, basePath + "/item_marketprices/"), "item_marketprices", coalesce, repartition).orc(baseOutput + "/item_marketprices/")
  }

  def setupAllCSV(spark: SparkSession, basePath: String): Unit = {
    setupCustomerCSV(spark, basePath + "/customer/")
    setupCustomerAddressCSV(spark, basePath + "/customer_address/")
    setupItemCSV(spark, basePath + "/item/")
    setupStoreSalesCSV(spark, basePath + "/store_sales/")
    setupDateDimCSV(spark, basePath + "/date_dim/")
    setupStoreCSV(spark, basePath + "/store/")
    setupCustomerDemographicsCSV(spark, basePath + "/customer_demographics/")
    setupReviewsCSV(spark, basePath + "/product_reviews/")
    setupWebSalesCSV(spark, basePath + "/web_sales/")
    setupWebClickStreamsCSV(spark, basePath + "/web_clickstreams/")
    setupHouseholdDemographicsCSV(spark, basePath + "/household_demographics/")
    setupWebPageCSV(spark, basePath + "/web_page/")
    setupTimeDimCSV(spark, basePath + "/time_dim/")
    setupWebReturnsCSV(spark, basePath + "/web_returns/")
    setupWarehouseCSV(spark, basePath + "/warehouse/")
    setupPromotionCSV(spark, basePath + "/promotion/")
    setupStoreReturnsCSV(spark, basePath + "/store_returns/")
    setupInventoryCSV(spark, basePath + "/inventory/")
    setupMarketPricesCSV(spark, basePath + "/item_marketprices/")
  }

  def setupAllParquet(spark: SparkSession, basePath: String): Unit = {
    setupCustomerParquet(spark, basePath + "/customer/")
    setupCustomerAddressParquet(spark, basePath + "/customer_address/")
    setupItemParquet(spark, basePath + "/item/")
    setupStoreSalesParquet(spark, basePath + "/store_sales/")
    setupDateDimParquet(spark, basePath + "/date_dim/")
    setupStoreParquet(spark, basePath + "/store/")
    setupCustomerDemographicsParquet(spark, basePath + "/customer_demographics/")
    setupReviewsParquet(spark, basePath + "/product_reviews/")
    setupWebSalesParquet(spark, basePath + "/web_sales/")
    setupWebClickStreamsParquet(spark, basePath + "/web_clickstreams/")
    setupHouseholdDemographicsParquet(spark, basePath + "/household_demographics/")
    setupWebPageParquet(spark, basePath + "/web_page/")
    setupTimeDimParquet(spark, basePath + "/time_dim/")
    setupWebReturnsParquet(spark, basePath + "/web_returns/")
    setupWarehouseParquet(spark, basePath + "/warehouse/")
    setupPromotionParquet(spark, basePath + "/promotion/")
    setupStoreReturnsParquet(spark, basePath + "/store_returns/")
    setupInventoryParquet(spark, basePath + "/inventory/")
    setupMarketPricesParquet(spark, basePath + "/item_marketprices/")
  }

  def setupAllParquetWithMetastore(spark: SparkSession, basePath: String): Unit = {
    setupParquetTableWithMetastore(spark, "customer", basePath + "/customer/")
    setupParquetTableWithMetastore(spark, "customer_address", basePath + "/customer_address/")
    setupParquetTableWithMetastore(spark, "item", basePath + "/item/")
    setupParquetTableWithMetastore(spark, "store_sales", basePath + "/store_sales/")
    setupParquetTableWithMetastore(spark, "date_dim", basePath + "/date_dim/")
    setupParquetTableWithMetastore(spark, "store", basePath + "/store/")
    setupParquetTableWithMetastore(spark, "customer_demographics", basePath + "/customer_demographics/")
    setupParquetTableWithMetastore(spark, "product_reviews", basePath + "/product_reviews/")
    setupParquetTableWithMetastore(spark, "web_sales", basePath + "/web_sales/")
    setupParquetTableWithMetastore(spark, "web_clickstreams", basePath + "/web_clickstreams/")
    setupParquetTableWithMetastore(spark, "household_demographics", basePath + "/household_demographics/")
    setupParquetTableWithMetastore(spark, "web_page", basePath + "/web_page/")
    setupParquetTableWithMetastore(spark, "time_dim", basePath + "/time_dim/")
    setupParquetTableWithMetastore(spark, "web_returns", basePath + "/web_returns/")
    setupParquetTableWithMetastore(spark, "warehouse", basePath + "/warehouse/")
    setupParquetTableWithMetastore(spark, "promotion", basePath + "/promotion/")
    setupParquetTableWithMetastore(spark, "store_returns", basePath + "/store_returns/")
    setupParquetTableWithMetastore(spark, "inventory", basePath + "/inventory/")
    setupParquetTableWithMetastore(spark, "item_marketprices", basePath + "/item_marketprices/")

    spark.sql("SHOW TABLES").show
  }

  def setupParquetTableWithMetastore(spark: SparkSession, table: String, path: String): Unit = {
    setupTableWithMetastore(spark, table, "PARQUET", path)
  }

  def setupTableWithMetastore(spark: SparkSession, table: String, format: String, path: String): Unit = {
    // Yes there are SQL injection vulnerabilities here, so don't exploit it, this is just test code
    spark.catalog.dropTempView(table)
    spark.sql(s"DROP TABLE IF EXISTS ${table}")
    spark.sql(s"CREATE TABLE ${table} USING ${format} LOCATION '${path}'")
  }

  def setupAllOrc(spark: SparkSession, basePath: String): Unit = {
    setupCustomerOrc(spark, basePath + "/customer/")
    setupCustomerAddressOrc(spark, basePath + "/customer_address/")
    setupItemOrc(spark, basePath + "/item/")
    setupStoreSalesOrc(spark, basePath + "/store_sales/")
    setupDateDimOrc(spark, basePath + "/date_dim/")
    setupStoreOrc(spark, basePath + "/store/")
    setupCustomerDemographicsOrc(spark, basePath + "/customer_demographics/")
    setupReviewsOrc(spark, basePath + "/product_reviews/")
    setupWebSalesOrc(spark, basePath + "/web_sales/")
    setupWebClickStreamsOrc(spark, basePath + "/web_clickstreams/")
    setupHouseholdDemographicsOrc(spark, basePath + "/household_demographics/")
    setupWebPageOrc(spark, basePath + "/web_page/")
    setupTimeDimOrc(spark, basePath + "/time_dim/")
    setupWebReturnsOrc(spark, basePath + "/web_returns/")
    setupWarehouseOrc(spark, basePath + "/warehouse/")
    setupPromotionOrc(spark, basePath + "/promotion/")
    setupStoreReturnsOrc(spark, basePath + "/store_returns/")
    setupInventoryOrc(spark, basePath + "/inventory/")
    setupMarketPricesOrc(spark, basePath + "/item_marketprices/")
  }

  // CUSTOMER
  val customerSchema = StructType(Array(
    StructField("c_customer_sk", LongType, false),
    StructField("c_customer_id", StringType, false),
    StructField("c_current_cdemo_sk", LongType),
    StructField("c_current_hdemo_sk", LongType),
    StructField("c_current_addr_sk", LongType),
    StructField("c_first_shipto_date_sk", LongType),
    StructField("c_first_sales_date_sk", LongType),
    StructField("c_salutation", StringType),
    StructField("c_first_name", StringType),
    StructField("c_last_name", StringType),
    StructField("c_preferred_cust_flag", StringType),
    StructField("c_birth_day", IntegerType),
    StructField("c_birth_month", IntegerType),
    StructField("c_birth_year", IntegerType),
    StructField("c_birth_country", StringType),
    StructField("c_login", StringType),
    StructField("c_email_address", StringType),
    StructField("c_last_review_date", StringType)
  ))

  def readCustomerCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(customerSchema).csv(path)

  def setupCustomerCSV(spark: SparkSession, path: String): Unit =
    readCustomerCSV(spark, path).createOrReplaceTempView("customer")

  def setupCustomerParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("customer")

  def setupCustomerOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("customer")

  // CUSTOMER ADDRESS
  val customerAddressSchema = StructType(Array(
    StructField("ca_address_sk", LongType, false),
    StructField("ca_address_id", StringType, false),
    StructField("ca_street_number", StringType),
    StructField("ca_street_name", StringType),
    StructField("ca_street_type", StringType),
    StructField("ca_suite_number", StringType),
    StructField("ca_city", StringType),
    StructField("ca_county", StringType),
    StructField("ca_state", StringType),
    StructField("ca_zip", StringType),
    StructField("ca_country", StringType),
    StructField("ca_gmt_offset", DoubleType),
    StructField("ca_location_type", StringType)
  ))

  def readCustomerAddressCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(customerAddressSchema).csv(path)

  def setupCustomerAddressCSV(spark: SparkSession, path: String): Unit =
    readCustomerAddressCSV(spark, path).createOrReplaceTempView("customer_address")

  def setupCustomerAddressParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("customer_address")

  def setupCustomerAddressOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("customer_address")

  // STORE SALES
  val storeSalesSchema = StructType(Array(
    StructField("ss_sold_date_sk", LongType),
    StructField("ss_sold_time_sk", LongType),
    StructField("ss_item_sk", LongType, false),
    StructField("ss_customer_sk", LongType),
    StructField("ss_cdemo_sk", LongType),
    StructField("ss_hdemo_sk", LongType),
    StructField("ss_addr_sk", LongType),
    StructField("ss_store_sk", LongType),
    StructField("ss_promo_sk", LongType),
    StructField("ss_ticket_number", LongType, false),
    StructField("ss_quantity", IntegerType),
    StructField("ss_wholesale_cost", DoubleType),
    StructField("ss_list_price", DoubleType),
    StructField("ss_sales_price", DoubleType),
    StructField("ss_ext_discount_amt", DoubleType),
    StructField("ss_ext_sales_price", DoubleType),
    StructField("ss_ext_wholesale_cost", DoubleType),
    StructField("ss_ext_list_price", DoubleType),
    StructField("ss_ext_tax", DoubleType),
    StructField("ss_coupon_amt", DoubleType),
    StructField("ss_net_paid", DoubleType),
    StructField("ss_net_paid_inc_tax", DoubleType),
    StructField("ss_net_profit", DoubleType)
  ))

  def readStoreSalesCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(storeSalesSchema).csv(path)

  def setupStoreSalesCSV(spark: SparkSession, path: String): Unit =
    readStoreSalesCSV(spark, path).createOrReplaceTempView("store_sales")

  def setupStoreSalesParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("store_sales")

  def setupStoreSalesOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("store_sales")

  // ITEM
  val itemSchema = StructType(Array(
    StructField("i_item_sk", LongType, false),
    StructField("i_item_id", StringType, false),
    StructField("i_rec_start_date", StringType),
    StructField("i_rec_end_date", StringType),
    StructField("i_item_desc", StringType),
    StructField("i_current_price", DoubleType),
    StructField("i_wholesale_cost", DoubleType),
    StructField("i_brand_id", IntegerType),
    StructField("i_brand", StringType),
    StructField("i_class_id", IntegerType),
    StructField("i_class", StringType),
    StructField("i_category_id", IntegerType),
    StructField("i_category", StringType),
    StructField("i_manufact_id", IntegerType),
    StructField("i_manufact", StringType),
    StructField("i_size", StringType),
    StructField("i_formulation", StringType),
    StructField("i_color", StringType),
    StructField("i_units", StringType),
    StructField("i_container", StringType),
    StructField("i_manager_id", IntegerType),
    StructField("i_product_name", StringType)
  ))

  def readItemCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(itemSchema).csv(path)

  def setupItemCSV(spark: SparkSession, path: String): Unit =
    readItemCSV(spark, path).createOrReplaceTempView("item")

  def setupItemParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("item")

  def setupItemOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("item")

  // DATE DIM
  val dateDimSchema = StructType(Array(
    StructField("d_date_sk", LongType, false),
    StructField("d_date_id", StringType, false),
    StructField("d_date", StringType),
    StructField("d_month_seq", IntegerType),
    StructField("d_week_seq", IntegerType),
    StructField("d_quarter_seq", IntegerType),
    StructField("d_year", IntegerType),
    StructField("d_dow", IntegerType),
    StructField("d_moy", IntegerType),
    StructField("d_dom", IntegerType),
    StructField("d_qoy", IntegerType),
    StructField("d_fy_year", IntegerType),
    StructField("d_fy_quarter_seq", IntegerType),
    StructField("d_fy_week_seq", IntegerType),
    StructField("d_day_name", StringType),
    StructField("d_quarter_name", StringType),
    StructField("d_holiday", StringType),
    StructField("d_weekend", StringType),
    StructField("d_following_holiday", StringType),
    StructField("d_first_dom", IntegerType),
    StructField("d_last_dom", IntegerType),
    StructField("d_same_day_ly", IntegerType),
    StructField("d_same_day_lq", IntegerType),
    StructField("d_current_day", StringType),
    StructField("d_current_week", StringType),
    StructField("d_current_month", StringType),
    StructField("d_current_quarter", StringType),
    StructField("d_current_year", StringType)
  ))

  def readDateDimCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(dateDimSchema).csv(path)

  def setupDateDimCSV(spark: SparkSession, path: String): Unit =
    readDateDimCSV(spark, path).createOrReplaceTempView("date_dim")

  def setupDateDimParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("date_dim")

  def setupDateDimOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("date_dim")

  // STORE
  val storeSchema = StructType(Array(
    StructField("s_store_sk", LongType, false),
    StructField("s_store_id", StringType, false),
    StructField("s_rec_start_date", StringType),
    StructField("s_rec_end_date", StringType),
    StructField("s_closed_date_sk", LongType),
    StructField("s_store_name", StringType),
    StructField("s_number_employees", IntegerType),
    StructField("s_floor_space", IntegerType),
    StructField("s_hours", StringType),
    StructField("s_manager", StringType),
    StructField("s_market_id", IntegerType),
    StructField("s_geography_class", StringType),
    StructField("s_market_desc", StringType),
    StructField("s_market_manager", StringType),
    StructField("s_division_id", IntegerType),
    StructField("s_division_name", StringType),
    StructField("s_company_id", IntegerType),
    StructField("s_company_name", StringType),
    StructField("s_street_number", StringType),
    StructField("s_street_name", StringType),
    StructField("s_street_type", StringType),
    StructField("s_suite_number", StringType),
    StructField("s_city", StringType),
    StructField("s_county", StringType),
    StructField("s_state", StringType),
    StructField("s_zip", StringType),
    StructField("s_country", StringType),
    StructField("s_gmt_offset", DoubleType),
    StructField("s_tax_precentage", DoubleType)
  ))

  def readStoreCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(storeSchema).csv(path)

  def setupStoreCSV(spark: SparkSession, path: String): Unit =
    readStoreCSV(spark, path).createOrReplaceTempView("store")

  def setupStoreParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("store")

  def setupStoreOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("store")

  // CUSTOMER DEMOGRAPHICS
  val customerDemoSchema = StructType(Array(
    StructField("cd_demo_sk", LongType, false),
    StructField("cd_gender", StringType),
    StructField("cd_marital_status", StringType),
    StructField("cd_education_status", StringType),
    StructField("cd_purchase_estimate", IntegerType),
    StructField("cd_credit_rating", StringType),
    StructField("cd_dep_count", IntegerType),
    StructField("cd_dep_employed_count", IntegerType),
    StructField("cd_dep_college_count", IntegerType)
  ))

  def readCustomerDemographicsCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(customerDemoSchema).csv(path)

  def setupCustomerDemographicsCSV(spark: SparkSession, path: String): Unit =
    readCustomerDemographicsCSV(spark, path).createOrReplaceTempView("customer_demographics")

  def setupCustomerDemographicsParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("customer_demographics")

  def setupCustomerDemographicsOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("customer_demographics")

  // PRODUCT REVIEWS
  val reviewsSchema = StructType(Array(
    StructField("pr_review_sk", LongType, false),
    StructField("pr_review_date", StringType),
    StructField("pr_review_time", StringType),
    StructField("pr_review_rating", IntegerType, false),
    StructField("pr_item_sk", LongType, false),
    StructField("pr_user_sk", LongType),
    StructField("pr_order_sk", LongType),
    StructField("pr_review_content", StringType, false)
  ))

  def readReviewsCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(reviewsSchema).csv(path)

  def setupReviewsCSV(spark: SparkSession, path: String): Unit =
    readReviewsCSV(spark, path).createOrReplaceTempView("product_reviews")

  def setupReviewsParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("product_reviews")

  def setupReviewsOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("product_reviews")

  // WEB SALES
  val webSalesSchema = StructType(Array(
    StructField("ws_sold_date_sk", LongType),
    StructField("ws_sold_time_sk", LongType),
    StructField("ws_ship_date_sk", LongType),
    StructField("ws_item_sk", LongType, false),
    StructField("ws_bill_customer_sk", LongType),
    StructField("ws_bill_cdemo_sk", LongType),
    StructField("ws_bill_hdemo_sk", LongType),
    StructField("ws_bill_addr_sk", LongType),
    StructField("ws_ship_customer_sk", LongType),
    StructField("ws_ship_cdemo_sk", LongType),
    StructField("ws_ship_hdemo_sk", LongType),
    StructField("ws_ship_addr_sk", LongType),
    StructField("ws_web_page_sk", LongType),
    StructField("ws_web_site_sk", LongType),
    StructField("ws_ship_mode_sk", LongType),
    StructField("ws_warehouse_sk", LongType),
    StructField("ws_promo_sk", LongType),
    StructField("ws_order_number", LongType, false),
    StructField("ws_quantity", IntegerType),
    StructField("ws_wholesale_cost", DoubleType),
    StructField("ws_list_price", DoubleType),
    StructField("ws_sales_price", DoubleType),
    StructField("ws_ext_discount_amt", DoubleType),
    StructField("ws_ext_sales_price", DoubleType),
    StructField("ws_ext_wholesale_cost", DoubleType),
    StructField("ws_ext_list_price", DoubleType),
    StructField("ws_ext_tax", DoubleType),
    StructField("ws_coupon_amt", DoubleType),
    StructField("ws_ext_ship_cost", DoubleType),
    StructField("ws_net_paid", DoubleType),
    StructField("ws_net_paid_inc_tax", DoubleType),
    StructField("ws_net_paid_inc_ship", DoubleType),
    StructField("ws_net_paid_inc_ship_tax", DoubleType),
    StructField("ws_net_profit", DoubleType)
  ))

  def readWebSalesCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(webSalesSchema).csv(path)

  def setupWebSalesCSV(spark: SparkSession, path: String): Unit =
    readWebSalesCSV(spark, path).createOrReplaceTempView("web_sales")

  def setupWebSalesParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("web_sales")

  def setupWebSalesOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("web_sales")

  // CLICK STREAMS
  val clickStreamsSchema = StructType(Array(
    StructField("wcs_click_date_sk", LongType),
    StructField("wcs_click_time_sk", LongType),
    StructField("wcs_sales_sk", LongType),
    StructField("wcs_item_sk", LongType),
    StructField("wcs_web_page_sk", LongType),
    StructField("wcs_user_sk", LongType)
  ))

  def readWebClickStreamsCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(clickStreamsSchema).csv(path)

  def setupWebClickStreamsCSV(spark: SparkSession, path: String): Unit =
    readWebClickStreamsCSV(spark, path).createOrReplaceTempView("web_clickstreams")

  def setupWebClickStreamsParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("web_clickstreams")

  def setupWebClickStreamsOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("web_clickstreams")

  // HOUSEHOLD DEMOGRAPHICS
  val houseDemoSchema = StructType(Array(
    StructField("hd_demo_sk", LongType, false),
    StructField("hd_income_band_sk", LongType),
    StructField("hd_buy_potential", StringType),
    StructField("hd_dep_count", IntegerType),
    StructField("hd_vehicle_count", IntegerType)
  ))

  def readHouseholdDemographicsCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(houseDemoSchema).csv(path)

  def setupHouseholdDemographicsCSV(spark: SparkSession, path: String): Unit =
    readHouseholdDemographicsCSV(spark, path).createOrReplaceTempView("household_demographics")

  def setupHouseholdDemographicsParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("household_demographics")

  def setupHouseholdDemographicsOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("household_demographics")

  // WEB PAGE
  val webPageSchema = StructType(Array(
    StructField("wp_web_page_sk", LongType, false),
    StructField("wp_web_page_id", StringType, false),
    StructField("wp_rec_start_date", StringType),
    StructField("wp_rec_end_date", StringType),
    StructField("wp_creation_date_sk", LongType),
    StructField("wp_access_date_sk", LongType),
    StructField("wp_autogen_flag", StringType),
    StructField("wp_customer_sk", LongType),
    StructField("wp_url", StringType),
    StructField("wp_type", StringType),
    StructField("wp_char_count", IntegerType),
    StructField("wp_link_count", IntegerType),
    StructField("wp_image_count", IntegerType),
    StructField("wp_max_ad_count", IntegerType)
  ))

  def readWebPageCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(webPageSchema).csv(path)

  def setupWebPageCSV(spark: SparkSession, path: String): Unit =
    readWebPageCSV(spark, path).createOrReplaceTempView("web_page")

  def setupWebPageParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("web_page")

  def setupWebPageOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("web_page")

  // TIME DIM
  val timeDimSchema = StructType(Array(
    StructField("t_time_sk", LongType, false),
    StructField("t_time_id", StringType, false),
    StructField("t_time", IntegerType),
    StructField("t_hour", IntegerType),
    StructField("t_minute", IntegerType),
    StructField("t_second", IntegerType),
    StructField("t_am_pm", StringType),
    StructField("t_shift", StringType),
    StructField("t_sub_shift", StringType),
    StructField("t_meal_time", StringType)
  ))

  def readTimeDimCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(timeDimSchema).csv(path)

  def setupTimeDimCSV(spark: SparkSession, path: String): Unit =
    readTimeDimCSV(spark, path).createOrReplaceTempView("time_dim")

  def setupTimeDimParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("time_dim")

  def setupTimeDimOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("time_dim")

  // WEB RETURNS
  val webReturnsSchema = StructType(Array(
    StructField("wr_returned_date_sk", LongType),
    StructField("wr_returned_time_sk", LongType),
    StructField("wr_item_sk", LongType, false),
    StructField("wr_refunded_customer_sk", LongType),
    StructField("wr_refunded_cdemo_sk", LongType),
    StructField("wr_refunded_hdemo_sk", LongType),
    StructField("wr_refunded_addr_sk", LongType),
    StructField("wr_returning_customer_sk", LongType),
    StructField("wr_returning_cdemo_sk", LongType),
    StructField("wr_returning_hdemo_sk", LongType),
    StructField("wr_returning_addr_sk", LongType),
    StructField("wr_web_page_sk", LongType),
    StructField("wr_reason_sk", LongType),
    StructField("wr_order_number", LongType, false),
    StructField("wr_return_quantity", IntegerType),
    StructField("wr_return_amt", DoubleType),
    StructField("wr_return_tax", DoubleType),
    StructField("wr_return_amt_inc_tax", DoubleType),
    StructField("wr_fee", DoubleType),
    StructField("wr_return_ship_cost", DoubleType),
    StructField("wr_refunded_cash", DoubleType),
    StructField("wr_reversed_charge", DoubleType),
    StructField("wr_account_credit", DoubleType),
    StructField("wr_net_loss", DoubleType)
  ))

  def readWebReturnsCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(webReturnsSchema).csv(path)

  def setupWebReturnsCSV(spark: SparkSession, path: String): Unit =
    readWebReturnsCSV(spark, path).createOrReplaceTempView("web_returns")

  def setupWebReturnsParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("web_returns")

  def setupWebReturnsOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("web_returns")

  // WAREHOUSE
  val warehouseSchema = StructType(Array(
    StructField("w_warehouse_sk", LongType, false),
    StructField("w_warehouse_id", StringType, false),
    StructField("w_warehouse_name", StringType),
    StructField("w_warehouse_sq_ft", IntegerType),
    StructField("w_street_number", StringType),
    StructField("w_street_name", StringType),
    StructField("w_street_type", StringType),
    StructField("w_suite_number", StringType),
    StructField("w_city", StringType),
    StructField("w_county", StringType),
    StructField("w_state", StringType),
    StructField("w_zip", StringType),
    StructField("w_country", StringType),
    StructField("w_gmt_offset", DoubleType)
  ))

  def readWarehouseCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(warehouseSchema).csv(path)

  def setupWarehouseCSV(spark: SparkSession, path: String): Unit =
    readWarehouseCSV(spark, path).createOrReplaceTempView("warehouse")

  def setupWarehouseParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("warehouse")

  def setupWarehouseOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("warehouse")

  // PROMOTION
  val promotionSchema = StructType(Array(
    StructField("p_promo_sk", LongType, false),
    StructField("p_promo_id", StringType, false),
    StructField("p_start_date_sk", LongType),
    StructField("p_end_date_sk", LongType),
    StructField("p_item_sk", LongType),
    StructField("p_cost", DoubleType),
    StructField("p_response_target", IntegerType),
    StructField("p_promo_name", StringType),
    StructField("p_channel_dmail", StringType),
    StructField("p_channel_email", StringType),
    StructField("p_channel_catalog", StringType),
    StructField("p_channel_tv", StringType),
    StructField("p_channel_radio", StringType),
    StructField("p_channel_press", StringType),
    StructField("p_channel_event", StringType),
    StructField("p_channel_demo", StringType),
    StructField("p_channel_details", StringType),
    StructField("p_purpose", StringType),
    StructField("p_discount_active", StringType)
  ))

  def readPromotionCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(promotionSchema).csv(path)

  def setupPromotionCSV(spark: SparkSession, path: String): Unit =
    readPromotionCSV(spark, path).createOrReplaceTempView("promotion")

  def setupPromotionParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("promotion")

  def setupPromotionOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("promotion")

  // STORE RETURNS
  val storeReturnsSchema = StructType(Array(
    StructField("sr_returned_date_sk", LongType),
    StructField("sr_return_time_sk", LongType),
    StructField("sr_item_sk", LongType, false),
    StructField("sr_customer_sk", LongType),
    StructField("sr_cdemo_sk", LongType),
    StructField("sr_hdemo_sk", LongType),
    StructField("sr_addr_sk", LongType),
    StructField("sr_store_sk", LongType),
    StructField("sr_reason_sk", LongType),
    StructField("sr_ticket_number", LongType, false),
    StructField("sr_return_quantity", IntegerType),
    StructField("sr_return_amt", DoubleType),
    StructField("sr_return_tax", DoubleType),
    StructField("sr_return_amt_inc_tax", DoubleType),
    StructField("sr_fee", DoubleType),
    StructField("sr_return_ship_cost", DoubleType),
    StructField("sr_refunded_cash", DoubleType),
    StructField("sr_reversed_charge", DoubleType),
    StructField("sr_store_credit", DoubleType),
    StructField("sr_net_loss", DoubleType)
  ))

  def readStoreReturnsCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(storeReturnsSchema).csv(path)

  def setupStoreReturnsCSV(spark: SparkSession, path: String): Unit =
    readStoreReturnsCSV(spark, path).createOrReplaceTempView("store_returns")

  def setupStoreReturnsParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("store_returns")

  def setupStoreReturnsOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("store_returns")

  // INVENTORY
  val inventorySchema = StructType(Array(
    StructField("inv_date_sk", LongType, false),
    StructField("inv_item_sk", LongType, false),
    StructField("inv_warehouse_sk", LongType, false),
    StructField("inv_quantity_on_hand", IntegerType)
  ))

  def readInventoryCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(inventorySchema).csv(path)

  def setupInventoryCSV(spark: SparkSession, path: String): Unit =
    readInventoryCSV(spark, path).createOrReplaceTempView("inventory")

  def setupInventoryParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("inventory")

  def setupInventoryOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("inventory")

  // MARKET PRICES
  val marketPricesSchema = StructType(Array(
    StructField("imp_sk", LongType, false),
    StructField("imp_item_sk", LongType, false),
    StructField("imp_competitor", StringType),
    StructField("imp_competitor_price", DoubleType),
    StructField("imp_start_date", LongType),
    StructField("imp_end_date", LongType)
  ))

  // The market prices directory has .dat files and separate audit .csv files,
  // so filter the path to only read the .dat files.
  def readMarketPricesCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(marketPricesSchema).csv(path + "/*.dat")

  def setupMarketPricesCSV(spark: SparkSession, path: String): Unit =
    readMarketPricesCSV(spark, path).createOrReplaceTempView("item_marketprices")

  def setupMarketPricesParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("item_marketprices")

  def setupMarketPricesOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("item_marketprices")

}

object Q1Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q1 uses UDTF")
  }
}

object Q2Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q2 uses UDTF")
  }
}

object Q3Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q3 calls python")
  }
}

object Q4Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q4 calls python")
  }
}

object Q5Like {
  def apply(spark: SparkSession): DataFrame = {
    spark.sql(
      """
        |-- TASK:
        |-- Build a model using logistic regression for a visitor to an online store: based on existing users online
        |-- activities (interest in items of different categories) and demographics.
        |-- This model will be used to predict if the visitor is interested in a given item category.
        |-- Output the precision, accuracy and confusion matrix of model.
        |-- Note: no need to actually classify existing users, as it will be later used to predict interests of unknown visitors.
        |
        |-- input vectors to the machine learning algorithm are:
        |--  clicks_in_category BIGINT, -- used as label - number of clicks in specified category "q05_i_category"
        |--  college_education  BIGINT, -- has college education [0,1]
        |--  male               BIGINT, -- isMale [0,1]
        |--  clicks_in_1        BIGINT, -- number of clicks in category id 1
        |--  clicks_in_2        BIGINT, -- number of clicks in category id 2
        |--  clicks_in_3        BIGINT, -- number of clicks in category id 3
        |--  clicks_in_4        BIGINT, -- number of clicks in category id 4
        |--  clicks_in_5        BIGINT, -- number of clicks in category id 5
        |--  clicks_in_6        BIGINT  -- number of clicks in category id 6
        |--  clicks_in_7        BIGINT  -- number of clicks in category id 7
        |
        |SELECT
        |  --wcs_user_sk,
        |  clicks_in_category,
        |  CASE WHEN cd_education_status IN ('Advanced Degree', 'College', '4 yr Degree', '2 yr Degree') THEN 1 ELSE 0 END AS college_education,
        |  CASE WHEN cd_gender = 'M' THEN 1 ELSE 0 END AS male,
        |  clicks_in_1,
        |  clicks_in_2,
        |  clicks_in_3,
        |  clicks_in_4,
        |  clicks_in_5,
        |  clicks_in_6,
        |  clicks_in_7
        |FROM(
        |  SELECT
        |    wcs_user_sk,
        |    SUM( CASE WHEN i_category = 'Books' THEN 1 ELSE 0 END) AS clicks_in_category,
        |    SUM( CASE WHEN i_category_id = 1 THEN 1 ELSE 0 END) AS clicks_in_1,
        |    SUM( CASE WHEN i_category_id = 2 THEN 1 ELSE 0 END) AS clicks_in_2,
        |    SUM( CASE WHEN i_category_id = 3 THEN 1 ELSE 0 END) AS clicks_in_3,
        |    SUM( CASE WHEN i_category_id = 4 THEN 1 ELSE 0 END) AS clicks_in_4,
        |    SUM( CASE WHEN i_category_id = 5 THEN 1 ELSE 0 END) AS clicks_in_5,
        |    SUM( CASE WHEN i_category_id = 6 THEN 1 ELSE 0 END) AS clicks_in_6,
        |    SUM( CASE WHEN i_category_id = 7 THEN 1 ELSE 0 END) AS clicks_in_7
        |  FROM web_clickstreams
        |  INNER JOIN item it ON (wcs_item_sk = i_item_sk
        |                     AND wcs_user_sk IS NOT NULL)
        |  GROUP BY  wcs_user_sk
        |)q05_user_clicks_in_cat
        |INNER JOIN customer ct ON wcs_user_sk = c_customer_sk
        |INNER JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
        |
      """.stripMargin)
  }
}

// use temporary view here
object Q6Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP VIEW IF EXISTS q6_temp_table1")
    spark.sql("DROP VIEW IF EXISTS q6_temp_table2")

    spark.sql(
      """
        |-- TASK:
        |-- Identifies customers shifting their purchase habit from store to web sales.
        |-- Find customers who spend in relation more money in the second year following a given year in the web_sales channel then in the store sales channel.
        |-- Hint: web second_year_total/first_year_total > store second_year_total/first_year_total
        |-- Report customers details: first name, last name, their country of origin, login name and email address) and identify if they are preferred customer, for the top 100 customers with the highest increase in their second year web purchase ratio.
        |-- Implementation notice:
        |-- loosely based on implementation of tpc-ds q4 - Query description in tpcds_1.1.0.pdf does NOT match implementation in tpc-ds qgen\query_templates\query4.tpl
        |-- This version:
        |--    * does not have the catalog_sales table (there is none in our dataset). Web_sales plays the role of catalog_sales.
        |--    * avoids the 4 self joins and replaces them with only one by creating two distinct views with better pre-filters and aggregations for store/web-sales first and second year
        |--    * introduces a more logical sorting by reporting the top 100 customers ranked by their web_sales increase instead of just reporting random 100 customers
        |
        |CREATE TEMPORARY VIEW q6_temp_table1 AS
        |SELECT ss_customer_sk AS customer_sk,
        |       sum( case when (d_year = 2001)   THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) first_year_total,
        |       sum( case when (d_year = 2001+1) THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) second_year_total
        |FROM  store_sales
        |     ,date_dim
        |WHERE ss_sold_date_sk = d_date_sk
        |AND   d_year BETWEEN 2001 AND 2001 +1
        |GROUP BY ss_customer_sk
        |HAVING first_year_total > 0  -- required to avoid division by 0, because later we will divide by this value
        |
      """.stripMargin)

    spark.sql(
      """
        |-- customer web sales
        |CREATE TEMPORARY VIEW q6_temp_table2 AS
        |SELECT ws_bill_customer_sk AS customer_sk ,
        |       sum( case when (d_year = 2001)   THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) first_year_total,
        |       sum( case when (d_year = 2001+1) THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) second_year_total
        |FROM web_sales
        |    ,date_dim
        |WHERE ws_sold_date_sk = d_date_sk
        |AND   d_year BETWEEN 2001 AND 2001 +1
        |GROUP BY ws_bill_customer_sk
        |HAVING first_year_total > 0  -- required to avoid division by 0, because later we will divide by this value
        |
      """.stripMargin)

    spark.sql(
      """
        |SELECT
        |      (web.second_year_total / web.first_year_total) AS web_sales_increase_ratio,
        |      c_customer_sk,
        |      c_first_name,
        |      c_last_name,
        |      c_preferred_cust_flag,
        |      c_birth_country,
        |      c_login,
        |      c_email_address
        |FROM q6_temp_table1 store,
        |     q6_temp_table2 web,
        |     customer c
        |WHERE store.customer_sk = web.customer_sk
        |AND   web.customer_sk = c_customer_sk
        |-- if customer has sales in first year for both store and websales, select him only if web second_year_total/first_year_total ratio is bigger then his store second_year_total/first_year_total ratio.
        |AND   (web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total)
        |ORDER BY
        |  web_sales_increase_ratio DESC,
        |  c_customer_sk,
        |  c_first_name,
        |  c_last_name,
        |  c_preferred_cust_flag,
        |  c_birth_country,
        |  c_login
        |LIMIT 100
        |
      """.stripMargin)
  }
}

object Q7Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP TABLE IF EXISTS q7_temp_table")

    // -- helper table: items with 20% higher then avg prices of product from same category
    spark.sql(
      """
        |-- TASK: (Based, but not equal to tpc-ds q6)
        |-- List top 10 states in descending order with at least 10 customers who during
        |-- a given month bought products with the price tag at least 20% higher than the
        |-- average price of products in the same category.
        |
        |CREATE TABLE q7_temp_table USING parquet as
        |-- "price tag at least 20% higher than the average price of products in the same category."
        |
        |SELECT
        |  k.i_item_sk
        |FROM
        |  item k,
        |(
        |  SELECT
        |    i_category,
        |    AVG(j.i_current_price) * 1.2 AS avg_price
        |  FROM item j
        |  GROUP BY j.i_category
        |) avgCategoryPrice
        |WHERE
        |  avgCategoryPrice.i_category = k.i_category
        |  AND k.i_current_price > avgCategoryPrice.avg_price
        |
        |""".stripMargin)


    spark.sql(
      """
        |
        |SELECT
        |  ca_state,
        |  COUNT(*) AS cnt
        |FROM
        |  customer_address a,
        |  customer c,
        |  store_sales s,
        |  q7_temp_table highPriceItems
        |WHERE a.ca_address_sk = c.c_current_addr_sk
        |AND c.c_customer_sk = s.ss_customer_sk
        |AND ca_state IS NOT NULL
        |AND ss_item_sk = highPriceItems.i_item_sk --cannot use "ss_item_sk IN ()". Hive only supports a single "IN" subquery per SQL statement.
        |AND s.ss_sold_date_sk
        |IN
        |( --during a given month
        |  SELECT d_date_sk
        |  FROM date_dim
        |  WHERE d_year = 2004
        |  AND d_moy = 7
        |)
        |GROUP BY ca_state
        |HAVING cnt >= 10 --at least 10 customers
        |ORDER BY cnt DESC, ca_state --top 10 states in descending order
        |LIMIT 10
        |
        |""".stripMargin)
  }
}

object Q8Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q8 calls python")
  }
}

object Q9Like {
  def apply(spark: SparkSession): DataFrame = {
    spark.sql(
      """
        |-- Aggregate total amount of sold items over different given types of combinations of customers based on selected groups of
        |-- marital status, education status, sales price  and   different combinations of state and sales profit.
        |
        |SELECT SUM(ss1.ss_quantity)
        |FROM store_sales ss1, date_dim dd,customer_address ca1, store s, customer_demographics cd
        |-- select date range
        |WHERE ss1.ss_sold_date_sk = dd.d_date_sk
        |AND dd.d_year=2001
        |AND ss1.ss_addr_sk = ca1.ca_address_sk
        |AND s.s_store_sk = ss1.ss_store_sk
        |AND cd.cd_demo_sk = ss1.ss_cdemo_sk
        |AND
        |(
        |  (
        |    cd.cd_marital_status = 'M'
        |    AND cd.cd_education_status = '4 yr Degree'
        |    AND 100 <= ss1.ss_sales_price
        |    AND ss1.ss_sales_price <= 150
        |  )
        |  OR
        |  (
        |    cd.cd_marital_status = 'M'
        |    AND cd.cd_education_status = '4 yr Degree'
        |    AND 50 <= ss1.ss_sales_price
        |    AND ss1.ss_sales_price <= 200
        |  )
        |  OR
        |  (
        |    cd.cd_marital_status = 'M'
        |    AND cd.cd_education_status = '4 yr Degree'
        |    AND 150 <= ss1.ss_sales_price
        |    AND ss1.ss_sales_price <= 200
        |  )
        |)
        |AND
        |(
        |  (
        |    ca1.ca_country = 'United States'
        |    AND ca1.ca_state IN ('KY', 'GA', 'NM')
        |    AND 0 <= ss1.ss_net_profit
        |    AND ss1.ss_net_profit <= 2000
        |  )
        |  OR
        |  (
        |    ca1.ca_country = 'United States'
        |    AND ca1.ca_state IN ('MT', 'OR', 'IN')
        |    AND 150 <= ss1.ss_net_profit
        |    AND ss1.ss_net_profit <= 3000
        |  )
        |  OR
        |  (
        |    ca1.ca_country = 'United States'
        |    AND ca1.ca_state IN ('WI', 'MO', 'WV')
        |    AND 50 <= ss1.ss_net_profit
        |    AND ss1.ss_net_profit <= 25000
        |  )
        |)
        |""".stripMargin)
  }
}


//
// Query 10 sets the following hive optimization configs that we need to investigate more:
// -- This query requires parallel order by for fast and deterministic global ordering of final result
// set hive.optimize.sampling.orderby=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby};
// set hive.optimize.sampling.orderby.number=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby.number};
// set hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby.percent};
// --debug print
// set hive.optimize.sampling.orderby;
// set hive.optimize.sampling.orderby.number;
// set hive.optimize.sampling.orderby.percent;
object Q10Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q10 uses UDF")
  }
}

object Q11Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- For a given product, measure the correlation of sentiments, including
        |-- the number of reviews and average review ratings, on product monthly revenues
        |-- within a given time frame.
        |
        |SELECT corr(reviews_count,avg_rating)
        |FROM (
        |  SELECT
        |    p.pr_item_sk AS pid,
        |    p.r_count    AS reviews_count,
        |    p.avg_rating AS avg_rating,
        |    s.revenue    AS m_revenue
        |  FROM (
        |    SELECT
        |      pr_item_sk,
        |      count(*) AS r_count,
        |      avg(pr_review_rating) AS avg_rating
        |    FROM product_reviews
        |    WHERE pr_item_sk IS NOT NULL
        |    --this is GROUP BY 1 in original::same as pr_item_sk here::hive complains anyhow
        |    GROUP BY pr_item_sk
        |  ) p
        |  INNER JOIN (
        |    SELECT
        |      ws_item_sk,
        |      SUM(ws_net_paid) AS revenue
        |    FROM web_sales ws
        |    -- Select date range of interest
        |    LEFT SEMI JOIN (
        |      SELECT d_date_sk
        |      FROM date_dim d
        |      WHERE d.d_date >= '2003-01-02'
        |      AND   d.d_date <= '2003-02-02'
        |    ) dd ON ( ws.ws_sold_date_sk = dd.d_date_sk )
        |    WHERE ws_item_sk IS NOT null
        |    --this is GROUP BY 1 in original::same as ws_item_sk here::hive complains anyhow
        |    GROUP BY ws_item_sk
        |  ) s
        |  ON p.pr_item_sk = s.ws_item_sk
        |) q11_review_stats
        |""".stripMargin)
  }
}

// query had the following set but we aren't using Hive so shouldn't matter
//
// -- This query requires parallel order by for fast and deterministic global ordering of final result
// set hive.optimize.sampling.orderby=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby};
// set hive.optimize.sampling.orderby.number=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby.number};
// set hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby.percent};
// --debug print
// set hive.optimize.sampling.orderby;
// set hive.optimize.sampling.orderby.number;
// set hive.optimize.sampling.orderby.percent;
object Q12Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- Find all customers who viewed items of a given category on the web
        |-- in a given month and year that was followed by an in-store purchase of an item from the same category in the three
        |-- consecutive months.
        |
        |SELECT DISTINCT wcs_user_sk -- Find all customers
        |-- TODO check if 37134 is first day of the month
        |FROM
        |( -- web_clicks viewed items in date range with items from specified categories
        |  SELECT
        |    wcs_user_sk,
        |    wcs_click_date_sk
        |  FROM web_clickstreams, item
        |  WHERE wcs_click_date_sk BETWEEN 37134 AND (37134 + 30) -- in a given month and year
        |  AND i_category IN ('Books', 'Electronics') -- filter given category
        |  AND wcs_item_sk = i_item_sk
        |  AND wcs_user_sk IS NOT NULL
        |  AND wcs_sales_sk IS NULL --only views, not purchases
        |) webInRange,
        |(  -- store sales in date range with items from specified categories
        |  SELECT
        |    ss_customer_sk,
        |    ss_sold_date_sk
        |  FROM store_sales, item
        |  WHERE ss_sold_date_sk BETWEEN 37134 AND (37134 + 90) -- in the three consecutive months.
        |  AND i_category IN ('Books', 'Electronics') -- filter given category
        |  AND ss_item_sk = i_item_sk
        |  AND ss_customer_sk IS NOT NULL
        |) storeInRange
        |-- join web and store
        |WHERE wcs_user_sk = ss_customer_sk
        |AND wcs_click_date_sk < ss_sold_date_sk -- buy AFTER viewed on website
        |ORDER BY wcs_user_sk
        |
        |""".stripMargin)
  }
}

object Q13Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP VIEW IF EXISTS q13_temp_table1")
    spark.sql("DROP VIEW IF EXISTS q13_temp_table2")

    // used temporary view here instead of permanent view
    spark.sql(
      """
        |-- based on tpc-ds q74
        |-- Display customers with both store and web sales in
        |-- consecutive years for whom the increase in web sales exceeds the increase in
        |-- store sales for a specified year.
        |
        |-- Implementation notice:
        |-- loosely based on implementation of tpc-ds q74 - Query description in tpcds_1.1.0.pdf does NOT match implementation in tpc-ds qgen\query_templates\query74.tpl
        |-- This version:
        |--    * avoids union of 2 sub-queries followed by 4 self joins and replaces them with only one join by creating two distinct views with better pre-filters and aggregations for store/web-sales first and second year
        |--    * introduces a more logical sorting by reporting the top 100 customers ranked by their web_sales increase ratio instead of just reporting random 100 customers
        |
        |CREATE TEMPORARY VIEW q13_temp_table1 AS
        |SELECT
        |    ss.ss_customer_sk AS customer_sk,
        |    sum( case when (d_year = 2001)   THEN ss_net_paid  ELSE 0 END) first_year_total,
        |    sum( case when (d_year = 2001+1) THEN ss_net_paid  ELSE 0 END) second_year_total
        |FROM store_sales ss
        |JOIN (
        |  SELECT d_date_sk, d_year
        |  FROM date_dim d
        |  WHERE d.d_year in (2001, (2001 + 1))
        |) dd on ( ss.ss_sold_date_sk = dd.d_date_sk )
        |GROUP BY ss.ss_customer_sk
        |HAVING first_year_total > 0
        |
      """.stripMargin)

    // used temporary view here instead of permanent view
    spark.sql(
      """
        |CREATE TEMPORARY VIEW q13_temp_table2 AS
        |SELECT
        |       ws.ws_bill_customer_sk AS customer_sk,
        |       sum( case when (d_year = 2001)   THEN ws_net_paid  ELSE 0 END) first_year_total,
        |       sum( case when (d_year = 2001+1) THEN ws_net_paid  ELSE 0 END) second_year_total
        |FROM web_sales ws
        |JOIN (
        |  SELECT d_date_sk, d_year
        |  FROM date_dim d
        |  WHERE d.d_year in (2001, (2001 + 1) )
        |) dd ON ( ws.ws_sold_date_sk = dd.d_date_sk )
        |GROUP BY ws.ws_bill_customer_sk
        |HAVING first_year_total > 0
        |
      """.stripMargin)

    spark.sql(
      """
        |SELECT
        |      c_customer_sk,
        |      c_first_name,
        |      c_last_name,
        |      (store.second_year_total / store.first_year_total) AS storeSalesIncreaseRatio ,
        |      (web.second_year_total / web.first_year_total) AS webSalesIncreaseRatio
        |FROM q13_temp_table1 store ,
        |     q13_temp_table2 web ,
        |     customer c
        |WHERE store.customer_sk = web.customer_sk
        |AND   web.customer_sk = c_customer_sk
        |-- if customer has sales in first year for both store and websales, select him only if web second_year_total/first_year_total ratio is bigger then his store second_year_total/first_year_total ratio.
        |AND   (web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total)
        |ORDER BY
        |  webSalesIncreaseRatio DESC,
        |  c_customer_sk,
        |  c_first_name,
        |  c_last_name
        |LIMIT 100
        |
        |""".stripMargin)
  }
}

object Q14Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- based on tpc-ds q90
        |-- What is the ratio between the number of items sold over
        |-- the internet in the morning (7 to 8am) to the number of items sold in the evening
        |-- (7 to 8pm) of customers with a specified number of dependents. Consider only
        |-- websites with a high amount of content.
        |
        |SELECT CASE WHEN pmc > 0 THEN amc/pmc  ELSE -1.00 END AS am_pm_ratio
        | FROM (
        |        SELECT SUM(amc1) AS amc, SUM(pmc1) AS pmc
        |        FROM(
        |         SELECT
        |         CASE WHEN t_hour BETWEEN  7 AND 8 THEN COUNT(1) ELSE 0 END AS amc1,
        |         CASE WHEN t_hour BETWEEN 19 AND 20 THEN COUNT(1) ELSE 0 END AS pmc1
        |          FROM web_sales ws
        |          JOIN household_demographics hd ON (hd.hd_demo_sk = ws.ws_ship_hdemo_sk and hd.hd_dep_count = 5)
        |          JOIN web_page wp ON (wp.wp_web_page_sk = ws.ws_web_page_sk and wp.wp_char_count BETWEEN 5000 AND 6000 )
        |          JOIN time_dim td ON (td.t_time_sk = ws.ws_sold_time_sk and td.t_hour IN (7,8,19,20))
        |          GROUP BY t_hour) cnt_am_pm
        |  ) sum_am_pm
        |
        |""".stripMargin)
  }
}

object Q15Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- Find the categories with flat or declining sales for in store purchases
        |-- during a given year for a given store.
        |
        |SELECT *
        |FROM (
        |  SELECT
        |    cat,
        |    --input:
        |    --SUM(x)as sumX,
        |    --SUM(y)as sumY,
        |    --SUM(xy)as sumXY,
        |    --SUM(xx)as sumXSquared,
        |    --count(x) as N,
        |
        |    --formula stage1 (logical):
        |    --N * sumXY - sumX * sumY AS numerator,
        |    --N * sumXSquared - sumX*sumX AS denom
        |    --numerator / denom as slope,
        |    --(sumY - slope * sumX) / N as intercept
        |    --
        |    --formula stage2(inserted hive aggregations):
        |    --(count(x) * SUM(xy) - SUM(x) * SUM(y)) AS numerator,
        |    --(count(x) * SUM(xx) - SUM(x) * SUM(x)) AS denom
        |    --numerator / denom as slope,
        |    --(sum(y) - slope * sum(x)) / count(X) as intercept
        |    --
        |    --Formula stage 3: (insert numerator and denom into slope and intercept function)
        |    ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x) * SUM(x)) ) AS slope,
        |    (SUM(y) - ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x)*SUM(x)) ) * SUM(x)) / count(x) AS intercept
        |  FROM (
        |    SELECT
        |      i.i_category_id AS cat, -- ranges from 1 to 10
        |      s.ss_sold_date_sk AS x,
        |      SUM(s.ss_net_paid) AS y,
        |      s.ss_sold_date_sk * SUM(s.ss_net_paid) AS xy,
        |      s.ss_sold_date_sk * s.ss_sold_date_sk AS xx
        |    FROM store_sales s
        |    -- select date range
        |    LEFT SEMI JOIN (
        |      SELECT d_date_sk
        |      FROM date_dim d
        |      WHERE d.d_date >= '2001-09-02'
        |      AND   d.d_date <= '2002-09-02'
        |    ) dd ON ( s.ss_sold_date_sk=dd.d_date_sk )
        |    INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
        |    WHERE i.i_category_id IS NOT NULL
        |    AND s.ss_store_sk = 10 -- for a given store ranges from 1 to 12
        |    GROUP BY i.i_category_id, s.ss_sold_date_sk
        |  ) temp
        |  GROUP BY cat
        |) regression
        |WHERE slope <= 0
        |ORDER BY cat
        |-- limit not required, number of categories is known to be small and of fixed size across scalefactors
        |
        |""".stripMargin)
  }
}

object Q16Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- based on tpc-ds q40
        |-- Compute the impact of an item price change on the
        |-- store sales by computing the total sales for items in a 30 day period before and
        |-- after the price change. Group the items by location of warehouse where they
        |-- were delivered from.
        |
        |SELECT w_state, i_item_id,
        |  SUM(
        |    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') < unix_timestamp('2001-03-16','yyyy-MM-dd'))
        |    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
        |    ELSE 0.0 END
        |  ) AS sales_before,
        |  SUM(
        |    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') >= unix_timestamp('2001-03-16','yyyy-MM-dd'))
        |    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
        |    ELSE 0.0 END
        |  ) AS sales_after
        |FROM (
        |  SELECT *
        |  FROM web_sales ws
        |  LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
        |    AND ws.ws_item_sk = wr.wr_item_sk)
        |) a1
        |JOIN item i ON a1.ws_item_sk = i.i_item_sk
        |JOIN warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
        |JOIN date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
        |AND unix_timestamp(d.d_date, 'yyyy-MM-dd') >= unix_timestamp('2001-03-16', 'yyyy-MM-dd') - 30*24*60*60 --subtract 30 days in seconds
        |AND unix_timestamp(d.d_date, 'yyyy-MM-dd') <= unix_timestamp('2001-03-16', 'yyyy-MM-dd') + 30*24*60*60 --add 30 days in seconds
        |GROUP BY w_state,i_item_id
        |--original was ORDER BY w_state,i_item_id , but CLUSTER BY is hives cluster scale counter part
        |ORDER BY w_state,i_item_id
        |LIMIT 100
        |
        |""".stripMargin)
  }
}

object Q17Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- based on tpc-ds q61
        |-- Find the ratio of items sold with and without promotions
        |-- in a given month and year. Only items in certain categories sold to customers
        |-- living in a specific time zone are considered.
        |
        |SELECT sum(promotional) as promotional, sum(total) as total,
        |       CASE WHEN sum(total) > 0 THEN 100*sum(promotional)/sum(total)
        |                                ELSE 0.0 END as promo_percent
        |FROM(
        |SELECT p_channel_email, p_channel_dmail, p_channel_tv,
        |CASE WHEN (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
        |THEN SUM(ss_ext_sales_price) ELSE 0 END as promotional,
        |SUM(ss_ext_sales_price) total
        |  FROM store_sales ss
        |  LEFT SEMI JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk AND dd.d_year = 2001 AND dd.d_moy = 12
        |  LEFT SEMI JOIN item i ON ss.ss_item_sk = i.i_item_sk AND i.i_category IN ('Books', 'Music')
        |  LEFT SEMI JOIN store s ON ss.ss_store_sk = s.s_store_sk AND s.s_gmt_offset = -5
        |  LEFT SEMI JOIN ( SELECT c.c_customer_sk FROM customer c LEFT SEMI JOIN customer_address ca
        |                   ON c.c_current_addr_sk = ca.ca_address_sk AND ca.ca_gmt_offset = -5
        |                 ) sub_c ON ss.ss_customer_sk = sub_c.c_customer_sk
        |  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
        |  GROUP BY p_channel_email, p_channel_dmail, p_channel_tv
        |  ) sum_promotional
        |-- we don't need a 'ON' join condition. result is just two numbers.
        |ORDER by promotional, total
        |LIMIT 100 -- kinda useless, result is one line with two numbers, but original tpc-ds query has it too.
        |
        |""".stripMargin)
  }
}

object Q18Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q18 uses UDF")
  }
}

object Q19Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q19 uses UDF")
  }
}


/*
 * query had the following set but we aren't using Hive so shouldn't matter
 *
 * -- This query requires parallel order by for fast and deterministic global ordering of final result
 * set hive.optimize.sampling.orderby=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby};
 * set hive.optimize.sampling.orderby.number=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby.number};
 * set hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.spark.sql.optimize.sampling.orderby.percent};
 * --debug print
 * set hive.optimize.sampling.orderby;
 * set hive.optimize.sampling.orderby.number;
 * set hive.optimize.sampling.orderby.percent;
 */
object Q20Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- TASK:
        |-- Customer segmentation for return analysis: Customers are separated
        |-- along the following dimensions: return frequency, return order ratio (total
        |-- number of orders partially or fully returned versus the total number of orders),
        |-- return item ratio (total number of items returned versus the number of items
        |-- purchased), return amount ration (total monetary amount of items returned versus
        |-- the amount purchased), return order ratio. Consider the store returns during
        |-- a given year for the computation.
        |
        |-- IMPLEMENTATION NOTICE:
        |-- hive provides the input for the clustering program
        |-- The input format for the clustering is:
        |--   user surrogate key,
        |--   order ratio (number of returns / number of orders),
        |--   item ratio (number of returned items / number of ordered items),
        |--   money ratio (returned money / payed money),
        |--   number of returns
        |
        |SELECT
        |  ss_customer_sk AS user_sk,
        |  round(CASE WHEN ((returns_count IS NULL) OR (orders_count IS NULL) OR ((returns_count / orders_count) IS NULL) ) THEN 0.0 ELSE (returns_count / orders_count) END, 7) AS orderRatio,
        |  round(CASE WHEN ((returns_items IS NULL) OR (orders_items IS NULL) OR ((returns_items / orders_items) IS NULL) ) THEN 0.0 ELSE (returns_items / orders_items) END, 7) AS itemsRatio,
        |  round(CASE WHEN ((returns_money IS NULL) OR (orders_money IS NULL) OR ((returns_money / orders_money) IS NULL) ) THEN 0.0 ELSE (returns_money / orders_money) END, 7) AS monetaryRatio,
        |  round(CASE WHEN ( returns_count IS NULL                                                                        ) THEN 0.0 ELSE  returns_count                 END, 0) AS frequency
        |FROM
        |  (
        |    SELECT
        |      ss_customer_sk,
        |      -- return order ratio
        |      COUNT(distinct(ss_ticket_number)) AS orders_count,
        |      -- return ss_item_sk ratio
        |      COUNT(ss_item_sk) AS orders_items,
        |      -- return monetary amount ratio
        |      SUM( ss_net_paid ) AS orders_money
        |    FROM store_sales s
        |    GROUP BY ss_customer_sk
        |  ) orders
        |  LEFT OUTER JOIN
        |  (
        |    SELECT
        |      sr_customer_sk,
        |      -- return order ratio
        |      count(distinct(sr_ticket_number)) as returns_count,
        |      -- return ss_item_sk ratio
        |      COUNT(sr_item_sk) as returns_items,
        |      -- return monetary amount ratio
        |      SUM( sr_return_amt ) AS returns_money
        |    FROM store_returns
        |    GROUP BY sr_customer_sk
        |  ) returned ON ss_customer_sk=sr_customer_sk
        |ORDER BY user_sk
        |
        |
        |""".stripMargin)
  }
}

object Q21Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- based on tpc-ds q29
        |-- Get all items that were sold in stores in a given month
        |-- and year and which were returned in the next 6 months and re-purchased by
        |-- the returning customer afterwards through the web sales channel in the following
        |-- three years. For those items, compute the total quantity sold through the
        |-- store, the quantity returned and the quantity purchased through the web. Group
        |-- this information by item and store.
        |
        |SELECT
        |  part_i.i_item_id AS i_item_id,
        |  part_i.i_item_desc AS i_item_desc,
        |  part_s.s_store_id AS s_store_id,
        |  part_s.s_store_name AS s_store_name,
        |  SUM(part_ss.ss_quantity) AS store_sales_quantity,
        |  SUM(part_sr.sr_return_quantity) AS store_returns_quantity,
        |  SUM(part_ws.ws_quantity) AS web_sales_quantity
        |FROM (
        |        SELECT
        |          sr_item_sk,
        |          sr_customer_sk,
        |          sr_ticket_number,
        |          sr_return_quantity
        |        FROM
        |          store_returns sr,
        |          date_dim d2
        |        WHERE d2.d_year = 2003
        |        AND d2.d_moy BETWEEN 1 AND 1 + 6 --which were returned in the next six months
        |        AND sr.sr_returned_date_sk = d2.d_date_sk
        |) part_sr
        |INNER JOIN (
        |  SELECT
        |    ws_item_sk,
        |    ws_bill_customer_sk,
        |    ws_quantity
        |  FROM
        |    web_sales ws,
        |    date_dim d3
        |  WHERE d3.d_year BETWEEN 2003 AND 2003 + 2 -- in the following three years (re-purchased by the returning customer afterwards through the web sales channel)
        |  AND ws.ws_sold_date_sk = d3.d_date_sk
        |) part_ws ON (
        |  part_sr.sr_item_sk = part_ws.ws_item_sk
        |  AND part_sr.sr_customer_sk = part_ws.ws_bill_customer_sk
        |)
        |INNER JOIN (
        |  SELECT
        |    ss_item_sk,
        |    ss_store_sk,
        |    ss_customer_sk,
        |    ss_ticket_number,
        |    ss_quantity
        |  FROM
        |    store_sales ss,
        |    date_dim d1
        |  WHERE d1.d_year = 2003
        |  AND d1.d_moy = 1
        |  AND ss.ss_sold_date_sk = d1.d_date_sk
        |) part_ss ON (
        |  part_ss.ss_ticket_number = part_sr.sr_ticket_number
        |  AND part_ss.ss_item_sk = part_sr.sr_item_sk
        |  AND part_ss.ss_customer_sk = part_sr.sr_customer_sk
        |)
        |INNER JOIN store part_s ON (
        |  part_s.s_store_sk = part_ss.ss_store_sk
        |)
        |INNER JOIN item part_i ON (
        |  part_i.i_item_sk = part_ss.ss_item_sk
        |)
        |GROUP BY
        |  part_i.i_item_id,
        |  part_i.i_item_desc,
        |  part_s.s_store_id,
        |  part_s.s_store_name
        |ORDER BY
        |  part_i.i_item_id,
        |  part_i.i_item_desc,
        |  part_s.s_store_id,
        |  part_s.s_store_name
        |LIMIT 100
        |
        |""".stripMargin)
  }
}

object Q22Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- based on tpc-ds q21
        |-- For all items whose price was changed on a given date,
        |-- compute the percentage change in inventory between the 30-day period BEFORE
        |-- the price change and the 30-day period AFTER the change. Group this
        |-- information by warehouse.
        |SELECT
        |  w_warehouse_name,
        |  i_item_id,
        |  SUM( CASE WHEN datediff(d_date, '2001-05-08') < 0
        |    THEN inv_quantity_on_hand
        |    ELSE 0 END
        |  ) AS inv_before,
        |  SUM( CASE WHEN datediff(d_date, '2001-05-08') >= 0
        |    THEN inv_quantity_on_hand
        |    ELSE 0 END
        |  ) AS inv_after
        |FROM inventory inv,
        |  item i,
        |  warehouse w,
        |  date_dim d
        |WHERE i_current_price BETWEEN 0.98 AND 1.5
        |AND i_item_sk        = inv_item_sk
        |AND inv_warehouse_sk = w_warehouse_sk
        |AND inv_date_sk      = d_date_sk
        |AND datediff(d_date, '2001-05-08') >= -30
        |AND datediff(d_date, '2001-05-08') <= 30
        |
        |GROUP BY w_warehouse_name, i_item_id
        |HAVING inv_before > 0
        |AND inv_after / inv_before >= 2.0 / 3.0
        |AND inv_after / inv_before <= 3.0 / 2.0
        |ORDER BY w_warehouse_name, i_item_id
        |LIMIT 100
        |
        |""".stripMargin)
  }
}

/*
 * query had the following set but we aren't using Hive so shouldn't matter
 *
 * -- This query requires parallel order by for fast and deterministic global ordering of final result
 * set hive.optimize.sampling.orderby=true;
 * set hive.optimize.sampling.orderby.number=20000;
 * set hive.optimize.sampling.orderby.percent=0.1;
 * --debug print
 * set hive.optimize.sampling.orderby;
 * set hive.optimize.sampling.orderby.number;
 * set hive.optimize.sampling.orderby.percent;
 */
object Q23Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP TABLE IF EXISTS q23_temp_table")

    spark.sql(
      """
        |-- based on tpc-ds q39
        |-- This query contains multiple, related iterations:
        |-- Iteration 1: Calculate the coefficient of variation and mean of every item
        |-- and warehouse of the given and the consecutive month
        |-- Iteration 2: Find items that had a coefficient of variation of 1.3 or larger
        |-- in the given and the consecutive month
        |
        |CREATE TABLE q23_temp_table USING parquet AS
        |SELECT
        |  inv_warehouse_sk,
        | -- w_warehouse_name,
        |  inv_item_sk,
        |  d_moy,
        |  cast( ( stdev / mean ) as decimal(15,5)) cov
        |FROM (
        |   --Iteration 1: Calculate the coefficient of variation and mean of every item
        |   -- and warehouse of the given and the consecutive month
        |  SELECT
        |    inv_warehouse_sk,
        |    inv_item_sk,
        |    d_moy,
        |    -- implicit group by d_moy using CASE filters inside the stddev_samp() and avg() UDF's. This saves us from requiring a self join for correlation of d_moy and d_moy+1 later on.
        |    cast( stddev_samp( inv_quantity_on_hand ) as decimal(15,5)) stdev,
        |    cast(         avg( inv_quantity_on_hand ) as decimal(15,5)) mean
        |
        |  FROM inventory inv
        |  JOIN date_dim d
        |       ON (inv.inv_date_sk = d.d_date_sk
        |       AND d.d_year = 2001
        |       AND d_moy between 1 AND (1 + 1)
        |       )
        |  GROUP BY
        |    inv_warehouse_sk,
        |    inv_item_sk,
        |    d_moy
        |) q23_tmp_inv_part
        |--JOIN warehouse w ON inv_warehouse_sk = w.w_warehouse_sk
        |WHERE mean > 0 --avoid "div by 0"
        |  AND stdev/mean >= 1.3
        |
      """.stripMargin)

    spark.sql(
      """
        |-- Begin: the real query part
        |-- Iteration 2: Find items that had a coefficient of variation of 1.5 or larger
        |-- in the given and the consecutive month
        |SELECT
        |  inv1.inv_warehouse_sk,
        |  inv1.inv_item_sk,
        |  inv1.d_moy,
        |  inv1.cov,
        |  inv2.d_moy,
        |  inv2.cov
        |FROM q23_temp_table inv1
        |JOIN q23_temp_table inv2
        |    ON(   inv1.inv_warehouse_sk=inv2.inv_warehouse_sk
        |      AND inv1.inv_item_sk =  inv2.inv_item_sk
        |      AND inv1.d_moy = 1
        |      AND inv2.d_moy = 1 + 1
        |    )
        |ORDER BY
        | inv1.inv_warehouse_sk,
        | inv1.inv_item_sk
        |
        |""".stripMargin)
  }
}

object Q24Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP TABLE IF EXISTS q24_temp_table")

    spark.sql(
      """
        |--For a given product, measure the effect of competitor's prices on
        |--products' in-store and online sales. (Compute the cross-price elasticity of demand
        |--for a given product.)
        |-- Step1 :
        |--Calculating the Percentage Change in Quantity Demanded of Good X : [QDemand(NEW) - QDemand(OLD)] / QDemand(OLD)
        |--Step 2:
        |-- Calculating the Percentage Change in Price of Good Y: [Price(NEW) - Price(OLD)] / Price(OLD)
        |-- Step 3 final:
        |--Cross-Price Elasticity of Demand (CPEoD) is given by: CPEoD = (% Change in Quantity Demand for Good X)/(% Change in Price for Good Y))
        |
        |-- compute the price change % for the competitor items
        |-- will give a list of competitor prices changes
        |
        |CREATE TABLE q24_temp_table USING parquet AS
        |SELECT
        |  i_item_sk,
        |  imp_sk,
        |  --imp_competitor,
        |  (imp_competitor_price - i_current_price)/i_current_price AS price_change,
        |  imp_start_date,
        |  (imp_end_date - imp_start_date) AS no_days_comp_price
        |FROM item i ,item_marketprices imp
        |WHERE i.i_item_sk = imp.imp_item_sk
        |AND i.i_item_sk = 10000
        |-- AND imp.imp_competitor_price < i.i_current_price --consider all price changes not just where competitor is cheaper
        |ORDER BY i_item_sk,
        |         imp_sk,
        |         --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
        |         imp_start_date
        |
        |
        |""".stripMargin)

    spark.sql(
      """
        |SELECT ws_item_sk,
        |       --ws.imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
        |       avg ( (current_ss_quant + current_ws_quant - prev_ss_quant - prev_ws_quant) / ((prev_ss_quant + prev_ws_quant) * ws.price_change)) AS cross_price_elasticity
        |FROM
        |    ( --websales items sold quantity before and after competitor price change
        |      SELECT
        |        ws_item_sk,
        |        imp_sk,
        |        --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
        |        price_change,
        |        SUM( CASE WHEN  ( (ws_sold_date_sk >= c.imp_start_date) AND (ws_sold_date_sk < (c.imp_start_date + c.no_days_comp_price))) THEN ws_quantity ELSE 0 END ) AS current_ws_quant,
        |        SUM( CASE WHEN  ( (ws_sold_date_sk >= (c.imp_start_date - c.no_days_comp_price)) AND (ws_sold_date_sk < c.imp_start_date)) THEN ws_quantity ELSE 0 END ) AS prev_ws_quant
        |      FROM web_sales ws
        |      JOIN q24_temp_table c ON ws.ws_item_sk = c.i_item_sk
        |      GROUP BY ws_item_sk,
        |              imp_sk,
        |              --imp_competitor,
        |              price_change
        |    ) ws
        |JOIN
        |    (--storesales items sold quantity before and after competitor price change
        |      SELECT
        |        ss_item_sk,
        |        imp_sk,
        |        --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
        |        price_change,
        |        SUM( CASE WHEN ((ss_sold_date_sk >= c.imp_start_date) AND (ss_sold_date_sk < (c.imp_start_date + c.no_days_comp_price))) THEN ss_quantity ELSE 0 END) AS current_ss_quant,
        |        SUM( CASE WHEN ((ss_sold_date_sk >= (c.imp_start_date - c.no_days_comp_price)) AND (ss_sold_date_sk < c.imp_start_date)) THEN ss_quantity ELSE 0 END) AS prev_ss_quant
        |      FROM store_sales ss
        |      JOIN q24_temp_table c ON c.i_item_sk = ss.ss_item_sk
        |      GROUP BY ss_item_sk,
        |              imp_sk,
        |              --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
        |              price_change
        |    ) ss
        | ON (ws.ws_item_sk = ss.ss_item_sk and ws.imp_sk = ss.imp_sk)
        |GROUP BY  ws.ws_item_sk
        |--uncomment below to compute cross_price_elasticity per competitor is instead of a single number (requires ordering)
        |         --,ws.imp_competitor
        |--ORDER BY ws.ws_item_sk,
        |--         ws.imp_competitor
        |
      """.stripMargin)
  }
}

/*
 * query had the following set but we aren't using Hive so shouldn't matter
 *
 * -- This query requires parallel order by for fast and deterministic global ordering of final result
 * set hive.optimize.sampling.orderby=true;
 * set hive.optimize.sampling.orderby.number=20000;
 * set hive.optimize.sampling.orderby.percent=0.1;
 * --debug print
 * set hive.optimize.sampling.orderby;
 * set hive.optimize.sampling.orderby.number;
 * set hive.optimize.sampling.orderby.percent;
 */
object Q25Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP TABLE IF EXISTS q25_temp_table")

    spark.sql(
      """
        |-- TASK:
        |-- Customer segmentation analysis: Customers are separated along the
        |-- following key shopping dimensions: recency of last visit, frequency of visits and
        |-- monetary amount. Use the store and online purchase data during a given year
        |-- to compute. After model of separation is build,
        |-- report for the analysed customers to which "group" they where assigned
        |
        |-- IMPLEMENTATION NOTICE:
        |-- hive provides the input for the clustering program
        |-- The input format for the clustering is:
        |--   customer ID,
        |--   flag if customer bought something within the last 60 days (integer 0 or 1),
        |--   number of orders,
        |--   total amount spent
        |CREATE TABLE q25_temp_table (
        |  cid               BIGINT,
        |  frequency         BIGINT,
        |  most_recent_date  BIGINT,
        |  amount            decimal(15,2)
        |) USING parquet
        |
        |""".stripMargin)

    spark.sql(
      """
        |-- Add store sales data
        |INSERT INTO TABLE q25_temp_table
        |SELECT
        |  ss_customer_sk                     AS cid,
        |  count(distinct ss_ticket_number)   AS frequency,
        |  max(ss_sold_date_sk)               AS most_recent_date,
        |  SUM(ss_net_paid)                   AS amount
        |FROM store_sales ss
        |JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        |WHERE d.d_date > '2002-01-02'
        |AND ss_customer_sk IS NOT NULL
        |GROUP BY ss_customer_sk
        |
      """.stripMargin)

    spark.sql(
      """
        |-- Add web sales data
        |INSERT INTO TABLE q25_temp_table
        |SELECT
        |  ws_bill_customer_sk             AS cid,
        |  count(distinct ws_order_number) AS frequency,
        |  max(ws_sold_date_sk)            AS most_recent_date,
        |  SUM(ws_net_paid)                AS amount
        |FROM web_sales ws
        |JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
        |WHERE d.d_date > '2002-01-02'
        |AND ws_bill_customer_sk IS NOT NULL
        |GROUP BY ws_bill_customer_sk
        |
      """.stripMargin)

    spark.sql(
      """
        |SELECT
        |  -- rounding of values not necessary
        |  cid            AS cid,
        |  CASE WHEN 37621 - max(most_recent_date) < 60 THEN 1.0 ELSE 0.0 END
        |                 AS recency, -- 37621 == 2003-01-02
        |  SUM(frequency) AS frequency, --total frequency
        |  SUM(amount)    AS totalspend --total amount
        |FROM q25_temp_table
        |GROUP BY cid
        |--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
        |--no total ordering with ORDER BY required, further processed by clustering algorithm
        |ORDER BY cid
        |
        |
      """.stripMargin)
  }
}

object Q26Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
        |-- TASK:
        |-- Cluster customers into book buddies/club groups based on their in
        |-- store book purchasing histories. After model of separation is build,
        |-- report for the analysed customers to which "group" they where assigned
        |
        |-- IMPLEMENTATION NOTICE:
        |-- hive provides the input for the clustering program
        |-- The input format for the clustering is:
        |--   customer ID,
        |--   sum of store sales in the item class ids [1,15]
        |
        |SELECT
        |  ss.ss_customer_sk AS cid,
        |  count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
        |  count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
        |  count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
        |  count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
        |  count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
        |  count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
        |  count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
        |  count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
        |  count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
        |  count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
        |  count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
        |  count(CASE WHEN i.i_class_id=12 THEN 1 ELSE NULL END) AS id12,
        |  count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
        |  count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
        |  count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15
        |FROM store_sales ss
        |INNER JOIN item i
        |  ON (ss.ss_item_sk = i.i_item_sk
        |  AND i.i_category IN ('Books')
        |  AND ss.ss_customer_sk IS NOT NULL
        |)
        |GROUP BY ss.ss_customer_sk
        |HAVING count(ss.ss_item_sk) > 5
        |--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
        |ORDER BY cid
        |
        |""".stripMargin)
  }
}

object Q27Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q27 uses UDF")
  }
}

// Not setting hive.optimize.sample.orderby settings
//
// NOTE - this was inserting into 2 different tables, need to figure out what we want to do here
//
// This currently fails on the GPU due to inserts
object Q28Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP TABLE IF EXISTS q28_temp_table1")

    spark.sql(
      """
        |CREATE TABLE q28_temp_table1 (
        |  pr_review_sk      BIGINT,
        |  pr_rating         INT,
        |  pr_review_content STRING
        |) USING parquet
      """.stripMargin)

    spark.sql("DROP TABLE IF EXISTS q28_temp_table2")

    spark.sql(
      """
        |CREATE TABLE q28_temp_table2 (
        |  pr_review_sk      BIGINT,
        |  pr_rating         INT,
        |  pr_review_content STRING
        |) USING parquet
      """.stripMargin)

    spark.sql(
      """
        |-- TASK
        |-- Build text classifier for online review sentiment classification (Positive,
        |-- Negative, Neutral), using 90% of available reviews for training and the remaining
        |-- 40% for testing. Display classifier accuracy on testing data
        |-- and classification result for the 10% testing data: <reviewSK>,<originalRating>,<classificationResult>
        |
        |--Split reviews table into training and testing
        |FROM (
        |  SELECT
        |    pr_review_sk,
        |    pr_review_rating,
        |    pr_review_content
        |  FROM product_reviews
        |  ORDER BY pr_review_sk
        |)p
        |INSERT OVERWRITE TABLE q28_temp_table1
        |  SELECT *
        |  WHERE pmod(pr_review_sk, 10) IN (1,2,3,4,5,6,7,8,9) -- 90% are training
        |
        |INSERT OVERWRITE TABLE q28_temp_table2
        |  SELECT *
        |  WHERE pmod(pr_review_sk, 10) IN (0) -- 10% are testing
        |
        |
        |""".stripMargin)
  }
}

object Q29Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q29 uses UDTF")
  }
}

object Q30Like {
  def apply(spark: SparkSession): DataFrame = {
    throw new UnsupportedOperationException("Q30 uses UDTF")
  }
}

object ConvertFiles {
  /**
   * Main method allows us to submit using spark-submit to perform conversions from CSV to
   * Parquet or Orc.
   */
  def main(arg: Array[String]): Unit = {
    val conf = new FileConversionConf(arg)
    val spark = SparkSession.builder.appName("TPC-xBB Like File Conversion").getOrCreate()
    conf.outputFormat() match {
      case "parquet" =>
        csvToParquet(
          spark,
          conf.input(),
          conf.output(),
          conf.coalesce,
          conf.repartition)
      case "orc" =>
        csvToOrc(
          spark,
          conf.input(),
          conf.output(),
          conf.coalesce,
          conf.repartition)
    }
  }

}

class FileConversionConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = true)
  val output = opt[String](required = true)
  val outputFormat = opt[String](required = true)
  val coalesce = propsLong[Int]("coalesce")
  val repartition = propsLong[Int]("repartition")
  verify()
  BenchUtils.validateCoalesceRepartition(coalesce, repartition)
}

// scalastyle:on line.size.limit
