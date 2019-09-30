/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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

package ai.rapids.sparkexamples.tpcxbb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

// DecimalType to DoubleType, bigint to LongType
object TpcxbbLikeSpark {
  def csvToParquet(spark: SparkSession, basePath: String, baseOutput: String): Unit = {
    readCustomerCSV(spark, basePath + "/customer/").write.parquet(baseOutput + "/customer/")
    readCustomerAddressCSV(spark, basePath + "/customer_address/").write.parquet(baseOutput + "/customer_address/")
    readItemCSV(spark, basePath + "/item/").write.parquet(baseOutput + "/item/")
    readStoreSalesCSV(spark, basePath + "/store_sales/").write.parquet(baseOutput + "/store_sales/")
    readDateDimCSV(spark, basePath + "/date_dim/").write.parquet(baseOutput + "/date_dim/")
    readStoreCSV(spark, basePath + "/store/").write.parquet(baseOutput + "/store/")
    readCustomerDemographicsCSV(spark, basePath + "/customer_demographics/").write.parquet(baseOutput + "/customer_demographics/")
    readReviewsCSV(spark, basePath + "/product_reviews/").write.parquet(baseOutput + "/product_reviews/")
    readWebSalesCSV(spark, basePath + "/web_sales/").write.parquet(baseOutput + "/web_sales/")
    readWebClickStreamsCSV(spark, basePath + "/web_clickstreams/").write.parquet(baseOutput + "/web_clickstreams/")
    readHouseholdDemographicsCSV(spark, basePath + "/household_demographics/").write.parquet(baseOutput + "/household_demographics/")
    readWebPageCSV(spark, basePath + "/web_page/").write.parquet(baseOutput + "/web_page/")
    readTimeDimCSV(spark, basePath + "/time_dim/").write.parquet(baseOutput + "/time_dim/")
    readWebReturnsCSV(spark, basePath + "/web_returns/").write.parquet(baseOutput + "/web_returns/")
    readWarehouseCSV(spark, basePath + "/warehouse/").write.parquet(baseOutput + "/warehouse/")
    readPromotionCSV(spark, basePath + "/promotion/").write.parquet(baseOutput + "/promotion/")
  }

  def csvToOrc(spark: SparkSession, basePath: String, baseOutput: String): Unit = {
    readCustomerCSV(spark, basePath + "/customer/").write.orc(baseOutput + "/customer/")
    readCustomerAddressCSV(spark, basePath + "/customer_address/").write.orc(baseOutput + "/customer_address/")
    readItemCSV(spark, basePath + "/item/").write.orc(baseOutput + "/item/")
    readStoreSalesCSV(spark, basePath + "/store_sales/").write.orc(baseOutput + "/store_sales/")
    readDateDimCSV(spark, basePath + "/date_dim/").write.orc(baseOutput + "/date_dim/")
    readStoreCSV(spark, basePath + "/store/").write.orc(baseOutput + "/store/")
    readCustomerDemographicsCSV(spark, basePath + "/customer_demographics/").write.orc(baseOutput + "/customer_demographics/")
    readReviewsCSV(spark, basePath + "/product_reviews/").write.orc(baseOutput + "/product_reviews/")
    readWebSalesCSV(spark, basePath + "/web_sales/").write.orc(baseOutput + "/web_sales/")
    readWebClickStreamsCSV(spark, basePath + "/web_clickstreams/").write.orc(baseOutput + "/web_clickstreams/")
    readHouseholdDemographicsCSV(spark, basePath + "/household_demographics/").write.orc(baseOutput + "/household_demographics/")
    readWebPageCSV(spark, basePath + "/web_page/").write.orc(baseOutput + "/web_page/")
    readTimeDimCSV(spark, basePath + "/time_dim/").write.orc(baseOutput + "/time_dim/")
    readWebReturnsCSV(spark, basePath + "/web_returns/").write.orc(baseOutput + "/web_returns/")
    readWarehouseCSV(spark, basePath + "/warehouse/").write.orc(baseOutput + "/warehouse/")
    readPromotionCSV(spark, basePath + "/promotion/").write.orc(baseOutput + "/promotion/")
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

}


object Q7Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql("DROP TABLE IF EXISTS q7temp_table")

    // -- helper table: items with 20% higher then avg prices of product from same category
    spark.sql(
      """
        |CREATE TABLE q7temp_table as
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
        |  q7temp_table highPriceItems
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

object Q9Like {
  def apply(spark: SparkSession): DataFrame = {
    spark.sql(
      """
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

/*
object Q10Like {
  def apply(spark: SparkSession): DataFrame = {

    // TOOD - requires UDF
    // CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF';
    spark.sql(
      """
        |SELECT item_sk, review_sentence, sentiment, sentiment_word
        |FROM (--wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
        |  SELECT extract_sentiment(pr_item_sk, pr_review_content) AS (item_sk, review_sentence, sentiment, sentiment_word)
        |  FROM product_reviews
        |) extracted
        |ORDER BY item_sk,review_sentence,sentiment,sentiment_word
        |
        |""".stripMargin)
  }
}
*/

object Q11Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
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

object Q12Like {
  def apply(spark: SparkSession): DataFrame = {

    spark.sql(
      """
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