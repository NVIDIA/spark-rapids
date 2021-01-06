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

package com.nvidia.spark.rapids.tests.tpch

import com.nvidia.spark.rapids.tests.common.BenchUtils
import com.nvidia.spark.rapids.tests.tpch.TpchLikeSpark.{csvToOrc, csvToParquet}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit

object TpchLikeSpark {
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
      coalesce: Map[String, Int] = Map.empty,
      repartition: Map[String, Int] = Map.empty): Unit = {
    setupWrite(readOrdersCSV(spark, basePath + "/orders.tbl"), "orders", coalesce, repartition).parquet(baseOutput + "/orders.tbl")
    setupWrite(readLineitemCSV(spark, basePath + "/lineitem.tbl"), "lineitem", coalesce, repartition).parquet(baseOutput + "/lineitem.tbl")
    setupWrite(readCustomerCSV(spark, basePath + "/customer.tbl"), "customers", coalesce, repartition).parquet(baseOutput + "/customer.tbl")
    setupWrite(readNationCSV(spark, basePath + "/nation.tbl"), "nation", coalesce, repartition).parquet(baseOutput + "/nation.tbl")
    setupWrite(readPartCSV(spark, basePath + "/part.tbl"), "part", coalesce, repartition).parquet(baseOutput + "/part.tbl")
    setupWrite(readPartsuppCSV(spark, basePath + "/partsupp.tbl"), "partsupp", coalesce, repartition).parquet(baseOutput + "/partsupp.tbl")
    setupWrite(readRegionCSV(spark, basePath + "/region.tbl"), "region", coalesce, repartition).parquet(baseOutput + "/region.tbl")
    setupWrite(readSupplierCSV(spark, basePath + "/supplier.tbl"), "supplier", coalesce, repartition).parquet(baseOutput + "/supplier.tbl")
  }

  def csvToOrc(
      spark: SparkSession, 
      basePath: String, 
      baseOutput: String,
      coalesce: Map[String, Int] = Map.empty,
      repartition: Map[String, Int] = Map.empty): Unit = {
    setupWrite(readOrdersCSV(spark, basePath + "/orders.tbl"), "orders", coalesce, repartition).orc(baseOutput + "/orders.tbl")
    setupWrite(readLineitemCSV(spark, basePath + "/lineitem.tbl"), "lineitem", coalesce, repartition).orc(baseOutput + "/lineitem.tbl")
    setupWrite(readCustomerCSV(spark, basePath + "/customer.tbl"), "customers", coalesce, repartition).orc(baseOutput + "/customer.tbl")
    setupWrite(readNationCSV(spark, basePath + "/nation.tbl"), "nation", coalesce, repartition).orc(baseOutput + "/nation.tbl")
    setupWrite(readPartCSV(spark, basePath + "/part.tbl"), "part", coalesce, repartition).orc(baseOutput + "/part.tbl")
    setupWrite(readPartsuppCSV(spark, basePath + "/partsupp.tbl"), "partsupp", coalesce, repartition).orc(baseOutput + "/partsupp.tbl")
    setupWrite(readRegionCSV(spark, basePath + "/region.tbl"), "region", coalesce, repartition).orc(baseOutput + "/region.tbl")
    setupWrite(readSupplierCSV(spark, basePath + "/supplier.tbl"), "supplier", coalesce, repartition).orc(baseOutput + "/supplier.tbl")
  }

  def setupAllCSV(spark: SparkSession, basePath: String): Unit = {
    setupOrdersCSV(spark, basePath + "/orders.tbl")
    setupLineitemCSV(spark, basePath + "/lineitem.tbl")
    setupCustomerCSV(spark, basePath + "/customer.tbl")
    setupNationCSV(spark, basePath + "/nation.tbl")
    setupPartCSV(spark, basePath + "/part.tbl")
    setupPartsuppCSV(spark, basePath + "/partsupp.tbl")
    setupRegionCSV(spark, basePath + "/region.tbl")
    setupSupplierCSV(spark, basePath + "/supplier.tbl")
  }

  def setupAllParquet(spark: SparkSession, basePath: String): Unit = {
    setupOrdersParquet(spark, basePath + "/orders.tbl")
    setupLineitemParquet(spark, basePath + "/lineitem.tbl")
    setupCustomerParquet(spark, basePath + "/customer.tbl")
    setupNationParquet(spark, basePath + "/nation.tbl")
    setupPartParquet(spark, basePath + "/part.tbl")
    setupPartsuppParquet(spark, basePath + "/partsupp.tbl")
    setupRegionParquet(spark, basePath + "/region.tbl")
    setupSupplierParquet(spark, basePath + "/supplier.tbl")
  }

  def setupAllOrc(spark: SparkSession, basePath: String): Unit = {
    setupOrdersOrc(spark, basePath + "/orders.tbl")
    setupLineitemOrc(spark, basePath + "/lineitem.tbl")
    setupCustomerOrc(spark, basePath + "/customer.tbl")
    setupNationOrc(spark, basePath + "/nation.tbl")
    setupPartOrc(spark, basePath + "/part.tbl")
    setupPartsuppOrc(spark, basePath + "/partsupp.tbl")
    setupRegionOrc(spark, basePath + "/region.tbl")
    setupSupplierOrc(spark, basePath + "/supplier.tbl")
  }

  // ORDERS
  val ordersSchema = StructType(Array(
    StructField("o_orderkey", LongType),
    StructField("o_custkey", LongType),
    StructField("o_orderstatus", StringType),
    StructField("o_totalprice", DoubleType),
    StructField("o_orderdate", DateType),
    StructField("o_orderpriority", StringType),
    StructField("o_clerk", StringType),
    StructField("o_shippriority", LongType),
    StructField("o_comment", StringType)
  ))

  def readOrdersCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(ordersSchema).csv(path)

  def setupOrdersCSV(spark: SparkSession, path: String): Unit =
    readOrdersCSV(spark, path).createOrReplaceTempView("orders")

  def setupOrdersParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("orders")

  def setupOrdersOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("orders")

  // LINEITEM
  val lineitemSchema = StructType(Array(
    StructField("l_orderkey", LongType),
    StructField("l_partkey", LongType),
    StructField("l_suppkey", LongType),
    StructField("l_linenumber", LongType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType),
    StructField("l_returnflag", StringType),
    StructField("l_linestatus", StringType),
    StructField("l_shipdate", DateType),
    StructField("l_commitdate", DateType),
    StructField("l_receiptdate", DateType),
    StructField("l_shipinstruct", StringType),
    StructField("l_shipmode", StringType),
    StructField("l_comment", StringType)
  ))

  def readLineitemCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(lineitemSchema).csv(path)

  def setupLineitemCSV(spark: SparkSession, path: String): Unit =
    readLineitemCSV(spark, path).createOrReplaceTempView("lineitem")

  def setupLineitemParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("lineitem")

  def setupLineitemOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("lineitem")

  // CUSTOMER
  val customerSchema = StructType(Array(
    StructField("c_custkey", LongType),
    StructField("c_name", StringType),
    StructField("c_address", StringType),
    StructField("c_nationkey", LongType),
    StructField("c_phone", StringType),
    StructField("c_acctbal", DoubleType),
    StructField("c_mktsegment", StringType),
    StructField("c_comment", StringType)
  ))

  def readCustomerCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(customerSchema).csv(path)

  def setupCustomerCSV(spark: SparkSession, path: String): Unit =
    readCustomerCSV(spark, path).createOrReplaceTempView("customer")

  def setupCustomerParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("customer")

  def setupCustomerOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("customer")

  // NATION
  val nationSchema = StructType(Array(
    StructField("n_nationkey", LongType),
    StructField("n_name", StringType),
    StructField("n_regionkey", LongType),
    StructField("n_comment", StringType)
  ))

  def readNationCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(nationSchema).csv(path)

  def setupNationCSV(spark: SparkSession, path: String): Unit =
    readNationCSV(spark, path).createOrReplaceTempView("nation")

  def setupNationParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("nation")

  def setupNationOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("nation")

  // PART
  val partSchema = StructType(Array(
    StructField("p_partkey", LongType),
    StructField("p_name", StringType),
    StructField("p_mfgr", StringType),
    StructField("p_brand", StringType),
    StructField("p_type", StringType),
    StructField("p_size", LongType),
    StructField("p_container", StringType),
    StructField("p_retailprice", DoubleType),
    StructField("p_comment", StringType)
  ))

  def readPartCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(partSchema).csv(path)

  def setupPartCSV(spark: SparkSession, path: String): Unit =
    readPartCSV(spark, path).createOrReplaceTempView("part")

  def setupPartParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("part")

  def setupPartOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("part")

  // PARTSUPP
  val partsuppSchema = StructType(Array(
    StructField("ps_partkey", LongType),
    StructField("ps_suppkey", LongType),
    StructField("ps_availqty", LongType),
    StructField("ps_supplycost", DoubleType),
    StructField("ps_comment", StringType)
  ))

  def readPartsuppCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(partsuppSchema).csv(path)

  def setupPartsuppCSV(spark: SparkSession, path: String): Unit =
    readPartsuppCSV(spark, path).createOrReplaceTempView("partsupp")

  def setupPartsuppParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("partsupp")

  def setupPartsuppOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("partsupp")

  // REGION
  val regionSchema = StructType(Array(
    StructField("r_regionkey", LongType),
    StructField("r_name", StringType),
    StructField("r_comment", StringType)
  ))

  def readRegionCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(regionSchema).csv(path)

  def setupRegionCSV(spark: SparkSession, path: String): Unit =
    readRegionCSV(spark, path).createOrReplaceTempView("region")

  def setupRegionParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("region")

  def setupRegionOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("region")

  // SUPPLIER
  val supplierSchema = StructType(Array(
    StructField("s_suppkey", LongType),
    StructField("s_name", StringType),
    StructField("s_address", StringType),
    StructField("s_nationkey", LongType),
    StructField("s_phone", StringType),
    StructField("s_acctbal", DoubleType),
    StructField("s_comment", StringType)
  ))

  def readSupplierCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", "|").schema(supplierSchema).csv(path)

  def setupSupplierCSV(spark: SparkSession, path: String): Unit =
    readSupplierCSV(spark, path).createOrReplaceTempView("supplier")

  def setupSupplierParquet(spark: SparkSession, path: String): Unit =
    spark.read.parquet(path).createOrReplaceTempView("supplier")

  def setupSupplierOrc(spark: SparkSession, path: String): Unit =
    spark.read.orc(path).createOrReplaceTempView("supplier")
}

object Q1Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        l_returnflag,
      |        l_linestatus,
      |        sum(l_quantity) as sum_qty,
      |        sum(l_extendedprice) as sum_base_price,
      |        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |        avg(l_quantity) as avg_qty,
      |        avg(l_extendedprice) as avg_price,
      |        avg(l_discount) as avg_disc,
      |        count(*) as count_order
      |from
      |        lineitem
      |where
      |        l_shipdate <= date '1998-12-01' - interval '90' day
      |group by
      |        l_returnflag,
      |        l_linestatus
      |order by
      |        l_returnflag,
      |        l_linestatus
      |
      |""".stripMargin)
}

object Q2Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        s_acctbal,
      |        s_name,
      |        n_name,
      |        p_partkey,
      |        p_mfgr,
      |        s_address,
      |        s_phone,
      |        s_comment
      |from
      |        part,
      |        supplier,
      |        partsupp,
      |        nation,
      |        region
      |where
      |        p_partkey = ps_partkey
      |        and s_suppkey = ps_suppkey
      |        and p_size = 15
      |        and p_type like '%BRASS'
      |        and s_nationkey = n_nationkey
      |        and n_regionkey = r_regionkey
      |        and r_name = 'EUROPE'
      |        and ps_supplycost = (
      |                select
      |                        min(ps_supplycost)
      |                from
      |                        partsupp,
      |                        supplier,
      |                        nation,
      |                        region
      |                where
      |                        p_partkey = ps_partkey
      |                        and s_suppkey = ps_suppkey
      |                        and s_nationkey = n_nationkey
      |                        and n_regionkey = r_regionkey
      |                        and r_name = 'EUROPE'
      |        )
      |order by
      |        s_acctbal desc,
      |        n_name,
      |        s_name,
      |        p_partkey
      |limit 100
      |
      |""".stripMargin)
}

object Q3Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        l_orderkey,
      |        sum(l_extendedprice * (1 - l_discount)) as revenue,
      |        o_orderdate,
      |        o_shippriority
      |from
      |        customer,
      |        orders,
      |        lineitem
      |where
      |        c_mktsegment = 'BUILDING'
      |        and c_custkey = o_custkey
      |        and l_orderkey = o_orderkey
      |        and o_orderdate < date '1995-03-15'
      |        and l_shipdate > date '1995-03-15'
      |group by
      |        l_orderkey,
      |        o_orderdate,
      |        o_shippriority
      |order by
      |        revenue desc,
      |        o_orderdate
      |limit 10
      |
      |""".stripMargin)
}

object Q4Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        o_orderpriority,
      |        count(*) as order_count
      |from
      |        orders
      |where
      |        o_orderdate >= date '1993-07-01'
      |        and o_orderdate < date '1993-07-01' + interval '3' month
      |        and exists (
      |                select
      |                        *
      |                from
      |                        lineitem
      |                where
      |                        l_orderkey = o_orderkey
      |                        and l_commitdate < l_receiptdate
      |        )
      |group by
      |        o_orderpriority
      |order by
      |        o_orderpriority
      |
      |""".stripMargin)
}

object Q5Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        n_name,
      |        sum(l_extendedprice * (1 - l_discount)) as revenue
      |from
      |        customer,
      |        orders,
      |        lineitem,
      |        supplier,
      |        nation,
      |        region
      |where
      |        c_custkey = o_custkey
      |        and l_orderkey = o_orderkey
      |        and l_suppkey = s_suppkey
      |        and c_nationkey = s_nationkey
      |        and s_nationkey = n_nationkey
      |        and n_regionkey = r_regionkey
      |        and r_name = 'ASIA'
      |        and o_orderdate >= date '1994-01-01'
      |        and o_orderdate < date '1994-01-01' + interval '1' year
      |group by
      |        n_name
      |order by
      |        revenue desc
      |
      |""".stripMargin)
}

object Q6Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        sum(l_extendedprice * l_discount) as revenue
      |from
      |        lineitem
      |where
      |        l_shipdate >= date '1994-01-01'
      |        and l_shipdate < date '1994-01-01' + interval '1' year
      |        and l_discount between .06 - 0.01 and .06 + 0.01
      |        and l_quantity < 24
      |
      |""".stripMargin)
}

object Q7Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        supp_nation,
      |        cust_nation,
      |        l_year,
      |        sum(volume) as revenue
      |from
      |        (
      |                select
      |                        n1.n_name as supp_nation,
      |                        n2.n_name as cust_nation,
      |                        year(l_shipdate) as l_year,
      |                        l_extendedprice * (1 - l_discount) as volume
      |                from
      |                        supplier,
      |                        lineitem,
      |                        orders,
      |                        customer,
      |                        nation n1,
      |                        nation n2
      |                where
      |                        s_suppkey = l_suppkey
      |                        and o_orderkey = l_orderkey
      |                        and c_custkey = o_custkey
      |                        and s_nationkey = n1.n_nationkey
      |                        and c_nationkey = n2.n_nationkey
      |                        and (
      |                                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
      |                                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
      |                        )
      |                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
      |        ) as shipping
      |group by
      |        supp_nation,
      |        cust_nation,
      |        l_year
      |order by
      |        supp_nation,
      |        cust_nation,
      |        l_year
      |
      |""".stripMargin)
}

object Q8Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        o_year,
      |        sum(case
      |                when nation = 'BRAZIL' then volume
      |                else 0
      |        end) / sum(volume) as mkt_share
      |from
      |        (
      |                select
      |                        year(o_orderdate) as o_year,
      |                        l_extendedprice * (1 - l_discount) as volume,
      |                        n2.n_name as nation
      |                from
      |                        part,
      |                        supplier,
      |                        lineitem,
      |                        orders,
      |                        customer,
      |                        nation n1,
      |                        nation n2,
      |                        region
      |                where
      |                        p_partkey = l_partkey
      |                        and s_suppkey = l_suppkey
      |                        and l_orderkey = o_orderkey
      |                        and o_custkey = c_custkey
      |                        and c_nationkey = n1.n_nationkey
      |                        and n1.n_regionkey = r_regionkey
      |                        and r_name = 'AMERICA'
      |                        and s_nationkey = n2.n_nationkey
      |                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
      |                        and p_type = 'ECONOMY ANODIZED STEEL'
      |        ) as all_nations
      |group by
      |        o_year
      |order by
      |        o_year
      |
      |""".stripMargin)
}

object Q9Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        nation,
      |        o_year,
      |        sum(amount) as sum_profit
      |from
      |        (
      |                select
      |                        n_name as nation,
      |                        year(o_orderdate) as o_year,
      |                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
      |                from
      |                        part,
      |                        supplier,
      |                        lineitem,
      |                        partsupp,
      |                        orders,
      |                        nation
      |                where
      |                        s_suppkey = l_suppkey
      |                        and ps_suppkey = l_suppkey
      |                        and ps_partkey = l_partkey
      |                        and p_partkey = l_partkey
      |                        and o_orderkey = l_orderkey
      |                        and s_nationkey = n_nationkey
      |                        and p_name like '%green%'
      |        ) as profit
      |group by
      |        nation,
      |        o_year
      |order by
      |        nation,
      |        o_year desc
      |
      |""".stripMargin)
}

object Q10Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        c_custkey,
      |        c_name,
      |        sum(l_extendedprice * (1 - l_discount)) as revenue,
      |        c_acctbal,
      |        n_name,
      |        c_address,
      |        c_phone,
      |        c_comment
      |from
      |        customer,
      |        orders,
      |        lineitem,
      |        nation
      |where
      |        c_custkey = o_custkey
      |        and l_orderkey = o_orderkey
      |        and o_orderdate >= date '1993-10-01'
      |        and o_orderdate < date '1993-10-01' + interval '3' month
      |        and l_returnflag = 'R'
      |        and c_nationkey = n_nationkey
      |group by
      |        c_custkey,
      |        c_name,
      |        c_acctbal,
      |        c_phone,
      |        n_name,
      |        c_address,
      |        c_comment
      |order by
      |        revenue desc
      |limit 20
      |
      |""".stripMargin)
}

object Q11Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        ps_partkey,
      |        sum(ps_supplycost * ps_availqty) as value
      |from
      |        partsupp,
      |        supplier,
      |        nation
      |where
      |        ps_suppkey = s_suppkey
      |        and s_nationkey = n_nationkey
      |        and n_name = 'GERMANY'
      |group by
      |        ps_partkey having
      |                sum(ps_supplycost * ps_availqty) > (
      |                        select
      |                                sum(ps_supplycost * ps_availqty) * 0.0001000000
      |                        from
      |                                partsupp,
      |                                supplier,
      |                                nation
      |                        where
      |                                ps_suppkey = s_suppkey
      |                                and s_nationkey = n_nationkey
      |                                and n_name = 'GERMANY'
      |                )
      |order by
      |        value desc
      |
      |""".stripMargin)
}

object Q12Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        l_shipmode,
      |        sum(case
      |                when o_orderpriority = '1-URGENT'
      |                        or o_orderpriority = '2-HIGH'
      |                        then 1
      |                else 0
      |        end) as high_line_count,
      |        sum(case
      |                when o_orderpriority <> '1-URGENT'
      |                        and o_orderpriority <> '2-HIGH'
      |                        then 1
      |                else 0
      |        end) as low_line_count
      |from
      |        orders,
      |        lineitem
      |where
      |        o_orderkey = l_orderkey
      |        and l_shipmode in ('MAIL', 'SHIP')
      |        and l_commitdate < l_receiptdate
      |        and l_shipdate < l_commitdate
      |        and l_receiptdate >= date '1994-01-01'
      |        and l_receiptdate < date '1994-01-01' + interval '1' year
      |group by
      |        l_shipmode
      |order by
      |        l_shipmode
      |
      |""".stripMargin)
}

object Q13Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        c_count,
      |        count(*) as custdist
      |from
      |        (
      |                select
      |                        c_custkey,
      |                        count(o_orderkey) as c_count
      |                from
      |                        customer left outer join orders on
      |                                c_custkey = o_custkey
      |                                and o_comment not like '%special%requests%'
      |                group by
      |                        c_custkey
      |        ) as c_orders
      |group by
      |        c_count
      |order by
      |        custdist desc,
      |        c_count desc
      |
      |""".stripMargin)
}

object Q14Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        100.00 * sum(case
      |                when p_type like 'PROMO%'
      |                        then l_extendedprice * (1 - l_discount)
      |                else 0
      |        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
      |from
      |        lineitem,
      |        part
      |where
      |        l_partkey = p_partkey
      |        and l_shipdate >= date '1995-09-01'
      |        and l_shipdate < date '1995-09-01' + interval '1' month
      |
      |""".stripMargin)
}

object Q15Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |with revenue0 as
      |        (select
      |                l_suppkey as supplier_no,
      |                sum(l_extendedprice * (1 - l_discount)) as total_revenue
      |        from
      |                lineitem
      |        where
      |                l_shipdate >= date '1996-01-01'
      |                and l_shipdate < date '1996-01-01' + interval '3' month
      |        group by
      |                l_suppkey)
      |
      |
      |select
      |        s_suppkey,
      |        s_name,
      |        s_address,
      |        s_phone,
      |        total_revenue
      |from
      |        supplier,
      |        revenue0
      |where
      |        s_suppkey = supplier_no
      |        and total_revenue = (
      |                select
      |                        max(total_revenue)
      |                from
      |                        revenue0
      |        )
      |order by
      |        s_suppkey
      |
      |""".stripMargin)
}

object Q16Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        p_brand,
      |        p_type,
      |        p_size,
      |        count(distinct ps_suppkey) as supplier_cnt
      |from
      |        partsupp,
      |        part
      |where
      |        p_partkey = ps_partkey
      |        and p_brand <> 'Brand#45'
      |        and p_type not like 'MEDIUM POLISHED%'
      |        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
      |        and ps_suppkey not in (
      |                select
      |                        s_suppkey
      |                from
      |                        supplier
      |                where
      |                        s_comment like '%Customer%Complaints%'
      |        )
      |group by
      |        p_brand,
      |        p_type,
      |        p_size
      |order by
      |        supplier_cnt desc,
      |        p_brand,
      |        p_type,
      |        p_size
      |
      |""".stripMargin)
}

object Q17Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        sum(l_extendedprice) / 7.0 as avg_yearly
      |from
      |        lineitem,
      |        part
      |where
      |        p_partkey = l_partkey
      |        and p_brand = 'Brand#23'
      |        and p_container = 'MED BOX'
      |        and l_quantity < (
      |                select
      |                        0.2 * avg(l_quantity)
      |                from
      |                        lineitem
      |                where
      |                        l_partkey = p_partkey
      |        )
      |
      |""".stripMargin)
}

object Q18Debug {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        o_orderkey,
      |        sum(l_quantity)
      |from
      |        orders,
      |        lineitem
      |where
      |        o_orderkey in (
      |                select
      |                        l_orderkey
      |                from
      |                        lineitem
      |                group by
      |                        l_orderkey having
      |                                sum(l_quantity) > 300
      |        )
      |        and o_orderkey = l_orderkey
      |group by
      |        o_orderkey
      |order by
      |        o_orderkey desc
      |limit 100
      |
      |""".stripMargin)
}

object Q18Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        c_name,
      |        c_custkey,
      |        o_orderkey,
      |        o_orderdate,
      |        o_totalprice,
      |        sum(l_quantity)
      |from
      |        customer,
      |        orders,
      |        lineitem
      |where
      |        o_orderkey in (
      |                select
      |                        l_orderkey
      |                from
      |                        lineitem
      |                group by
      |                        l_orderkey having
      |                                sum(l_quantity) > 300
      |        )
      |        and c_custkey = o_custkey
      |        and o_orderkey = l_orderkey
      |group by
      |        c_name,
      |        c_custkey,
      |        o_orderkey,
      |        o_orderdate,
      |        o_totalprice
      |order by
      |        o_totalprice desc,
      |        o_orderdate
      |limit 100
      |
      |""".stripMargin)
}

object Q19Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        sum(l_extendedprice* (1 - l_discount)) as revenue
      |from
      |        lineitem,
      |        part
      |where
      |        (
      |                p_partkey = l_partkey
      |                and p_brand = 'Brand#12'
      |                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      |                and l_quantity >= 1 and l_quantity <= 1 + 10
      |                and p_size between 1 and 5
      |                and l_shipmode in ('AIR', 'AIR REG')
      |                and l_shipinstruct = 'DELIVER IN PERSON'
      |        )
      |        or
      |        (
      |                p_partkey = l_partkey
      |                and p_brand = 'Brand#23'
      |                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      |                and l_quantity >= 10 and l_quantity <= 10 + 10
      |                and p_size between 1 and 10
      |                and l_shipmode in ('AIR', 'AIR REG')
      |                and l_shipinstruct = 'DELIVER IN PERSON'
      |        )
      |        or
      |        (
      |                p_partkey = l_partkey
      |                and p_brand = 'Brand#34'
      |                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      |                and l_quantity >= 20 and l_quantity <= 20 + 10
      |                and p_size between 1 and 15
      |                and l_shipmode in ('AIR', 'AIR REG')
      |                and l_shipinstruct = 'DELIVER IN PERSON'
      |        )
      |
      |""".stripMargin)
}

object Q20Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        s_name,
      |        s_address
      |from
      |        supplier,
      |        nation
      |where
      |        s_suppkey in (
      |                select
      |                        ps_suppkey
      |                from
      |                        partsupp
      |                where
      |                        ps_partkey in (
      |                                select
      |                                        p_partkey
      |                                from
      |                                        part
      |                                where
      |                                        p_name like 'forest%'
      |                        )
      |                        and ps_availqty > (
      |                                select
      |                                        0.5 * sum(l_quantity)
      |                                from
      |                                        lineitem
      |                                where
      |                                        l_partkey = ps_partkey
      |                                        and l_suppkey = ps_suppkey
      |                                        and l_shipdate >= date '1994-01-01'
      |                                        and l_shipdate < date '1994-01-01' + interval '1' year
      |                        )
      |        )
      |        and s_nationkey = n_nationkey
      |        and n_name = 'CANADA'
      |order by
      |        s_name
      |
      |""".stripMargin)
}

object Q21Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        s_name,
      |        count(*) as numwait
      |from
      |        supplier,
      |        lineitem l1,
      |        orders,
      |        nation
      |where
      |        s_suppkey = l1.l_suppkey
      |        and o_orderkey = l1.l_orderkey
      |        and o_orderstatus = 'F'
      |        and l1.l_receiptdate > l1.l_commitdate
      |        and exists (
      |                select
      |                        *
      |                from
      |                        lineitem l2
      |                where
      |                        l2.l_orderkey = l1.l_orderkey
      |                        and l2.l_suppkey <> l1.l_suppkey
      |        )
      |        and not exists (
      |                select
      |                        *
      |                from
      |                        lineitem l3
      |                where
      |                        l3.l_orderkey = l1.l_orderkey
      |                        and l3.l_suppkey <> l1.l_suppkey
      |                        and l3.l_receiptdate > l3.l_commitdate
      |        )
      |        and s_nationkey = n_nationkey
      |        and n_name = 'SAUDI ARABIA'
      |group by
      |        s_name
      |order by
      |        numwait desc,
      |        s_name
      |limit 100
      |
      |""".stripMargin)
}

object Q22Like {
  def apply(spark: SparkSession): DataFrame = spark.sql(
    """
      |-- using default substitutions
      |
      |select
      |        cntrycode,
      |        count(*) as numcust,
      |        sum(c_acctbal) as totacctbal
      |from
      |        (
      |                select
      |                        substring(c_phone, 1, 2) as cntrycode,
      |                        c_acctbal
      |                from
      |                        customer
      |                where
      |                        substring(c_phone, 1, 2) in
      |                                ('13', '31', '23', '29', '30', '18', '17')
      |                        and c_acctbal > (
      |                                select
      |                                        avg(c_acctbal)
      |                                from
      |                                        customer
      |                                where
      |                                        c_acctbal > 0.00
      |                                        and substring(c_phone, 1, 2) in
      |                                                ('13', '31', '23', '29', '30', '18', '17')
      |                        )
      |                        and not exists (
      |                                select
      |                                        *
      |                                from
      |                                        orders
      |                                where
      |                                        o_custkey = c_custkey
      |                        )
      |        ) as custsale
      |group by
      |        cntrycode
      |order by
      |        cntrycode
      |
      |""".stripMargin)
}

object ConvertFiles {
  /**
   * Main method allows us to submit using spark-submit to perform conversions from CSV to
   * Parquet or Orc.
   */
  def main(arg: Array[String]): Unit = {
    val conf = new FileConversionConf(arg)
    val spark = SparkSession.builder.appName("TPC-H Like File Conversion").getOrCreate()
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
