/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.Assertions._

import org.apache.spark.sql.functions.{log => nickslog}

import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener
import java.io._

class OpcodeSuite extends QueryTest with SharedSparkSession {

  import testImplicits._
  import org.scalatest.Tag


// Utility Function for checking equivalency of Dataset type  
  def checkEquiv[T](ds1: Dataset[T], ds2: Dataset[T]) : Unit = {
    val resultdf = ds1.toDF()
    val refdf = ds2.toDF()
    ds1.show
    ds2.show
    val columns = refdf.schema.fields.map(_.name)
    val selectiveDifferences = columns.map(col => refdf.select(col).except(resultdf.select(col))) 
    selectiveDifferences.map(diff => { assert(diff.count==0) } )
    println("TEST: ***PASSED***")
  }


  object test0 extends Tag("test0")
  object test1 extends Tag("test1")
  object test2 extends Tag("test2")
  object test3 extends Tag("test3")
  object test4 extends Tag("test4")
  object test5 extends Tag("test5")
  object test6 extends Tag("test6")
  object test7 extends Tag("test7")
  object test8 extends Tag("test8")
  object test9 extends Tag("test9")
  object test10 extends Tag("test10")
  object test11 extends Tag("test11")
  object test12 extends Tag("test12")
  object test13 extends Tag("test13")
  object test14 extends Tag("test14")
  object test15 extends Tag("test15")
  object test16 extends Tag("test16")
  object test17 extends Tag("test17")
  object test18 extends Tag("test18")
  object test19 extends Tag("test19")
  object test20 extends Tag("test20")
  object test21 extends Tag("test21")
  object test22 extends Tag("test22")
  object test23 extends Tag("test23")
  object test24 extends Tag("test24")
  object test25 extends Tag("test25")
  object test26 extends Tag("test26")
  object test27 extends Tag("test27")
  object test28 extends Tag("test28")
  object test29 extends Tag("test29")
  object test30 extends Tag("test30")
  object test31 extends Tag("test31")
  object test32 extends Tag("test32")
  object test33 extends Tag("test33")
  object test34 extends Tag("test34")
  object test35 extends Tag("test35")
  object test36 extends Tag("test36")
  object test37 extends Tag("test37")
  object test38 extends Tag("test38")
  object test39 extends Tag("test39")
  object test40 extends Tag("test40")
  object test41 extends Tag("test41")
  object test42 extends Tag("test42")
  object test43 extends Tag("test43")
  object test44 extends Tag("test44")
  object test45 extends Tag("test45")
  object test46 extends Tag("test46")
  object test47 extends Tag("test47")

// START OF TESTS 




// conditional tests, all but test0 fall back to JVM execution
  test("conditional floats", test0) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: conditional floats\n\n")

    val myudf: Float => Float = { x =>
      val t =
        if (x > 1.0f && x < 3.7f) {
          (if (x > 1.1f && x < 2.0f) 1.0f else 1.1f) + 24.0f
        } else {
          if (x < 0.1f) 2.3f else 4.1f
        }
      t + 2.2f
    }
    val u = udf(myudf)
    val dataset = List(2.0f).toDS()
    val result = dataset.withColumn("new", u('value))
    val ref = dataset.withColumn("new", lit(27.300001f))
    checkEquiv(result, ref)
    val dataset2 = List(4.0f).toDS()
    val result2 = dataset2.withColumn("new", u('value))
    val ref2 = dataset2.withColumn("new", lit(6.3f))
    checkEquiv(result2, ref2)
    println("TEST: *** END ***\n")
  }



  test("conditional doubles",test1) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: conditional doubles\n\n")
    val myudf: Double => Double = { x => 
      val t =
        if (x > 1.0 && x <= 3.7) {
          (if (x >= 1.1 && x < 2.1) 1.0 else 1.1) + 24.0
        } else {
          if (x < 1.1) 2.3 else 4.1
        }  
      t + 2.2
    }
    val u = udf(myudf)
    val dataset = List(1.0).toDS()
    val result = dataset.withColumn("new", u('value))
    val ref = dataset.withColumn("new", lit(4.5))
    checkEquiv(result, ref)
    val dataset2 = List(2.0).toDS()
    val result2 = dataset2.withColumn("new", u('value))
    val ref2 = dataset2.withColumn("new", lit(27.2))
    checkEquiv(result2, ref2)
    println("TEST: *** END ***\n")
  }

  test("conditional ints",test2) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: conditional ints\n\n")
    val myudf: Int => Int = { x =>
      val t =
        if (x > 1 && x < 5) {
          10
        } else {
          if (x > 7) 20 else 30
        }
      t + 5
    }
    val u = udf(myudf)
    val dataset = List(2).toDS()
    val result = dataset.withColumn("new",u('value))
    val ref = dataset.withColumn("new", lit(15))
    checkEquiv(result, ref)
    val dataset2 = List(8).toDS()
    val result2 = dataset2.withColumn("new", u('value))
    val ref2 = dataset2.withColumn("new", lit(25))
    checkEquiv(result2, ref2)
    println("TEST: *** END ***\n")
  }

  test("conditional longs", test3) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: conditional longs\n\n")
    val myudf: Long => Long = { x =>
      val t = 
        if (x > 1l && x < 5l) {
          10l
        } else {
          if (x > 7l) 20l else 30l
        }
      t + 5l
    }
    val u = udf(myudf)
    val dataset = List(2l).toDS()
    val result = dataset.withColumn("new", u('value))
    val ref = dataset.withColumn("new", lit(15l))
    checkEquiv(result, ref)
    val dataset2 = List(8l).toDS()
    val result2 = dataset2.withColumn("new", u('value))
    val ref2 = dataset2.withColumn("new", lit(25l))
    checkEquiv(result2, ref2)
    println("TEST: *** END ***\n")
  }



// tests for load and store operations, also cover +/-/* operators for int,long,double,float
  test("LLOAD_<n> odd", test4) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LLOAD_<n> odd")
    println("\n\n")
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",lit(1l))
    val dataset3 = dataset2.withColumn("value3",lit(1l))
    val myudf: (Int, Long, Long) => Long = (a,b,c) => {
      (b+c)*c-b
    }
    val u = udf(myudf)
    val result = dataset3.withColumn("new",u(col("value"),col("value2"),col("value3")))
    val ref = dataset3.withColumn("new",(col("value2")+col("value3"))*col("value3") - col("value2"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }


  test("DLOAD_<n> odd", test5) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: DLOAD_<n> odd")
    println("\n\n")
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",lit(1.0))
    val dataset3 = dataset2.withColumn("value3",lit(1.0))
    val myudf: (Int, Double, Double) => Double = (a,b,c) => {
      (b+c)*b-c
    }
    val u = udf(myudf)
    val result = dataset3.withColumn("new", u(col("value"),col("value2"),col("value3")))
    val ref = dataset3.withColumn("new",(col("value2")+col("value3"))*col("value2")-col("value3"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("DLOAD_<n> even", test6) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: DLOAD_<n> even")
    println("\n\n")
    val dataset = List(1.0).toDS()
    val dataset2 = dataset.withColumn("value2",col("value"))
    val myudf: (Double, Double) => Double = (a,b) => {
      (a+b)*a-b
    }
    val u = udf(myudf)
    val result = dataset2.withColumn("new",u(col("value"),col("value2"))) 
    val ref = dataset2.withColumn("new",(col("value")+col("value2"))*col("value")-col("value2"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("LLOAD_<n> even", test7) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LLOAD_<n> even")
    println("\n\n")
    val dataset = List(1l).toDS()
    val dataset2 = dataset.withColumn("value2",col("value"))
    val myudf: (Long, Long) => Long = (a,b) => {
      (a+b)*a-b
    }
    val u = udf(myudf)
    val result = dataset2.withColumn("new",u(col("value"),col("value2")))
    val ref = dataset2.withColumn("new",(col("value")+col("value2"))*col("value")-col("value2"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ILOAD_<n> all",test8) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ILOAD_<n> all")
    println("\n\n")
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",col("value"))
    val dataset3 = dataset2.withColumn("value3",col("value"))
    val dataset4 = dataset3.withColumn("value4",col("value"))
    val myudf: (Int, Int, Int, Int) => Int = (a,b,c,d) => {
      (a+b-c)*d
    }
    val u = udf(myudf)
    val result = dataset4.withColumn("new",u(col("value"),col("value2"),col("value3"),col("value4")))
    val ref = dataset4.withColumn("new",(col("value")+col("value2")-col("value3"))*col("value4"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("FLOAD_<n> all", test9) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: FLOAD_<n> all")
    println("\n\n")
    val dataset = List(1.0f).toDS()
    val dataset2 = dataset.withColumn("value2",col("value"))
    val dataset3 = dataset2.withColumn("value3",col("value"))
    val dataset4 = dataset3.withColumn("value4",col("value"))
    val myudf: (Float, Float, Float, Float) => Float = (a,b,c,d) => {
      (a+b-c)*d
    }
    val u = udf(myudf)
    val result = dataset4.withColumn("new",u(col("value"),col("value2"),col("value3"),col("value4")))
    val ref = dataset4.withColumn("new",(col("value")+col("value2")-col("value3"))*col("value4"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ISTORE_<n> all", test10) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ISTORE_<n> all")
    println("\n\n")
    val myudf: () => Int = () => {
      var myInt : Int = 1
      var myInt2 : Int = 1
      var myInt3 : Int = myInt 
      var myInt4 : Int = myInt * myInt3
      myInt4
    }
    val dataset = List(1).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("DSTORE_<n> even", test11) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: DSTORE_<n> even")
    println("\n\n")
    val myudf: () => Double = () => {
      var myDoub : Double = 0.0
      var myDoub2 : Double = 1.0 - myDoub
      myDoub2
    }
    val dataset = List(1).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("DSTORE_<n> odd", test12) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: DSTORE_<n> odd")
    println("\n\n")
    val myudf: (Int) => Double = (a) => {
      var myDoub : Double = 1.0
      var myDoub2 : Double = 1.0 * myDoub
      myDoub2
    }
    val dataset = List(1).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new",lit(1.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ALOAD_0", test13) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ALOAD_0")
    println("\n\n")
    val myudf: (String,String,String,String) => String = (a,b,c,d) => {
      a
    }
    val dataset = List("a").toDS()
    val dataset2 = dataset.withColumn("value2",lit("b"))
    val dataset3 = dataset2.withColumn("value3",lit("c"))
    val dataset4 = dataset3.withColumn("value4",lit("d"))
    val u = udf(myudf)
    val result = dataset4.withColumn("new",u(col("value"),col("value2"),col("value3"),col("value4")))
    val ref = dataset4.withColumn("new",col("value"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ALOAD_1", test14) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ALOAD_1")
    println("\n\n")
    val myudf: (String,String,String,String) => String = (a,b,c,d) => {
      b
    }
    val dataset = List("a").toDS()
    val dataset2 = dataset.withColumn("value2",lit("b"))
    val dataset3 = dataset2.withColumn("value3",lit("c"))
    val dataset4 = dataset3.withColumn("value4",lit("d"))
    val u = udf(myudf)
    val result = dataset4.withColumn("new",u(col("value"),col("value2"),col("value3"),col("value4")))
    val ref = dataset4.withColumn("new",col("value2"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ALOAD_2", test15) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ALOAD_2")
    println("\n\n")
    val myudf: (String,String,String,String) => String = (a,b,c,d) => {
      c
    }
    val dataset = List("a").toDS()
    val dataset2 = dataset.withColumn("value2",lit("b"))
    val dataset3 = dataset2.withColumn("value3",lit("c"))
    val dataset4 = dataset3.withColumn("value4",lit("d"))
    val u = udf(myudf)
    val result = dataset4.withColumn("new",u(col("value"),col("value2"),col("value3"),col("value4")))
    val ref = dataset4.withColumn("new",col("value3"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ALOAD_3", test16) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ALOAD_3")
    println("\n\n")
    val myudf: (String,String,String,String) => String = (a,b,c,d) => {
      d
    }
    val dataset = List("a").toDS()
    val dataset2 = dataset.withColumn("value2",lit("b"))
    val dataset3 = dataset2.withColumn("value3",lit("c"))
    val dataset4 = dataset3.withColumn("value4",lit("d"))
    val u = udf(myudf)
    val result = dataset4.withColumn("new",u(col("value"),col("value2"),col("value3"),col("value4")))
    val ref = dataset4.withColumn("new",col("value4"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ASTORE_1,2,3", test17) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ASTORE_1,2,3")
    println("\n\n")
    val myudf: (String) => String = (a) => {
      val myString : String = a
      val myString2 : String = myString
      val myString3 : String = myString2
      myString3
    }
    val dataset = List("a").toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new",u(col("value")))
    val ref = dataset.withColumn("new",col("value"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("FSTORE_1,2,3", test18) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: FSTORE_1,2,3")
    println("\n\n")
    val myudf: (Float) => Float = (a) => {
      var myFloat : Float = a
      var myFloat2 : Float = myFloat + a
      var myFloat3 : Float = myFloat2 + a
      myFloat3
    }
    val dataset = List(5.0f).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new",col("value")*3)
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }


  test("LSTORE_2", test19) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LSTORE_2")
    println("\n\n")
    val myudf: (Long) => Long = (a) => {
      var myLong : Long = a
      myLong
    }
    val dataset = List(5l).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new",col("value"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("LSTORE_3", test20) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LSTORE_3")
    println("\n\n")
    val myudf: (Int, Long) => Long = (a,b) => {
      var myLong : Long = b
      myLong
    }
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2", lit(5l))
    val u = udf(myudf)
    val result = dataset2.withColumn("new", u(col("value"),col("value2")))
    val ref = dataset2.withColumn("new",col("value2"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  // misc. tests. Boolean check currently failing, can't handle true/false

  test("Boolean check", test21) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Boolean check\n\n")
    val myudf: () => Boolean = () => {
      var myBool : Boolean = true
      myBool
    }
    val dataset = List(true).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new",u())
    val ref = dataset.withColumn("new",lit(true))
    // val resultdf = result.toDF()
    // val refdf = ref.toDF()
    // val columns = refdf.schema.fields.map(_.name)
    // val selectiveDifferences = columns.map(col => refdf.select(col).except(resultdf.select(col)))
    // selectiveDifferences.map(diff => {assert(diff.count==0)})
    result.show
    ref.show
    println("This test is *** FAILED *** as of 5/5/2019. If the two tables directly above are not identical, test is still failing.\n")
    println("TEST: *** END ***\n")
  }

  
  // the test immediately below is meant to cover IFEQ, but is failing due to absense of IFNE

  test("IFEQ opcode", test22) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: IFEQ\n\n")
    val myudf: (Double) => Double = (a) => {
      var myDoub : Double = a;
      if (a==a) {
        myDoub = a*a
      }
      myDoub
    }
    val dataset = List(2.0).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(4.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }


  // the test below is a one-off test used to test the functionality of LDC, also covers ASTORE_0. currently having trouble verifying output


  test("LDC tests", test23) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LDC tests\n\n")
    class placeholder {
      val myudf: () => (String) = () => {
        val myString : String = "a"
        myString
      }
      val u = udf(myudf)
      val dataset = List("a").toDS()
      val result = dataset.withColumn("new", u())
      val ref = dataset.withColumn("new",lit("a"))
      // val resultdf = result.toDF()
      // val refdf = ref.toDF()
      // val columns = refdf.schema.fields.map(_.name)
      // val selectiveDifferences = columns.map(col => refdf.select(col).except(resultdf.select(col)))
      // selectiveDifferences.map(diff => {assert(diff.count==0)})
      // result.show
      // ref.show
      //println("LDC test: ***PASSED***")
      checkEquiv(result, ref)
    }
    println("TEST: *** END ***\n")
  }



  // this test makes sure we can handle udfs with more than 2 args

  test("UDF 4 args",test24) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: UDF 4 args\n\n")
    val myudf: (Int, Int, Int, Int) => Int = (w,x,y,z) => { w+x+y+z }
    val u = udf(myudf)
    val dataset = List(1,2,3,4).toDS()
    val dataset2 = dataset.withColumn("value2",col("value") + 1)
    val dataset3 = dataset2.withColumn("value3",col("value2") + 1)
    val dataset4 = dataset3.withColumn("value4",col("value3") + 1)
    // val result = u(data)
    // dataset3.show
    // val dataset = List((1,2,3),(2,3,4),(3,4,5)).toDS()
    val result = dataset4.withColumn("new", u(col("value"), col("value2"), col("value3"), col("value4")))
    val ref = dataset4.withColumn("new", col("value")+col("value2")+col("value3")+col("value4"))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  // this test covers getstatic and invokevirtual, shows we can handle math ops (only acos/asin)

  test("math functions - trig - (a)sin and (a)cos", test25) {
    println("\n\n")
    Thread.sleep(1000) 
    println("EXECUTING TEST: math functions - trig - (a)sin and (a)cos\n\n")
    val myudf1: Double => Double = x => { math.cos(x) }
    val u1 = udf(myudf1)
    val myudf2: Double => Double = x => { math.sin(x) }
    val u2 = udf(myudf2)
    val myudf3: Double => Double = x => { math.acos(x) }
    val u3 = udf(myudf3)
    val myudf4: Double => Double = x => { math.asin(x) }
    val u4 = udf(myudf4)
    val dataset = List(1.0,2.0,3.0).toDS()
    val result = dataset.withColumn("new", u1('value)+u2('value)+u3('value)+u4('value))
    val ref = dataset.withColumn("new", cos(col("value"))+sin(col("value"))+acos(col("value"))+asin(col("value")))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }


  test("math functions - trig - (a)tan(h) and cosh", test26) {
    println("\n\n")
    Thread.sleep(1000) 
    println("EXECUTING TEST: math functions - trig - (a)tan(h) and cosh\n\n")
    val myudf1: Double => Double = x => { math.tan(x) }
    val u1 = udf(myudf1)
    val myudf2: Double => Double = x => { math.atan(x) }
    val u2 = udf(myudf2)
    val myudf3: Double => Double = x => { math.cosh(x) }
    val u3 = udf(myudf3)
    val myudf4: Double => Double = x => { math.tanh(x) }
    val u4 = udf(myudf4)
    val dataset = List(1.0,2.0,3.0).toDS()
    val result = dataset.withColumn("new", u1('value)+u2('value)+u3('value)+u4('value))
    val ref = dataset.withColumn("new", tan(col("value")) + atan(col("value")) + cosh(col("value")) + tanh(col("value")))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("math functions - abs, ceil, floor", test27) {
    println("\n\n")
    Thread.sleep(1000) 
    println("EXECUTING TEST: math functions - abs, ceil, floor\n\n")
    val myudf1: Double => Double = x => { math.abs(x) }
    val u1 = udf(myudf1)
    val myudf2: Double => Double = x => { math.ceil(x) }
    val u2 = udf(myudf2)
    val myudf3: Double => Double = x => { math.floor(x) }
    val u3 = udf(myudf3)
    val dataset = List(-0.5,0.5).toDS()
    val result = dataset.withColumn("new", u2(u1('value))+u3(u1('value)))
    val ref = dataset.withColumn("new", ceil(abs(col("value"))) + floor(abs(col("value"))))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }


  test("math functions - exp, log, log10, sqrt", test28) {
    println("\n\n")
    Thread.sleep(1000) 
    println("EXECUTING TEST: math functions - exp, log, log10, sqrt\n\n")
    val myudf1: Double => Double = x => { math.exp(x) }
    val u1 = udf(myudf1)
    val myudf2: Double => Double = x => { math.log(x) }
    val u2 = udf(myudf2)
    val myudf3: Double => Double = x => { math.log10(x) }
    val u3 = udf(myudf3)
    val myudf4: Double => Double = x => { math.sqrt(x) }
    val u4 = udf(myudf4)
    val dataset = List(2.0,5.0).toDS()
    val result = dataset.withColumn("new", u1('value)+u2('value)+u3('value)+u4('value))
    val ref = dataset.withColumn("new", exp(col("value"))+nickslog(col("value"))+log10(col("value"))+sqrt(col("value")))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("FSTORE_0, LSTORE_1", test29) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: FSTORE_0, LSTORE_1\n\n")
    val myudf: () => Float = () => {
      var myFloat : Float = 1.0f
      var myLong : Long = 1l
      myFloat
    }
    val dataset = List(5.0f).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1.0f))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("LSTORE_0", test30) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LSTORE_0\n\n")
    val myudf: () => Long = () => {
      var myLong : Long = 1l
      myLong
    }
    val dataset = List(1l).toDS()
    val u = udf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1l))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("ILOAD",test31) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: ILOAD\n\n")
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Int = (a,b,c,d,e,f,g,h) => {
      e
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",col("value") + 1)
    val dataset3 = dataset2.withColumn("value3",col("value2") + 1)
    val dataset4 = dataset3.withColumn("value4",col("value3") + 1)
    val dataset5 = dataset4.withColumn("value5",col("value4") + 1)
    val dataset6 = dataset5.withColumn("value6",lit(1l))
    val dataset7 = dataset6.withColumn("value7",lit(1.0f))
    val dataset8 = dataset7.withColumn("value8",lit(1.0))
    val result = dataset8.withColumn("new", u(col("value"),col("value2"),col("value3"),col("value4"),col("value5"),col("value6"),col("value7"),col("value8")))
    val ref = dataset8.withColumn("new", lit(5))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("LLOAD",test32) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: LLOAD\n\n")
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Long = (a,b,c,d,e,f,g,h) => {
      f
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",col("value") + 1)
    val dataset3 = dataset2.withColumn("value3",col("value2") + 1)
    val dataset4 = dataset3.withColumn("value4",col("value3") + 1)
    val dataset5 = dataset4.withColumn("value5",col("value4") + 1)
    val dataset6 = dataset5.withColumn("value6",lit(1l))
    val dataset7 = dataset6.withColumn("value7",lit(1.0f))
    val dataset8 = dataset7.withColumn("value8",lit(1.0))
    val result = dataset8.withColumn("new", u(col("value"),col("value2"),col("value3"),col("value4"),col("value5"),col("value6"),col("value7"),col("value8")))
    val ref = dataset8.withColumn("new", lit(1l))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("FLOAD",test33) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: FLOAD\n\n")
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Float = (a,b,c,d,e,f,g,h) => {
      g
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",col("value") + 1)
    val dataset3 = dataset2.withColumn("value3",col("value2") + 1)
    val dataset4 = dataset3.withColumn("value4",col("value3") + 1)
    val dataset5 = dataset4.withColumn("value5",col("value4") + 1)
    val dataset6 = dataset5.withColumn("value6",lit(1l))
    val dataset7 = dataset6.withColumn("value7",lit(1.0f))
    val dataset8 = dataset7.withColumn("value8",lit(1.0))
    val result = dataset8.withColumn("new", u(col("value"),col("value2"),col("value3"),col("value4"),col("value5"),col("value6"),col("value7"),col("value8")))
    val ref = dataset8.withColumn("new", lit(1.0f))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("DLOAD",test34) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: DLOAD\n\n")
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Double = (a,b,c,d,e,f,g,h) => {
      h
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val dataset2 = dataset.withColumn("value2",col("value") + 1)
    val dataset3 = dataset2.withColumn("value3",col("value2") + 1)
    val dataset4 = dataset3.withColumn("value4",col("value3") + 1)
    val dataset5 = dataset4.withColumn("value5",col("value4") + 1)
    val dataset6 = dataset5.withColumn("value6",lit(1l))
    val dataset7 = dataset6.withColumn("value7",lit(1.0f))
    val dataset8 = dataset7.withColumn("value8",lit(1.0))
    val result = dataset8.withColumn("new", u(col("value"),col("value2"),col("value3"),col("value4"),col("value5"),col("value6"),col("value7"),col("value8")))
    val ref = dataset8.withColumn("new", lit(1.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Double to Int",test35) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Double to Int\n\n")
    val myudf: () => Int = () => {
      var myVar : Double = 0.0
      myVar = myVar + 1.0
      val myVar2 : Int = myVar.toInt
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Float to Int",test36) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Float to Int\n\n")
    val myudf: () => Int = () => {
      var myVar : Float = 0.0f
      myVar = myVar + 1.0f + 2.0f
      val myVar2 : Int = myVar.toInt
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(3))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Long to Int",test37) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Long to Int\n\n")
    val myudf: () => Int = () => {
      var myVar : Long = 0l
      myVar = myVar + 1l
      val myVar2 : Int = myVar.toInt
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1l))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Int to Long",test38) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Int to Long\n\n")
    val myudf: () => Long = () => {
      var myVar : Int = 0
      myVar = myVar + 1 + 2 + 3 + 4 + 5
      val myVar2 : Long = myVar.toLong
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(15l))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Float to Long",test39) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Float to Long\n\n")
    val myudf: () => Long = () => {
      var myVar : Float = 0.0f
      myVar = myVar + 1.0f + 2.0f
      val myVar2 : Long = myVar.toLong
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(3l))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Double to Long",test40) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Double to Long\n\n")
    val myudf: () => Long = () => {
      var myVar : Double = 0.0
      myVar = myVar + 1.0
      val myVar2 : Long = myVar.toLong
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1l))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }


  test("Cast Int to Float",test41) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Int to Float\n\n")
    val myudf: () => Float = () => {
      var myVar : Int = 0
      myVar = myVar + 1 + 2 + 3 + 4 + 5
      val myVar2 : Float = myVar.toFloat
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(15.0f))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Long to Float",test42) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Long to Float\n\n")
    val myudf: () => Float = () => {
      var myVar : Long = 0l
      myVar = myVar + 1l
      val myVar2 : Float = myVar.toFloat
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1.0f))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Double to Float",test43) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Double to Float\n\n")
    val myudf: () => Float = () => {
      var myVar : Double = 0.0
      myVar = myVar + 1.0
      val myVar2 : Float = myVar.toFloat
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1.0f))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Int to Double",test44) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Int to Double\n\n")
    val myudf: () => Double = () => {
      var myVar : Int = 0
      myVar = myVar + 1 + 2 + 3 + 4 + 5
      val myVar2 : Double = myVar.toDouble
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(15.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Long to Double",test45) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Long to Double\n\n")
    val myudf: () => Double = () => {
      var myVar : Long = 0l
      myVar = myVar + 1l
      val myVar2 : Double = myVar.toDouble
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }

  test("Cast Float to Double",test46) {
    println("\n\n")
    Thread.sleep(1000)
    println("EXECUTING TEST: Cast Float to Double\n\n")
    val myudf: () => Double = () => {
      var myVar : Float = 0.0f
      myVar = myVar + 1.0f + 2.0f
      val myVar2 : Double = myVar.toDouble
      myVar2
    }
    val u = udf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(3.0))
    checkEquiv(result, ref)
    println("TEST: *** END ***\n")
  }




  // test("UDF 4 args",test26) {
  //   println("\n\n")
  //   Thread.sleep(1000)
  //   println("EXECUTING TEST: UDF 4 args\n\n")
  //   val myudf: (Int, Int, Int, Int) => Int = (w,x,y,z) => { w+x+y+z }
  //   val u = udf(myudf)
  //   val dataset = List(1,2,3,4).toDS()
  //   val dataset2 = dataset.withColumn("value2",col("value") + 1)
  //   val dataset3 = dataset2.withColumn("value3",col("value2") + 1)
  //   val dataset4 = dataset3.withColumn("value4",col("value3") + 1)
  //   // val result = u(data)
  //   // dataset3.show
  //   // val dataset = List((1,2,3),(2,3,4),(3,4,5)).toDS()
  //   val result = dataset4.withColumn("new", u(col("value"), col("value2"), col("value3"), col("value4")))
  //   val ref = dataset4.withColumn("new", col("value")+col("value2")+col("value3")+col("value4"))
  //   checkEquiv(result, ref)
  //   println("TEST: *** END ***\n")
  // }

}
 
