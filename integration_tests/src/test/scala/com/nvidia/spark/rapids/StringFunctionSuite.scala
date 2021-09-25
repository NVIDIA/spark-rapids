/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.scalatest.Ignore

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

 /* 
 * Different versions of Java support different versions of Unicode. 
 *
 * java 8  :  unicode 6.2
 * java 9  :  unicode 8.0     (unsupported)
 * java 10 :  unicode 8.0     (unsupported)
 * java 11 :  unicode 10.0
 * java 12 :  unicode 11.0    (unsupported)
 * java 13 :  unicode 12.1
 *
 */
object SupportedUnicodeVersion extends Enumeration {
  val UNICODE_6 = 0
  val UNICODE_10 = 1
  val UNICODE_12 = 2  
  
  val NUM_UNICODE_VERSIONS = 3  
  val UNICODE_UNSUPPORTED = 4

  val currentSupportedVersion = UNICODE_12
}

/*
 * Cudf only supports a single version of Unicode (currently, 12.1). However, people running
 * various versions of Java that support different unicode versions will see different results
 * from Spark cpu. The lists below represent characters which we know will come up 'incorrect'
 * during a test, depending on Java/unicode version. To work around this, we maintain lists of
 * known bad characters for earlier versions of Java/unicode so that the tests don't break.
 */
object CudfIncompatibleCodepoints {     
  val uppercaseIncompatible = Array[List[Int]](
                                  // Java 8 / unicode 6.2
                                  List( 604,   609,   618,   620,   642,   647,   669,   670,
                                        1011,  4304,  4305,  4306,  4307,  4308,  4309,  4310,
                                        4311,  4312,  4313,  4314,  4315,  4316,  4317,  4318,
                                        4319,  4320,  4321,  4322,  4323,  4324,  4325,  4326,
                                        4327,  4328,  4329,  4330,  4331,  4332,  4333,  4334,
                                        4335,  4336,  4337,  4338,  4339,  4340,  4341,  4342,
                                        4343,  4344,  4345,  4346,  4349,  4350,  4351,  7566),

                                  // java 11, unicode 10
                                  List( 642,   4304,  4305,  4306,  4307,  4308,  4309,  4310,
                                        4311,  4312,  4313,  4314,  4315,  4316,  4317,  4318,
                                        4319,  4320,  4321,  4322,  4323,  4324,  4325,  4326,
                                        4327,  4328,  4329,  4330,  4331,  4332,  4333,  4334,
                                        4335,  4336,  4337,  4338,  4339,  4340,  4341,  4342,
                                        4343,  4344,  4345,  4346,  4349,  4350,  4351,  7566,
                                        42900),

                                  // java 13, unicode 12
                                  List())

  val lowercaseIncompatible = Array[List[Int]](
                                  // Java 8 / unicode 6.2
                                  List( 5024,  5025,  5026,  5027,  5028,  5029,  5030,  5031,
                                        5032,  5033,  5034,  5035,  5036,  5037,  5038,  5039,
                                        5040,  5041,  5042,  5043,  5044,  5045,  5046,  5047,
                                        5048,  5049,  5050,  5051,  5052,  5053,  5054,  5055,
                                        5056,  5057,  5058,  5059,  5060,  5061,  5062,  5063,
                                        5064,  5065,  5066,  5067,  5068,  5069,  5070,  5071,
                                        5072,  5073,  5074,  5075,  5076,  5077,  5078,  5079,
                                        5080,  5081,  5082,  5083,  5084,  5085,  5086,  5087,
                                        5088,  5089,  5090,  5091,  5092,  5093,  5094,  5095,
                                        5096,  5097,  5098,  5099,  5100,  5101,  5102,  5103,
                                        5104,  5105,  5106,  5107,  5108),

                                  // java 11, unicode 10
                                  List(),

                                  // java 13, unicode 12
                                  List())
}

/* 
 * Different versions of Java support different versions of Unicode.  this class provides
 * the valid list of codepoints for whatever version of Java is being run.  It also 
 * provides filtered lists of codepoints that are known to be broken in cudf.
 *
 * java 8  :  unicode 6.2
 * java 9  :  unicode 8.0     (unsupported)
 * java 10 :  unicode 8.0     (unsupported)
 * java 11 :  unicode 10.0
 * java 12 :  unicode 11.0    (unsupported)
 * java 13 :  unicode 12.1
 *
 */
object TestCodepoints {
  val validCodepoints = (0 until 65536).filter(Character.isDefined).map(i => (i, i.toChar.toString))
  val validCodepointIndices = validCodepoints.map(tuple => tuple._1)

  // determine what 'supported' version of unicode we're using, if any
  def getActiveUnicodeVersion(): Int = {       
    val vp = System.getProperties().getProperty("java.specification.version").split('.')    

    // java <= 8
    if (vp(0).toInt == 1 && vp.length > 1) {
      if(vp(1).toInt == 8){
        return SupportedUnicodeVersion.UNICODE_6
      }
      return SupportedUnicodeVersion.UNICODE_UNSUPPORTED
    } 

    // java 9+
    vp(0).toInt match {
      case 11 => SupportedUnicodeVersion.UNICODE_10
      case 13 => SupportedUnicodeVersion.UNICODE_12
      case _ => SupportedUnicodeVersion.UNICODE_UNSUPPORTED
    }    
  }
  // print out a warning if we're on an unsupported version
  if (getActiveUnicodeVersion() == SupportedUnicodeVersion.UNICODE_UNSUPPORTED) {
    printf("WARNING : Unsupported version of Java (%s). You may encounted unexpected " +
      "test failures\n", System.getProperties().getProperty("java.specification.version"))
  }

  // get the unicode index to use. if we are on an unknown/unsupported version, just
  // default to unicode 12
  def getUnicodeIncompatibleIndex(): Int = {            
    val version = getActiveUnicodeVersion()  
    if (version == SupportedUnicodeVersion.UNICODE_UNSUPPORTED) {
      SupportedUnicodeVersion.UNICODE_12 
    } else {
      version
    }
  }  
    
  // all unicode codepoints valid for this particular version of Java/Unicode.
  def validCodepointCharsDF(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    validCodepoints.toDF("indices", "strings")
  }

  // codepoint chars that we know should be working.  known issues in cudf are filtered out here
  def uppercaseCompatibleCharsDF(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    val version = getUnicodeIncompatibleIndex()
    val utf8Chars = (validCodepointIndices diff
      CudfIncompatibleCodepoints.uppercaseIncompatible(version)).map(i => i.toChar.toString)
    utf8Chars.toDF("strings")
  }

  // codepoint chars that we know should be working.  known issues in cudf are filtered out here
  def lowercaseCompatibleCharsDF(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    val version = getUnicodeIncompatibleIndex()
    val utf8Chars = (validCodepointIndices diff
      CudfIncompatibleCodepoints.lowercaseIncompatible(version)).map(i => i.toChar.toString)
    utf8Chars.toDF("strings")
  }  
}

class StringOperatorsSuite extends SparkQueryCompareTestSuite {         
  INCOMPAT_testSparkResultsAreEqual("Test compatible values upper case modifier",
    TestCodepoints.uppercaseCompatibleCharsDF) {
    frame => frame.select(upper(col("strings")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test compatible values lower case modifier",
    TestCodepoints.lowercaseCompatibleCharsDF) {
    frame => frame.select(lower(col("strings")))
  }
}

/*
* This isn't actually a test.  It's just useful to help visualize what's going on when there are
* differences present.
*/
@Ignore
class StringOperatorsDiagnostics extends SparkQueryCompareTestSuite {  
  def generateResults(gen : org.apache.spark.sql.Column => org.apache.spark.sql.Column):
      (Array[Row], Array[Row]) = {
    val (testConf, _) = setupTestConfAndQualifierName("", true, false,
      new SparkConf(), Seq.empty, 0.0, false, false)
    runOnCpuAndGpu(TestCodepoints.validCodepointCharsDF,
      frame => frame.select(gen(col("strings"))), testConf)
  }
 
  // utility function to print out detailed information on differences
  def generateUnicodeDiffs(title  : String,
      gen: () => (Array[Row], Array[Row])): Unit = {
    val (fromCpu, fromGpu) = gen()
   
    println(s"$title ----------------------------------------")

    println("\u001b[1;36mSummary of diffs:\u001b[0m")
    println("\u001b[1;36mCodepoint:\u001b[0m ")    
    for (i <- fromCpu.indices) {
      if (fromCpu(i) != fromGpu(i)) { 
        val codepoint = TestCodepoints.validCodepointIndices(i)
        print(f"$codepoint%5d, ")  
      }
    }
    print("\n\n")

    println("\u001b[1;36mDetails:")
    println("Codepoint       CPU               GPU")
    println("single -> single mappings\u001b[0m");
    for (i <- fromCpu.indices) {
      if (fromCpu(i) != fromGpu(i) && fromCpu(i).getString(0).length == 1) {
        val codepoint = TestCodepoints.validCodepointIndices(i)

        print(f"(${codepoint.toChar.toString} $codepoint%5d[$codepoint%04x] " +
          f"(${fromCpu(i).getString(0)}")
        print(f"${fromCpu(i).getString(0)(0).toInt}%5d[${fromCpu(i).getString(0)(0).toInt}%04x]) ")
        println(f"${fromGpu(i).getString(0)(0).toInt}%5d[${fromGpu(i).getString(0)(0).toInt}%04x])")
      }
    }
    println("\u001b[1;36msingle -> multi mappings\u001b[0m");
    for (i <- fromCpu.indices) {
      if (fromCpu(i) != fromGpu(i) && fromCpu(i).getString(0).length > 1) {
        var cpu_str = fromCpu(i).getString(0)
        var gpu_str = fromGpu(i).getString(0)

        val codepoint = TestCodepoints.validCodepointIndices(i)
        print(f"(${codepoint.toChar.toString} $codepoint[$codepoint%04x]) ($cpu_str ")        
        print(f"${cpu_str.map(_.toInt.formatted("%d")).mkString(",")}")
        print("[")        
        print(f"${cpu_str.map(_.toInt.formatted("%04x")).mkString(",")}")
        print(f"]) ($gpu_str ")
        print(f"${gpu_str.map(_.toInt.formatted("%d")).mkString(",")}")        
        print("[");
        print(f"${gpu_str.map(_.toInt.formatted("%04x")).mkString(",")}")
        println("])");
      }
    }
    println("---------------------------------------------")
  }  
  // generateUnicodeDiffs("UPPER", () => generateResults(upper))
  // generateUnicodeDiffs("LOWER", () => generateResults(lower))
  
  // generates special case character mapping hash table generation input data.
  def generateCharMappings(): Unit = {    
    class charMapping {
      var   num_upper = 0
      val   upper = Array(0, 0, 0)
      var   num_lower = 0
      val   lower = Array(0, 0, 0)
    }    
    val mapping = Array.fill[charMapping](65536)(new charMapping())
            
    // upper results
    val (fromCpuUpper, fromGpuUpper) = generateResults(upper)
    for (i <- fromCpuUpper.indices) {
      if (fromCpuUpper(i) != fromGpuUpper(i) && fromGpuUpper(i).getString(0).length == 1) {
        val codepoint = TestCodepoints.validCodepointIndices(i)
        
        val cpu_str = fromCpuUpper(i).getString(0)        
        mapping(codepoint).num_upper = cpu_str.length
        for (c <- 0 until cpu_str.length) { mapping(codepoint).upper(c) = cpu_str(c).toInt }
      }
    }

    // lower results
    val (fromCpuLower, fromGpuLower) = generateResults(lower)
    for (i <- fromCpuLower.indices) {
      if (fromCpuLower(i) != fromGpuLower(i) && fromGpuLower(i).getString(0).length == 1) {
        val codepoint = TestCodepoints.validCodepointIndices(i)
        
        val cpu_str = fromCpuLower(i).getString(0)
        mapping(codepoint).num_lower = cpu_str.length
        for (c <- 0 until cpu_str.length) { mapping(codepoint).lower(c) = cpu_str(c).toInt }
      }
    }

    // struct declaration
    println("struct special_case_mapping_in {")
    println("   uint16_t num_upper_chars;")
    println("   uint16_t upper[3];")
    println("   uint16_t num_lower_chars;")
    println("   uint16_t lower[3];")
    println("};")
            
    // mappings
    println("constexpr special_case_mapping_in codepoint_mapping_in[] = {")
    for (i <- 0 until 65536) {
      val mc = mapping(i)
      if (mc.num_upper != 0 || mc.num_lower != 0) {
        println(s"   { ${mc.num_upper} {${mc.upper(0)}, ${mc.upper(1)}, ${mc.upper(2)}}, " +
          s"${mc.num_lower}, {${mc.lower(0)}, ${mc.lower(1)}, ${mc.lower(2)}} },")                
      }
    }
    println("};")

    // codepoints
    println("constexpr uint16_t codepoints_in[] = {\n")
    var count = 0
    for (i <- 0 until 65536) {
      val mc = mapping(i)
      if (mc.num_upper != 0 || mc.num_lower != 0) {
        print(s"   $i,")
        count = count + 1
        if (count > 0 && count % 10 == 0) {
          println("")
        }
      }
    }
    println("\n};")
  }  
  // generateCharMappings()  
}
