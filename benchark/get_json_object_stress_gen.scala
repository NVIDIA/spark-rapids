/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

spark.conf.set("spark.rapids.sql.enabled", false)

import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.types._

val numRows = 3000000
//val nullProbability = 0.1
val nullProbability = 0.0001
val output = "/data/tmp/SCALE_FROM_JSON"

def doIt(): Unit = {
  val ts_0 = StructField("test_string_0", StringType)
  val ts_1 = StructField("test_string_1", StringType)
  val ts_2 = StructField("test_string_2", StringType)
  val ts_3 = StructField("test_string_3", StringType)
  val ts_4 = StructField("test_string_4", StringType)
  val ts_5 = StructField("test_string_5", StringType)
  val ts_6 = StructField("test_string_6", StringType)
  val ts_7 = StructField("test_string_7", StringType)

  val ti_0 = StructField("test_int_0", IntegerType)

  val tl_0 = StructField("test_long_0", LongType)

  val item001 = StructField("item001", StructType(Seq(ts_0, ts_1, ti_0)))
  val item005 = StructField("item005", FloatType)
  val item004 = StructField("item004", StructType(Seq(item005, ts_0)))
  val t_struct_0 = StructField("test_struct_0", StructType(Seq(item005, ts_0)))
  val item003 = StructField("item003", StructType(Seq(item004, t_struct_0, ts_2)))
  val item002 = StructField("item002", ArrayType(StructType(Seq(item003))))
  val item006 = StructField("item006", StringType)
  val t_struct_1 = StructField("test_struct_1", StructType(Seq(ts_0, ts_1, ts_2, ti_0)))
  val t_struct_2 = StructField("test_struct_2", StructType(Seq(ts_0, ts_1, ts_2, ts_3, ts_4)))
  val t_struct_3 = StructField("test_struct_3", StructType(Seq(ts_0, ts_1, ts_2, ts_3, ts_4, ts_5, ts_6)))
  val t_struct_4 = StructField("test_struct_4", StructType(Seq(ts_0, ts_1, ts_2, ts_3, ts_4, ts_5, ts_6)))
  val t_struct_5 = StructField("test_struct_5", StructType(Seq(t_struct_4)))
  val item008 = StructField("item008", ArrayType(StructType(Seq(t_struct_2, t_struct_3))))
  val item007 = StructField("item007", StructType(Seq(t_struct_1, item008, t_struct_5)))
  val item013 = StructField("item013", StringType)
  val item014 = StructField("item014", StringType)
  val item015 = StructField("item015", StringType)
  val item012 = StructField("item012", StructType(Seq(item013, ts_0, item014, ts_1, ts_2, ts_3, ts_4, item015, ts_5, ts_6, ts_7)))
  val t_struct_7 = StructField("test_struct_7", StructType(Seq(ts_0)))
  val t_struct_8 = StructField("test_struct_8", StructType(Seq(ts_0, ts_1)))
  val t_array_1 = StructField("test_array_1", ArrayType(StructType(Seq(t_struct_8))))
  val t_struct_6 = StructField("test_struct_6", StructType(Seq(t_struct_7, t_array_1)))
  val item011 = StructField("item011", StringType)
  val item084 = StructField("item084", ArrayType(StructType(Seq(item011))))
  val item010 = StructField("item010", StructType(Seq(t_struct_7, item084)))
  val t_struct_9 = StructField("test_struct_9", StructType(Seq(t_struct_7, item084)))
  val item009 = StructField("item009", StructType(Seq(t_struct_6, item010, t_struct_9)))
  val item018 = StructField("item018", StringType)
  val item030 = StructField("item030", LongType)
  val item051 = StructField("item051", StringType)
  val item052 = StructField("item052", StringType)
  val item085 = StructField("item085", ArrayType(StructType(Seq(item018, item030, item051, item052))))
  val item028 = StructField("item028", StringType)
  val item029 = StructField("item029", StringType)
  val item027 = StructField("item027", StructType(Seq(item028, item029)))
  val item017 = StructField("item017", StructType(Seq(item027, item085)))
  val item019 = StructField("item019", StructType(Seq(item027, item085)))
  val item020 = StructField("item020", StructType(Seq(item027, item085)))
  val item021 = StructField("item021", StructType(Seq(item027, item085)))
  val item022 = StructField("item022", StructType(Seq(item027, item085)))
  val item023 = StructField("item023", StructType(Seq(item027, item085)))
  val item024 = StructField("item024", StructType(Seq(item027, item085)))
  val item025 = StructField("item025", StructType(Seq(item027, item085)))
  val item026 = StructField("item026", StructType(Seq(item027, item085)))
  val item031 = StructField("item031", StructType(Seq(item027, item085)))
  val item032 = StructField("item032", StructType(Seq(item027, item085)))
  val item033 = StructField("item033", StructType(Seq(item027, item085)))
  val item034 = StructField("item034", StructType(Seq(item027, item085)))
  val item035 = StructField("item035", StructType(Seq(item027, item085)))
  val item036 = StructField("item036", StructType(Seq(item027, item085)))
  val item037 = StructField("item037", StructType(Seq(item027, item085)))
  val item038 = StructField("item038", StructType(Seq(item027, item085)))
  val item039 = StructField("item039", StructType(Seq(item027, item085)))
  val item040 = StructField("item040", StructType(Seq(item027, item085)))
  val item041 = StructField("item041", StructType(Seq(item027, item085)))
  val item042 = StructField("item042", StructType(Seq(item027, item085)))
  val item043 = StructField("item043", StructType(Seq(item027, item085)))
  val item044 = StructField("item044", StructType(Seq(item027, item085)))
  val item045 = StructField("item045", StructType(Seq(item027, item085)))
  val item046 = StructField("item046", StructType(Seq(item027, item085)))
  val item047 = StructField("item047", StructType(Seq(item027, item085)))
  val item048 = StructField("item048", StructType(Seq(item027, item085)))
  val item049 = StructField("item049", StructType(Seq(item027, item085)))
  val item050 = StructField("item050", StructType(Seq(item027, item085)))
  val item053 = StructField("item053", StructType(Seq(item027, item085)))
  val item054 = StructField("item054", StructType(Seq(item027, item085)))
  val item055 = StructField("item055", StructType(Seq(item027, item085)))
  val item056 = StructField("item056", StructType(Seq(item027, item085)))
  val item057 = StructField("item057", StructType(Seq(item027, item085)))
  val item058 = StructField("item058", StructType(Seq(item027, item085)))
  val item059 = StructField("item059", StructType(Seq(item027, item085)))
  val item060 = StructField("item060", StructType(Seq(item027, item085)))
  val item061 = StructField("item061", StructType(Seq(item027, item085)))
  val item062 = StructField("item062", StructType(Seq(item027, item085)))
  val item016 = StructField("item016", StructType(Seq(item017, item019,
    item020, item021, item022, item023, item024, item025, item026,
    item031, item032, item033, item034, item035, item036, item037, item038, item039,
    item040, item041, item042, item043, item044, item045, item046, item047, item048, item049,
    item050, item053, item054, item055, item056, item057, item058, item059,
    item060, item061, item062)))

  val item063 = StructField("item063", StringType)

  val item065 = StructField("item065", StringType)
  val item064 = StructField("item064", ArrayType(StructType(Seq(ti_0, ts_0, ts_1, item065, tl_0))))

  val columnA = StructField("columnA", StructType(Seq(item001)))
  val columnB = StructField("columnB", ArrayType(StructType(Seq(item002))))
  val columnC = StructField("columnC", StructType(Seq(ts_0, item063, ts_2, ti_0, tl_0, ts_3, item006, t_struct_4, item016, item012, item009, item007, item064)))

  val jsonTable = DBGen().addTable("json_data", StructType(Seq(columnA, columnB, columnC)), numRows)

  jsonTable("columnB").setLength(1,3)
  jsonTable("columnB")("data")("item002").setLength(1,2)
  jsonTable("columnB")("data")("item002")("data")("item003")("item004")("test_string_0").setLength(2)
  jsonTable("columnB")("data")("item002")("data")("item003")("test_struct_0")("test_string_0").setLength(2)

  jsonTable("columnC")("item007")("item008").setLength(2)
  jsonTable("columnC")("item007")("item008")("data")("test_struct_2")("test_string_0").setLength(140,200)
  jsonTable("columnC")("item007")("item008")("data")("test_struct_2")("test_string_1").setLength(140,200)
  jsonTable("columnC")("item007")("item008")("data")("test_struct_2")("test_string_2").setLength(140,200)
  jsonTable("columnC")("item007")("item008")("data")("test_struct_2")("test_string_3").setLength(140,200)
  jsonTable("columnC")("item007")("item008")("data")("test_struct_2")("test_string_4").setLength(140,200)

  jsonTable("columnC")("item009")("item010")("item084").setLength(2,5)
  jsonTable("columnC")("item009")("test_struct_9")("item084").setLength(2,4)
  jsonTable("columnC")("item016")("item017")("item085").setLength(2,3)
  jsonTable("columnC")("item016")("item019")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item020")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item021")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item022")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item023")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item024")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item025")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item026")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item031")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item032")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item033")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item034")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item035")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item036")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item037")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item038")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item039")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item040")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item041")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item042")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item043")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item044")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item045")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item046")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item047")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item048")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item049")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item050")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item053")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item054")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item055")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item056")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item057")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item058")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item059")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item060")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item061")("item085").setLength(1,2)
  jsonTable("columnC")("item016")("item062")("item085").setLength(1,2)

  jsonTable.setNullProbabilityRecursively(nullProbability)

  jsonTable.toDF(spark).selectExpr("to_json(columnA) as columnA", "to_json(columnB) as columnB",
    "to_json(columnC) as columnC").write.mode("overwrite").parquet(output)

  spark.time(spark.read.parquet(output).selectExpr("get_json_object(columnA, '$.item001') as A_001").show())
  spark.time(spark.read.parquet(output).selectExpr("get_json_object(columnB, '$[0].item002[0].item003.item004.item005') as B_005").show())
  spark.time(spark.read.parquet(output).selectExpr("get_json_object(columnC, '$.item006') as C_006",
    "get_json_object(columnC, '$.item007.item008') as C_008",
    "get_json_object(columnC, '$.item009.item010.item084[0].item011') as C_011",
    "get_json_object(columnC, '$.item012.item013') as C_013",
    "get_json_object(columnC, '$.item012.item014') as C_014",
    "get_json_object(columnC, '$.item012.item015') as C_015",
    "get_json_object(columnC, '$.item016.item017.item085[*].item018') as C_017",
    "get_json_object(columnC, '$.item016.item019.item085[0].item018') as C_019",
    "get_json_object(columnC, '$.item016.item020.item085[0].item018') as C_020",
    "get_json_object(columnC, '$.item016.item021.item085[0].item018') as C_021",
    "get_json_object(columnC, '$.item016.item022.item085[0].item018') as C_022",
    "get_json_object(columnC, '$.item016.item023.item085[0].item018') as C_023",
    "get_json_object(columnC, '$.item016.item024.item085[0].item018') as C_024",
    "get_json_object(columnC, '$.item016.item025.item085[0].item018') as C_025",
    "get_json_object(columnC, '$.item016.item026.item085[0].item018') as C_026_18",
    "get_json_object(columnC, '$.item016.item026.item085[0].item030') as C_026_30",
    "get_json_object(columnC, '$.item016.item026.item027.item028') as C_028",
    "get_json_object(columnC, '$.item016.item026.item027.item029') as C_029",
    "get_json_object(columnC, '$.item016.item031.item085[0].item018') as C_031",
    "get_json_object(columnC, '$.item016.item032.item085[0].item018') as C_032",
    "get_json_object(columnC, '$.item016.item033.item085[0].item018') as C_033",
    "get_json_object(columnC, '$.item016.item034.item085[0].item018') as C_034",
    "get_json_object(columnC, '$.item016.item035.item085[0].item018') as C_035",
    "get_json_object(columnC, '$.item016.item036.item085[0].item018') as C_036",
    "get_json_object(columnC, '$.item016.item037.item085[0].item018') as C_037",
    "get_json_object(columnC, '$.item016.item038.item085[0].item018') as C_038",
    "get_json_object(columnC, '$.item016.item039.item085[0].item018') as C_039",
    "get_json_object(columnC, '$.item016.item040.item085[0].item018') as C_040",
    "get_json_object(columnC, '$.item016.item041.item085[0].item018') as C_041",
    "get_json_object(columnC, '$.item016.item042.item085[0].item018') as C_042",
    "get_json_object(columnC, '$.item016.item043.item085[0].item018') as C_043",
    "get_json_object(columnC, '$.item016.item044.item085[0].item018') as C_044",
    "get_json_object(columnC, '$.item016.item045.item085[0].item018') as C_045",
    "get_json_object(columnC, '$.item016.item046.item085[0].item018') as C_046",
    "get_json_object(columnC, '$.item016.item047.item085[0].item018') as C_047",
    "get_json_object(columnC, '$.item016.item048.item085[0].item018') as C_048",
    "get_json_object(columnC, '$.item016.item049.item085[0].item018') as C_049",
    "get_json_object(columnC, '$.item016.item050.item085[0].item051') as C_051",
    "get_json_object(columnC, '$.item016.item050.item085[0].item052') as C_052",
    "get_json_object(columnC, '$.item016.item053.item085[0].item018') as C_053",
    "get_json_object(columnC, '$.item016.item054.item085[0].item018') as C_054",
    "get_json_object(columnC, '$.item016.item055.item027.item028') as C_055_28",
    "get_json_object(columnC, '$.item016.item055.item027.item029') as C_055_29",
    "get_json_object(columnC, '$.item016.item055.item085[0].item030') as C_055_30",
    "get_json_object(columnC, '$.item016.item055.item085[0].item018') as C_055_18",
    "get_json_object(columnC, '$.item016.item056.item027.item028') as C_056_28",
    "get_json_object(columnC, '$.item016.item056.item027.item029') as C_056_29",
    "get_json_object(columnC, '$.item016.item056[0].item018') as C_056_29",
    "get_json_object(columnC, '$.item016.item057.item085[0].item018') as C_057",
    "get_json_object(columnC, '$.item016.item058.item085[0].item018') as C_058",
    "get_json_object(columnC, '$.item016.item059.item085[0].item018') as C_059",
    "get_json_object(columnC, '$.item016.item060.item085[0].item051') as C_60_51",
    "get_json_object(columnC, '$.item016.item060.item085[0].item052') as C_60_52",
    "get_json_object(columnC, '$.item016.item061.item085[0].item018') as C_061",
    "get_json_object(columnC, '$.item016.item062.item085[0].item018') as C_062",
    "get_json_object(columnC, '$.item063') as C_063",
    "get_json_object(columnC, '$.item064[*].item065') as C_065"
    ).show())

  spark.read.parquet(output).selectExpr("AVG(octet_length(columnA))", "AVG(octet_length(columnB))",
    "AVG(octet_length(columnC))").show()
}

doIt()

