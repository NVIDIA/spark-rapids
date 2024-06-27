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

val input = "/data/tmp/SCALE_FROM_JSON"
val iters = 3

def doIt(): Unit = {
  (0 until iters).foreach { _ =>
    spark.time(spark.read.parquet(input).selectExpr(
      "SUM(octet_length(get_json_object(columnA, '$.item001'))) as A_001",
      "SUM(octet_length(get_json_object(columnB, '$[0].item002[0].item003.item004.item005'))) as B_005",
      "SUM(octet_length(get_json_object(columnC, '$.item006'))) as C_006",
      "SUM(octet_length(get_json_object(columnC, '$.item007.item008'))) as C_008",
      "SUM(octet_length(get_json_object(columnC, '$.item009.item010.item084[0].item011'))) as C_011",
      "SUM(octet_length(get_json_object(columnC, '$.item012.item013'))) as C_013",
      "SUM(octet_length(get_json_object(columnC, '$.item012.item014'))) as C_014",
      "SUM(octet_length(get_json_object(columnC, '$.item012.item015'))) as C_015",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item017.item085[*].item018'))) as C_017",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item019.item085[0].item018'))) as C_019",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item020.item085[0].item018'))) as C_020",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item021.item085[0].item018'))) as C_021",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item022.item085[0].item018'))) as C_022",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item023.item085[0].item018'))) as C_023",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item024.item085[0].item018'))) as C_024",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item025.item085[0].item018'))) as C_025",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item026.item085[0].item018'))) as C_026_18",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item026.item085[0].item030'))) as C_026_30",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item026.item027.item028'))) as C_028",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item026.item027.item029'))) as C_029",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item031.item085[0].item018'))) as C_031",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item032.item085[0].item018'))) as C_032",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item033.item085[0].item018'))) as C_033",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item034.item085[0].item018'))) as C_034",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item035.item085[0].item018'))) as C_035",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item036.item085[0].item018'))) as C_036",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item037.item085[0].item018'))) as C_037",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item038.item085[0].item018'))) as C_038",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item039.item085[0].item018'))) as C_039",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item040.item085[0].item018'))) as C_040",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item041.item085[0].item018'))) as C_041",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item042.item085[0].item018'))) as C_042",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item043.item085[0].item018'))) as C_043",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item044.item085[0].item018'))) as C_044",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item045.item085[0].item018'))) as C_045",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item046.item085[0].item018'))) as C_046",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item047.item085[0].item018'))) as C_047",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item048.item085[0].item018'))) as C_048",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item049.item085[0].item018'))) as C_049",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item050.item085[0].item051'))) as C_051",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item050.item085[0].item052'))) as C_052",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item053.item085[0].item018'))) as C_053",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item054.item085[0].item018'))) as C_054",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item055.item027.item028'))) as C_055_28",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item055.item027.item029'))) as C_055_29",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item055.item085[0].item030'))) as C_055_30",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item055.item085[0].item018'))) as C_055_18",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item056.item027.item028'))) as C_056_28",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item056.item027.item029'))) as C_056_29",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item056[0].item018'))) as C_056_29",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item057.item085[0].item018'))) as C_057",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item058.item085[0].item018'))) as C_058",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item059.item085[0].item018'))) as C_059",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item060.item085[0].item051'))) as C_60_51",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item060.item085[0].item052'))) as C_60_52",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item061.item085[0].item018'))) as C_061",
      "SUM(octet_length(get_json_object(columnC, '$.item016.item062.item085[0].item018'))) as C_062",
      "SUM(octet_length(get_json_object(columnC, '$.item063'))) as C_063",
      "SUM(octet_length(get_json_object(columnC, '$.item064[*].item065'))) as C_065"
    ).show())
  }

  (0 until iters).foreach { _ =>
    spark.time(spark.read.parquet(input).selectExpr("SUM(octet_length(columnA))", "SUM(octet_length(columnB))",
      "SUM(octet_length(columnC))").show())
  }
}

doIt()

