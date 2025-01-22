/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.hybrid

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.shims.HybridFileSourceScanExecMeta

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

object HybridScanUtils {
  val supportedByHybridFilters = {
    // Only fully supported functions are listed here
    val ansiOn = Seq(
      classOf[Acos],
      classOf[Acosh],
      classOf[AddMonths],
      classOf[And],
      classOf[ArrayAggregate],
      classOf[ArrayContains],
      classOf[ArrayDistinct],
      classOf[ArrayExcept],
      classOf[ArrayExists],
      classOf[ArrayForAll],
      classOf[ArrayIntersect],
      classOf[ArrayJoin],
      classOf[ArrayMax],
      classOf[ArrayMin],
      classOf[ArrayPosition],
      classOf[ArrayRemove],
      classOf[ArrayRepeat],
      classOf[ArraySort],
      classOf[ArraysZip],
      classOf[Ascii],
      classOf[Asin],
      classOf[Asinh],
      classOf[Atan],
      classOf[Atan2],
      classOf[Atanh],
      classOf[BitLength],
      classOf[BitwiseAnd],
      classOf[BitwiseOr],
      classOf[Cbrt],
      classOf[Ceil],
      classOf[Chr],
      classOf[Concat],
      classOf[Cos],
      classOf[Cosh],
      classOf[Crc32],
      classOf[CreateNamedStruct],
      classOf[DateAdd],
      classOf[DateDiff],
      classOf[DateFormatClass],
      classOf[DateFromUnixDate],
      classOf[DateSub],
      classOf[DayOfMonth],
      classOf[DayOfWeek],
      classOf[DayOfYear],
      classOf[ElementAt],
      classOf[EqualNullSafe],
      classOf[EqualTo],
      classOf[Exp],
      classOf[Expm1],
      classOf[FindInSet],
      classOf[Flatten],
      classOf[Floor],
      classOf[FromUTCTimestamp],
      classOf[FromUnixTime],
      classOf[GetJsonObject],
      classOf[GetMapValue],
      classOf[GreaterThan],
      classOf[GreaterThanOrEqual],
      classOf[Greatest],
      classOf[Hex],
      classOf[Hour],
      classOf[If],
      classOf[In],
      classOf[IsNaN],
      classOf[IsNotNull],
      classOf[IsNull],
      classOf[LastDay],
      classOf[Least],
      classOf[Left],
      classOf[Length],
      classOf[LengthOfJsonArray],
      classOf[LessThan],
      classOf[Levenshtein],
      classOf[Like],
      classOf[Log],
      classOf[Log10],
      classOf[Log2],
      classOf[Lower],
      classOf[MapFromArrays],
      classOf[MapKeys],
      classOf[MapValues],
      classOf[MapZipWith],
      classOf[Md5],
      classOf[MicrosToTimestamp],
      classOf[MillisToTimestamp],
      classOf[Minute],
      classOf[MonotonicallyIncreasingID],
      classOf[Month],
      classOf[NaNvl],
      classOf[NextDay],
      classOf[Not],
      classOf[Or],
      classOf[Overlay],
      classOf[Pi],
      classOf[Pow],
      classOf[Quarter],
      classOf[Rand],
      classOf[Remainder],
      classOf[Reverse],
      classOf[Rint],
      classOf[Round],
      classOf[RoundCeil],
      classOf[RoundFloor],
      classOf[Second],
      classOf[Sha1],
      classOf[Sha2],
      classOf[ShiftLeft],
      classOf[ShiftRight],
      classOf[Shuffle],
      classOf[Sin],
      classOf[Size],
      classOf[SortArray],
      classOf[SoundEx],
      classOf[SparkPartitionID],
      classOf[Sqrt],
      classOf[Stack],
      classOf[StringInstr],
      classOf[StringLPad],
      classOf[StringRPad],
      classOf[StringRepeat],
      classOf[StringReplace],
      classOf[StringToMap],
      classOf[StringTrim],
      classOf[StringTrimLeft],
      classOf[StringTrimRight],
      classOf[Substring],
      classOf[SubstringIndex],
      classOf[Tan],
      classOf[Tanh],
      classOf[ToDegrees],
      classOf[ToRadians],
      classOf[ToUnixTimestamp],
      classOf[UnaryPositive],
      classOf[Unhex],
      classOf[UnixMicros],
      classOf[UnixMillis],
      classOf[UnixSeconds],
      classOf[Upper],
      classOf[Uuid],
      classOf[WeekDay],
      classOf[WeekOfYear],
      classOf[WidthBucket],
      classOf[Year],
      classOf[ZipWith]
    )

    // Some functions are fully supported with ANSI mode off
    lazy val ansiOff = Seq(
      classOf[Remainder],
      classOf[Multiply],
      classOf[Add],
      classOf[Subtract],
      classOf[Divide],
      classOf[Abs],
      classOf[Pmod]
    )

    if (SQLConf.get.ansiEnabled) {
      ansiOn
    } else {
      ansiOn ++ ansiOff
    }
  }

  def canBePushedToHybrid(child: SparkPlan, conf: RapidsConf): String = {
    child match {
      case fsse: FileSourceScanExec if HybridFileSourceScanExecMeta.useHybridScan(conf, fsse) =>
        conf.pushDownFiltersToHybrid
      case _ => "OFF"
    }
  }
}
