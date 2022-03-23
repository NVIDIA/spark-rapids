#!/bin/bash
#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
#

# This scripts diffs the code in spark2-sql-module with the corresponding files and functions in
# sql-plugin to look for anything that has changed

# Generally speaking this assumes the convertToGpu is the last function in the meta classes,
# if someone adds something after it we may not catch it.
# This also doesn't catch if someone adds an override in a shim or someplace we don't diff.

# just using interface, and we don't really expect them to use it on 2.x so just skip diffing
#  ../spark2-sql-plugin/src/main/java/com/nvidia/spark/RapidsUDF.java

# If this script fails then a developer should do something like:
#  1. Look at each file with a diff output from the script
#  2. Look at the commits for that file in sql-plugin module and see what changed dealing
#     with the metadata.
#  3. update the corresponding spark2-sql-plugin file to pick up the meta changes if necessary
#  4. Rerun the diff script: cd scripts && ./rundiffspark2.sh
#  5. If there is still diffs update the diff file in spark2diffs/ corresponding to the
#     changed file.
#     Generally the way I do this is find the commands below for the file that changed
#     and run them manually. If the diff looks ok then just copy the .newdiff file to
#     the diff file in the spark2diffs directory.
#

set -e

echo "Done running Diffs of spark2 files"

tmp_dir=$(mktemp -d -t spark2diff-XXXXXXXXXX)
echo "Using temporary directory: $tmp_dir"

diff ../sql-plugin/src/main/scala/org/apache/spark/sql/hive/rapids/GpuHiveOverrides.scala  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/hive/rapids/GpuHiveOverrides.scala > $tmp_dir/GpuHiveOverrides.newdiff || true
if [[ $(diff spark2diffs/GpuHiveOverrides.diff $tmp_dir/GpuHiveOverrides.newdiff) ]]; then
  echo "check diff for  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/hive/rapids/GpuHiveOverrides.scala"
fi

sed -n  '/class GpuBroadcastNestedLoopJoinMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastNestedLoopJoinMeta.scala > $tmp_dir/GpuBroadcastNestedLoopJoinMeta_new.out
sed -n  '/class GpuBroadcastNestedLoopJoinMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastNestedLoopJoinExec.scala > $tmp_dir/GpuBroadcastNestedLoopJoinMeta_old.out
diff $tmp_dir/GpuBroadcastNestedLoopJoinMeta_new.out $tmp_dir/GpuBroadcastNestedLoopJoinMeta_old.out > $tmp_dir/GpuBroadcastNestedLoopJoinMeta.newdiff || true
diff -c spark2diffs/GpuBroadcastNestedLoopJoinMeta.diff $tmp_dir/GpuBroadcastNestedLoopJoinMeta.newdiff

sed -n  '/object JoinTypeChecks/,/def extractTopLevelAttributes/{/def extractTopLevelAttributes/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuHashJoin.scala > $tmp_dir/GpuHashJoin_new.out
sed -n  '/object JoinTypeChecks/,/def extractTopLevelAttributes/{/def extractTopLevelAttributes/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuHashJoin.scala > $tmp_dir/GpuHashJoin_old.out
diff $tmp_dir/GpuHashJoin_new.out $tmp_dir/GpuHashJoin_old.out > $tmp_dir/GpuHashJoin.newdiff || true
diff -c spark2diffs/GpuHashJoin.diff $tmp_dir/GpuHashJoin.newdiff

sed -n  '/class GpuShuffleMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuShuffleMeta.scala > $tmp_dir/GpuShuffleMeta_new.out
sed -n  '/class GpuShuffleMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuShuffleExchangeExecBase.scala > $tmp_dir/GpuShuffleMeta_old.out
diff $tmp_dir/GpuShuffleMeta_new.out $tmp_dir/GpuShuffleMeta_old.out > $tmp_dir/GpuShuffleMeta.newdiff || true
diff -c spark2diffs/GpuShuffleMeta.diff $tmp_dir/GpuShuffleMeta.newdiff

sed -n  '/class GpuBroadcastMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastExchangeExecMeta.scala > $tmp_dir/GpuBroadcastExchangeExecMeta_new.out
sed -n  '/class GpuBroadcastMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastExchangeExec.scala > $tmp_dir/GpuBroadcastExchangeExecMeta_old.out
diff $tmp_dir/GpuBroadcastExchangeExecMeta_new.out $tmp_dir/GpuBroadcastExchangeExecMeta_old.out > $tmp_dir/GpuBroadcastExchangeExecMeta.newdiff || true
diff -c spark2diffs/GpuBroadcastExchangeExecMeta.diff $tmp_dir/GpuBroadcastExchangeExecMeta.newdiff

sed -n  '/abstract class UnixTimeExprMeta/,/sealed trait TimeParserPolicy/{/sealed trait TimeParserPolicy/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressionsMeta.scala > $tmp_dir/UnixTimeExprMeta_new.out
sed -n  '/abstract class UnixTimeExprMeta/,/sealed trait TimeParserPolicy/{/sealed trait TimeParserPolicy/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/UnixTimeExprMeta_old.out
diff $tmp_dir/UnixTimeExprMeta_new.out $tmp_dir/UnixTimeExprMeta_old.out > $tmp_dir/UnixTimeExprMeta.newdiff || true
diff -c spark2diffs/UnixTimeExprMeta.diff $tmp_dir/UnixTimeExprMeta.newdiff

sed -n  '/object GpuToTimestamp/,/abstract class UnixTimeExprMeta/{/abstract class UnixTimeExprMeta/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressionsMeta.scala > $tmp_dir/GpuToTimestamp_new.out
sed -n  '/object GpuToTimestamp/,/val REMOVE_WHITESPACE_FROM_MONTH_DAY/{/val REMOVE_WHITESPACE_FROM_MONTH_DAY/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/GpuToTimestamp_old.out
diff $tmp_dir/GpuToTimestamp_new.out $tmp_dir/GpuToTimestamp_old.out > $tmp_dir/GpuToTimestamp.newdiff || true
diff -c spark2diffs/GpuToTimestamp.diff $tmp_dir/GpuToTimestamp.newdiff

sed -n  '/case class ParseFormatMeta/,/case class RegexReplace/p'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressionsMeta.scala > $tmp_dir/datemisc_new.out
sed -n  '/case class ParseFormatMeta/,/case class RegexReplace/p'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/datemisc_old.out
diff -c $tmp_dir/datemisc_new.out $tmp_dir/datemisc_old.out

sed -n  '/class GpuRLikeMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringMeta.scala > $tmp_dir/GpuRLikeMeta_new.out
sed -n  '/class GpuRLikeMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringFunctions.scala > $tmp_dir/GpuRLikeMeta_old.out
diff $tmp_dir/GpuRLikeMeta_new.out $tmp_dir/GpuRLikeMeta_old.out > $tmp_dir/GpuRLikeMeta.newdiff || true
diff -c spark2diffs/GpuRLikeMeta.diff  $tmp_dir/GpuRLikeMeta.newdiff

sed -n  '/class GpuRegExpExtractMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringMeta.scala > $tmp_dir/GpuRegExpExtractMeta_new.out
sed -n  '/class GpuRegExpExtractMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringFunctions.scala > $tmp_dir/GpuRegExpExtractMeta_old.out
diff $tmp_dir/GpuRegExpExtractMeta_new.out $tmp_dir/GpuRegExpExtractMeta_old.out > $tmp_dir/GpuRegExpExtractMeta.newdiff || true
diff -c spark2diffs/GpuRegExpExtractMeta.diff $tmp_dir/GpuRegExpExtractMeta.newdiff

sed -n  '/class SubstringIndexMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringMeta.scala > $tmp_dir/SubstringIndexMeta_new.out
sed -n  '/class SubstringIndexMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringFunctions.scala > $tmp_dir/SubstringIndexMeta_old.out
diff $tmp_dir/SubstringIndexMeta_new.out $tmp_dir/SubstringIndexMeta_old.out > $tmp_dir/SubstringIndexMeta.newdiff || true
diff -c spark2diffs/SubstringIndexMeta.diff  $tmp_dir/SubstringIndexMeta.newdiff

sed -n  '/object CudfRegexp/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringMeta.scala > $tmp_dir/CudfRegexp_new.out
sed -n  '/object CudfRegexp/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringFunctions.scala > $tmp_dir/CudfRegexp_old.out
diff -c $tmp_dir/CudfRegexp_new.out $tmp_dir/CudfRegexp_old.out > $tmp_dir/CudfRegexp.newdiff

sed -n  '/object GpuSubstringIndex/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringMeta.scala > $tmp_dir/GpuSubstringIndex_new.out
sed -n  '/object GpuSubstringIndex/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringFunctions.scala > $tmp_dir/GpuSubstringIndex_old.out
diff -c $tmp_dir/GpuSubstringIndex_new.out $tmp_dir/GpuSubstringIndex_old.out > $tmp_dir/GpuSubstringIndex.newdiff

sed -n  '/class GpuStringSplitMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringMeta.scala > $tmp_dir/GpuStringSplitMeta_new.out
sed -n  '/class GpuStringSplitMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringFunctions.scala > $tmp_dir/GpuStringSplitMeta_old.out
diff $tmp_dir/GpuStringSplitMeta_new.out $tmp_dir/GpuStringSplitMeta_old.out > $tmp_dir/GpuStringSplitMeta.newdiff || true
diff -c spark2diffs/GpuStringSplitMeta.diff  $tmp_dir/GpuStringSplitMeta.newdiff

sed -n  '/object GpuOrcFileFormat/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuOrcFileFormat.scala > $tmp_dir/GpuOrcFileFormat_new.out
sed -n  '/object GpuOrcFileFormat/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuOrcFileFormat.scala  > $tmp_dir/GpuOrcFileFormat_old.out
diff  $tmp_dir/GpuOrcFileFormat_new.out $tmp_dir/GpuOrcFileFormat_old.out > $tmp_dir/GpuOrcFileFormat.newdiff || true
diff -c spark2diffs/GpuOrcFileFormat.diff $tmp_dir/GpuOrcFileFormat.newdiff

sed -n  '/class GpuSequenceMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/collectionOperations.scala > $tmp_dir/GpuSequenceMeta_new.out
sed -n  '/class GpuSequenceMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/collectionOperations.scala > $tmp_dir/GpuSequenceMeta_old.out
diff $tmp_dir/GpuSequenceMeta_new.out $tmp_dir/GpuSequenceMeta_old.out > $tmp_dir/GpuSequenceMeta.newdiff || true
diff -c spark2diffs/GpuSequenceMeta.diff  $tmp_dir/GpuSequenceMeta.newdiff

sed -n  '/object GpuDataSource/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuDataSource.scala > $tmp_dir/GpuDataSource_new.out
sed -n  '/object GpuDataSource/,/val GLOB_PATHS_KEY/{/val GLOB_PATHS_KEY/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuDataSource.scala > $tmp_dir/GpuDataSource_old.out
diff  $tmp_dir/GpuDataSource_new.out $tmp_dir/GpuDataSource_old.out > $tmp_dir/GpuDataSource.newdiff || true
diff -c spark2diffs/GpuDataSource.diff $tmp_dir/GpuDataSource.newdiff

sed -n  '/class GpuGetArrayItemMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/complexTypeExtractors.scala  > $tmp_dir/GpuGetArrayItemMeta_new.out
sed -n  '/class GpuGetArrayItemMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/complexTypeExtractors.scala > $tmp_dir/GpuGetArrayItemMeta_old.out
diff $tmp_dir/GpuGetArrayItemMeta_new.out $tmp_dir/GpuGetArrayItemMeta_old.out > $tmp_dir/GpuGetArrayItemMeta.newdiff || true
diff -c spark2diffs/GpuGetArrayItemMeta.diff  $tmp_dir/GpuGetArrayItemMeta.newdiff

sed -n  '/class GpuGetMapValueMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/complexTypeExtractors.scala  > $tmp_dir/GpuGetMapValueMeta_new.out
sed -n  '/class GpuGetMapValueMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/complexTypeExtractors.scala > $tmp_dir/GpuGetMapValueMeta_old.out
diff $tmp_dir/GpuGetMapValueMeta_new.out $tmp_dir/GpuGetMapValueMeta_old.out > $tmp_dir/GpuGetMapValueMeta.newdiff || true
diff -c spark2diffs/GpuGetMapValueMeta.diff  $tmp_dir/GpuGetMapValueMeta.newdiff

sed -n  '/abstract class ScalaUDFMetaBase/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuScalaUDFMeta.scala> $tmp_dir/ScalaUDFMetaBase_new.out
sed -n  '/abstract class ScalaUDFMetaBase/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuScalaUDF.scala > $tmp_dir/ScalaUDFMetaBase_old.out
diff $tmp_dir/ScalaUDFMetaBase_new.out $tmp_dir/ScalaUDFMetaBase_old.out > $tmp_dir/ScalaUDFMetaBase.newdiff || true
diff -c spark2diffs/ScalaUDFMetaBase.diff  $tmp_dir/ScalaUDFMetaBase.newdiff

sed -n  '/object GpuScalaUDF/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuScalaUDFMeta.scala > $tmp_dir/GpuScalaUDF_new.out
sed -n  '/object GpuScalaUDF/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuScalaUDF.scala > $tmp_dir/GpuScalaUDF_old.out
diff  -c $tmp_dir/GpuScalaUDF_new.out $tmp_dir/GpuScalaUDF_old.out > $tmp_dir/GpuScalaUDF.newdiff

sed -n  '/object GpuDecimalMultiply/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala > $tmp_dir/GpuDecimalMultiply_new.out
sed -n  '/object GpuDecimalMultiply/,/def checkForOverflow/{/def checkForOverflow/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala > $tmp_dir/GpuDecimalMultiply_old.out
diff  $tmp_dir/GpuDecimalMultiply_new.out $tmp_dir/GpuDecimalMultiply_old.out > $tmp_dir/GpuDecimalMultiply.newdiff || true
diff -c spark2diffs/GpuDecimalMultiply.diff $tmp_dir/GpuDecimalMultiply.newdiff

sed -n  '/object GpuDecimalDivide/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala > $tmp_dir/GpuDecimalDivide_new.out
sed -n  '/object GpuDecimalDivide/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala > $tmp_dir/GpuDecimalDivide_old.out
diff  $tmp_dir/GpuDecimalDivide_new.out $tmp_dir/GpuDecimalDivide_old.out > $tmp_dir/GpuDecimalDivide.newdiff || true
diff -c spark2diffs/GpuDecimalDivide.diff $tmp_dir/GpuDecimalDivide.newdiff

sed -n  '/def isSupportedRelation/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/TrampolineUtil.scala > $tmp_dir/isSupportedRelation_new.out
sed -n  '/def isSupportedRelation/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/TrampolineUtil.scala > $tmp_dir/isSupportedRelation_old.out
diff  -c $tmp_dir/isSupportedRelation_new.out $tmp_dir/isSupportedRelation_old.out 

sed -n  '/def dataTypeExistsRecursively/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/TrampolineUtil.scala > $tmp_dir/dataTypeExistsRecursively_new.out
sed -n  '/def dataTypeExistsRecursively/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/TrampolineUtil.scala > $tmp_dir/dataTypeExistsRecursively_old.out
diff  -c $tmp_dir/dataTypeExistsRecursively_new.out $tmp_dir/dataTypeExistsRecursively_old.out 

sed -n  '/def getSimpleName/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/TrampolineUtil.scala > $tmp_dir/getSimpleName_new.out
sed -n  '/def getSimpleName/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/TrampolineUtil.scala > $tmp_dir/getSimpleName_old.out
diff  -c $tmp_dir/getSimpleName_new.out $tmp_dir/getSimpleName_old.out 

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/RegexParser.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/RegexParser.scala > $tmp_dir/RegexParser.newdiff || true
diff -c spark2diffs/RegexParser.diff $tmp_dir/RegexParser.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/TypeChecks.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/TypeChecks.scala > $tmp_dir/TypeChecks.newdiff || true
diff -c spark2diffs/TypeChecks.diff  $tmp_dir/TypeChecks.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DataTypeUtils.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/DataTypeUtils.scala > $tmp_dir/DataTypeUtils.newdiff || true
diff -c spark2diffs/DataTypeUtils.diff $tmp_dir/DataTypeUtils.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala > $tmp_dir/GpuOverrides.newdiff || true
diff -c spark2diffs/GpuOverrides.diff $tmp_dir/GpuOverrides.newdiff

sed -n  '/GpuOverrides.expr\[Cast\]/,/doFloatToIntCheck/p'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/cast_new.out
sed -n  '/GpuOverrides.expr\[Cast\]/,/doFloatToIntCheck/p'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/cast_old.out
diff $tmp_dir/cast_new.out $tmp_dir/cast_old.out > $tmp_dir/cast.newdiff || true
diff -c spark2diffs/cast.diff $tmp_dir/cast.newdiff

sed -n  '/GpuOverrides.expr\[Average\]/,/GpuOverrides.expr\[Abs/p'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/average_new.out
sed -n  '/GpuOverrides.expr\[Average\]/,/GpuOverrides.expr\[Abs/p'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/average_old.out
diff -w $tmp_dir/average_new.out $tmp_dir/average_old.out > $tmp_dir/average.newdiff || true
diff -c spark2diffs/average.diff $tmp_dir/average.newdiff

sed -n  '/GpuOverrides.expr\[Abs\]/,/GpuOverrides.expr\[RegExpReplace/p'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/abs_new.out
sed -n  '/GpuOverrides.expr\[Abs\]/,/GpuOverrides.expr\[RegExpReplace/p'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/abs_old.out
diff -w $tmp_dir/abs_new.out $tmp_dir/abs_old.out > $tmp_dir/abs.newdiff || true
diff -c spark2diffs/abs.diff $tmp_dir/abs.newdiff

sed -n  '/GpuOverrides.expr\[RegExpReplace\]/,/GpuOverrides.expr\[TimeSub/{/GpuOverrides.expr\[TimeSub/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/regexreplace_new.out
sed -n  '/GpuOverrides.expr\[RegExpReplace\]/,/GpuOverrides.expr\[Lead/{/GpuOverrides.expr\[Lead/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/regexreplace_old.out
diff -w $tmp_dir/regexreplace_new.out $tmp_dir/regexreplace_old.out > $tmp_dir/regexreplace.newdiff || true
diff -c spark2diffs/regexreplace.diff $tmp_dir/regexreplace.newdiff

sed -n  '/GpuOverrides.expr\[TimeSub\]/,/GpuOverrides.expr\[ScalaUDF/{/GpuOverrides.expr\[ScalaUDF/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/TimeSub_new.out
sed -n  '/GpuOverrides.expr\[TimeSub\]/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/TimeSub_old.out
diff -w $tmp_dir/TimeSub_new.out $tmp_dir/TimeSub_old.out > $tmp_dir/TimeSub.newdiff || true
diff -c spark2diffs/TimeSub.diff $tmp_dir/TimeSub.newdiff

sed -n  '/GpuOverrides.expr\[ScalaUDF\]/,/})/{/})/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/ScalaUDF_new.out
sed -n  '/GpuOverrides.expr\[ScalaUDF\]/,/})/{/})/!p}'  ../sql-plugin/src/main/301until310-all/scala/com/nvidia/spark/rapids/shims/v2/GpuRowBasedScalaUDF.scala > $tmp_dir/ScalaUDF_old.out
diff -w $tmp_dir/ScalaUDF_new.out $tmp_dir/ScalaUDF_old.out > $tmp_dir/ScalaUDF.newdiff || true
diff -c spark2diffs/ScalaUDF.diff $tmp_dir/ScalaUDF.newdiff

sed -n  '/GpuOverrides.exec\[FileSourceScanExec\]/,/})/{/})/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/FileSourceScanExec_new.out
sed -n  '/GpuOverrides.exec\[FileSourceScanExec\]/,/override def convertToCpu/{/override def convertToCpu/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/FileSourceScanExec_old.out
diff -w $tmp_dir/FileSourceScanExec_new.out $tmp_dir/FileSourceScanExec_old.out > $tmp_dir/FileSourceScanExec.newdiff || true
diff -c spark2diffs/FileSourceScanExec.diff $tmp_dir/FileSourceScanExec.newdiff

sed -n  '/GpuOverrides.exec\[ArrowEvalPythonExec\]/,/})/{/})/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/ArrowEvalPythonExec_new.out
sed -n  '/GpuOverrides.exec\[ArrowEvalPythonExec\]/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/ArrowEvalPythonExec_old.out
diff -w $tmp_dir/ArrowEvalPythonExec_new.out $tmp_dir/ArrowEvalPythonExec_old.out > $tmp_dir/ArrowEvalPythonExec.newdiff || true
diff -c spark2diffs/ArrowEvalPythonExec.diff $tmp_dir/ArrowEvalPythonExec.newdiff

sed -n  '/GpuOverrides.exec\[FlatMapGroupsInPandasExec\]/,/GpuOverrides.exec\[WindowInPandasExec/{/GpuOverrides.exec\[WindowInPandasExec/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/FlatMapGroupsInPandasExec_new.out
sed -n  '/GpuOverrides.exec\[FlatMapGroupsInPandasExec\]/,/GpuOverrides.exec\[AggregateInPandasExec/{/GpuOverrides.exec\[AggregateInPandasExec/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/FlatMapGroupsInPandasExec_old.out
diff -c -w $tmp_dir/FlatMapGroupsInPandasExec_new.out $tmp_dir/FlatMapGroupsInPandasExec_old.out

sed -n  '/GpuOverrides.exec\[WindowInPandasExec\]/,/})/{/})/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/WindowInPandasExec_new.out
sed -n  '/GpuOverrides.exec\[WindowInPandasExec\]/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/WindowInPandasExec_old.out
diff -c -w --ignore-blank-lines $tmp_dir/WindowInPandasExec_new.out $tmp_dir/WindowInPandasExec_old.out

sed -n  '/GpuOverrides.exec\[AggregateInPandasExec\]/,/)\.collect/{/)\.collect/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimGpuOverrides.scala > $tmp_dir/AggregateInPandasExec_new.out
sed -n  '/GpuOverrides.exec\[AggregateInPandasExec\]/,/)\.map/{/)\.map/!p}'  ../sql-plugin/src/main/311until320-nondb/scala/com/nvidia/spark/rapids/shims/Spark31XShims.scala > $tmp_dir/AggregateInPandasExec_old.out
diff -c -w $tmp_dir/AggregateInPandasExec_new.out $tmp_dir/AggregateInPandasExec_old.out

sed -n  '/object GpuOrcScanBase/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOrcScanBase.scala > $tmp_dir/GpuOrcScanBase_new.out
sed -n  '/object GpuOrcScanBase/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOrcScanBase.scala > $tmp_dir/GpuOrcScanBase_old.out
diff  $tmp_dir/GpuOrcScanBase_new.out $tmp_dir/GpuOrcScanBase_old.out > $tmp_dir/GpuOrcScanBase.newdiff || true
diff -c spark2diffs/GpuOrcScanBase.diff $tmp_dir/GpuOrcScanBase.newdiff

sed -n  '/class LiteralExprMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/literalsMeta.scala > $tmp_dir/LiteralExprMeta_new.out
sed -n  '/class LiteralExprMeta/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/literals.scala > $tmp_dir/LiteralExprMeta_old.out
diff  $tmp_dir/LiteralExprMeta_new.out $tmp_dir/LiteralExprMeta_old.out > $tmp_dir/LiteralExprMeta.newdiff || true
diff -c spark2diffs/LiteralExprMeta.diff $tmp_dir/LiteralExprMeta.newdiff

# 2.x doesn't have a base aggregate class so this is much different, check the revision for now
diff ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/aggregateMeta.scala ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/aggregate.scala > $tmp_dir/aggregate.newdiff || true
diff -c spark2diffs/aggregate.diff $tmp_dir/aggregate.newdiff

sed -n  '/class GpuGenerateExecSparkPlanMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGenerateExecMeta.scala > $tmp_dir/GpuGenerateExecSparkPlanMeta_new.out
sed -n  '/class GpuGenerateExecSparkPlanMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGenerateExec.scala > $tmp_dir/GpuGenerateExecSparkPlanMeta_old.out
diff $tmp_dir/GpuGenerateExecSparkPlanMeta_new.out $tmp_dir/GpuGenerateExecSparkPlanMeta_old.out > $tmp_dir/GpuGenerateExecSparkPlanMeta.newdiff || true
diff -c spark2diffs/GpuGenerateExecSparkPlanMeta.diff  $tmp_dir/GpuGenerateExecSparkPlanMeta.newdiff

sed -n  '/abstract class GeneratorExprMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGenerateExecMeta.scala > $tmp_dir/GeneratorExprMeta_new.out
sed -n  '/abstract class GeneratorExprMeta/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGenerateExec.scala > $tmp_dir/GeneratorExprMeta_old.out
diff $tmp_dir/GeneratorExprMeta_new.out $tmp_dir/GeneratorExprMeta_old.out > $tmp_dir/GeneratorExprMeta.newdiff || true
diff -c spark2diffs/GeneratorExprMeta.diff  $tmp_dir/GeneratorExprMeta.newdiff

diff ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/ExplainPlan.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/ExplainPlan.scala > $tmp_dir/ExplainPlan.newdiff || true
diff spark2diffs/ExplainPlan.diff $tmp_dir/ExplainPlan.newdiff

sed -n  '/class GpuProjectExecMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperatorsMeta.scala > $tmp_dir/GpuProjectExecMeta_new.out
sed -n  '/class GpuProjectExecMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperators.scala > $tmp_dir/GpuProjectExecMeta_old.out
diff $tmp_dir/GpuProjectExecMeta_new.out $tmp_dir/GpuProjectExecMeta_old.out > $tmp_dir/GpuProjectExecMeta.newdiff || true
diff -c spark2diffs/GpuProjectExecMeta.diff  $tmp_dir/GpuProjectExecMeta.newdiff

sed -n  '/class GpuSampleExecMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperatorsMeta.scala > $tmp_dir/GpuSampleExecMeta_new.out
sed -n  '/class GpuSampleExecMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperators.scala > $tmp_dir/GpuSampleExecMeta_old.out
diff $tmp_dir/GpuSampleExecMeta_new.out $tmp_dir/GpuSampleExecMeta_old.out > $tmp_dir/GpuSampleExecMeta.newdiff || true
diff -c spark2diffs/GpuSampleExecMeta.diff $tmp_dir/GpuSampleExecMeta.newdiff 

sed -n  '/class GpuSortMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSortExecMeta.scala > $tmp_dir/GpuSortMeta_new.out
sed -n  '/class GpuSortMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSortExec.scala > $tmp_dir/GpuSortMeta_old.out
diff $tmp_dir/GpuSortMeta_new.out $tmp_dir/GpuSortMeta_old.out > $tmp_dir/GpuSortMeta.newdiff || true
diff -c spark2diffs/GpuSortMeta.diff  $tmp_dir/GpuSortMeta.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/InputFileBlockRule.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/InputFileBlockRule.scala > $tmp_dir/InputFileBlockRule.newdiff || true
diff -c spark2diffs/InputFileBlockRule.diff $tmp_dir/InputFileBlockRule.newdiff

sed -n  '/abstract class GpuWindowInPandasExecMetaBase/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuPandasMeta.scala > $tmp_dir/GpuWindowInPandasExecMetaBase_new.out
sed -n  '/abstract class GpuWindowInPandasExecMetaBase/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/python/GpuWindowInPandasExecBase.scala > $tmp_dir/GpuWindowInPandasExecMetaBase_old.out
diff $tmp_dir/GpuWindowInPandasExecMetaBase_new.out $tmp_dir/GpuWindowInPandasExecMetaBase_old.out > $tmp_dir/GpuWindowInPandasExecMetaBase.newdiff || true
diff -c spark2diffs/GpuWindowInPandasExecMetaBase.diff  $tmp_dir/GpuWindowInPandasExecMetaBase.newdiff

sed -n  '/class GpuAggregateInPandasExecMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuPandasMeta.scala > $tmp_dir/GpuAggregateInPandasExecMeta_new.out
sed -n  '/class GpuAggregateInPandasExecMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/python/GpuAggregateInPandasExec.scala > $tmp_dir/GpuAggregateInPandasExecMeta_old.out
diff $tmp_dir/GpuAggregateInPandasExecMeta_new.out $tmp_dir/GpuAggregateInPandasExecMeta_old.out > $tmp_dir/GpuAggregateInPandasExecMeta.newdiff || true
diff -c spark2diffs/GpuAggregateInPandasExecMeta.diff  $tmp_dir/GpuAggregateInPandasExecMeta.newdiff

sed -n  '/class GpuFlatMapGroupsInPandasExecMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuPandasMeta.scala > $tmp_dir/GpuFlatMapGroupsInPandasExecMeta_new.out
sed -n  '/class GpuFlatMapGroupsInPandasExecMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/301+-nondb/scala/org/apache/spark/sql/rapids/execution/python/shims/v2/GpuFlatMapGroupsInPandasExec.scala > $tmp_dir/GpuFlatMapGroupsInPandasExecMeta_old.out
diff $tmp_dir/GpuFlatMapGroupsInPandasExecMeta_new.out $tmp_dir/GpuFlatMapGroupsInPandasExecMeta_old.out > $tmp_dir/GpuFlatMapGroupsInPandasExecMeta.newdiff || true
diff -c spark2diffs/GpuFlatMapGroupsInPandasExecMeta.diff  $tmp_dir/GpuFlatMapGroupsInPandasExecMeta.newdiff

sed -n  '/class GpuShuffledHashJoinMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/GpuShuffledHashJoinExecMeta.scala > $tmp_dir/GpuShuffledHashJoinMeta_new.out
sed -n  '/class GpuShuffledHashJoinMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuShuffledHashJoinExec.scala > $tmp_dir/GpuShuffledHashJoinMeta_old.out
diff $tmp_dir/GpuShuffledHashJoinMeta_new.out $tmp_dir/GpuShuffledHashJoinMeta_old.out > $tmp_dir/GpuShuffledHashJoinMeta.newdiff || true
diff -c spark2diffs/GpuShuffledHashJoinMeta.diff  $tmp_dir/GpuShuffledHashJoinMeta.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/TreeNode.scala  ../sql-plugin/src/main/pre320-treenode/scala/com/nvidia/spark/rapids/shims/v2/TreeNode.scala > $tmp_dir/TreeNode.newdiff || true
diff -c spark2diffs/TreeNode.diff $tmp_dir/TreeNode.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/GpuSortMergeJoinMeta.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSortMergeJoinMeta.scala > $tmp_dir/GpuSortMergeJoinMeta.newdiff || true
diff -c spark2diffs/GpuSortMergeJoinMeta.diff $tmp_dir/GpuSortMergeJoinMeta.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/GpuJoinUtils.scala  ../sql-plugin/src/main/301db/scala/com/nvidia/spark/rapids/shims/v2/GpuJoinUtils.scala > $tmp_dir/GpuJoinUtils.newdiff || true
diff -c spark2diffs/GpuJoinUtils.diff $tmp_dir/GpuJoinUtils.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/TypeSigUtil.scala  ../sql-plugin/src/main/301until320-all/scala/com/nvidia/spark/rapids/shims/v2/TypeSigUtil.scala > $tmp_dir/TypeSigUtil.newdiff || true
diff -c spark2diffs/TypeSigUtil.diff $tmp_dir/TypeSigUtil.newdiff

sed -n  '/class GpuBroadcastHashJoinMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/GpuBroadcastHashJoinExecMeta.scala > $tmp_dir/GpuBroadcastHashJoinMeta_new.out
sed -n  '/class GpuBroadcastHashJoinMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuBroadcastHashJoinExec.scala > $tmp_dir/GpuBroadcastHashJoinMeta_old.out
diff $tmp_dir/GpuBroadcastHashJoinMeta_new.out $tmp_dir/GpuBroadcastHashJoinMeta_old.out > $tmp_dir/GpuBroadcastHashJoinMeta.newdiff || true
diff -c spark2diffs/GpuBroadcastHashJoinMeta.diff  $tmp_dir/GpuBroadcastHashJoinMeta.newdiff

sed -n  '/object GpuCSVScan/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/GpuCSVScan.scala > $tmp_dir/GpuCSVScan_new.out
sed -n  '/object GpuCSVScan/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuBatchScanExec.scala > $tmp_dir/GpuCSVScan_old.out
diff  $tmp_dir/GpuCSVScan_new.out $tmp_dir/GpuCSVScan_old.out  > $tmp_dir/GpuCSVScan.newdiff || true
diff -c spark2diffs/GpuCSVScan.diff $tmp_dir/GpuCSVScan.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/OffsetWindowFunctionMeta.scala  ../sql-plugin/src/main/301until310-all/scala/com/nvidia/spark/rapids/shims/v2/OffsetWindowFunctionMeta.scala > $tmp_dir/OffsetWindowFunctionMeta.newdiff || true
diff -c spark2diffs/OffsetWindowFunctionMeta.diff $tmp_dir/OffsetWindowFunctionMeta.newdiff

sed -n  '/class GpuRegExpReplaceMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/GpuRegExpReplaceMeta.scala > $tmp_dir/GpuRegExpReplaceMeta_new.out
sed -n  '/class GpuRegExpReplaceMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/301until310-nondb/scala/com/nvidia/spark/rapids/shims/v2/GpuRegExpReplaceMeta.scala > $tmp_dir/GpuRegExpReplaceMeta_old.out
diff $tmp_dir/GpuRegExpReplaceMeta_new.out $tmp_dir/GpuRegExpReplaceMeta_old.out > $tmp_dir/GpuRegExpReplaceMeta.newdiff || true
diff -c spark2diffs/GpuRegExpReplaceMeta.diff  $tmp_dir/GpuRegExpReplaceMeta.newdiff

sed -n  '/class GpuWindowExpressionMetaBase/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuWindowExpressionMetaBase_new.out
sed -n  '/class GpuWindowExpressionMetaBase/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowExpression.scala > $tmp_dir/GpuWindowExpressionMetaBase_old.out
diff $tmp_dir/GpuWindowExpressionMetaBase_new.out $tmp_dir/GpuWindowExpressionMetaBase_old.out > $tmp_dir/GpuWindowExpressionMetaBase.newdiff || true
diff -c spark2diffs/GpuWindowExpressionMetaBase.diff  $tmp_dir/GpuWindowExpressionMetaBase.newdiff

sed -n  '/abstract class GpuSpecifiedWindowFrameMetaBase/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuSpecifiedWindowFrameMetaBase_new.out
sed -n  '/abstract class GpuSpecifiedWindowFrameMetaBase/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowExpression.scala > $tmp_dir/GpuSpecifiedWindowFrameMetaBase_old.out
diff $tmp_dir/GpuSpecifiedWindowFrameMetaBase_new.out $tmp_dir/GpuSpecifiedWindowFrameMetaBase_old.out > $tmp_dir/GpuSpecifiedWindowFrameMetaBase.newdiff || true
diff -c spark2diffs/GpuSpecifiedWindowFrameMetaBase.diff  $tmp_dir/GpuSpecifiedWindowFrameMetaBase.newdiff

sed -n  '/class GpuSpecifiedWindowFrameMeta(/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuSpecifiedWindowFrameMeta_new.out
sed -n  '/class GpuSpecifiedWindowFrameMeta(/,/^}/{/^}/!p}'  ../sql-plugin/src/main/301until320-all/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuSpecifiedWindowFrameMeta_old.out
diff $tmp_dir/GpuSpecifiedWindowFrameMeta_new.out $tmp_dir/GpuSpecifiedWindowFrameMeta_old.out > $tmp_dir/GpuSpecifiedWindowFrameMeta.newdiff || true
diff -c spark2diffs/GpuSpecifiedWindowFrameMeta.diff  $tmp_dir/GpuSpecifiedWindowFrameMeta.newdiff

sed -n  '/class GpuWindowExpressionMeta(/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuWindowExpressionMeta_new.out
sed -n  '/class GpuWindowExpressionMeta(/,/^}/{/^}/!p}'  ../sql-plugin/src/main/301until320-all/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuWindowExpressionMeta_old.out
diff $tmp_dir/GpuWindowExpressionMeta_new.out $tmp_dir/GpuWindowExpressionMeta_old.out > $tmp_dir/GpuWindowExpressionMeta.newdiff || true
diff -c spark2diffs/GpuWindowExpressionMeta.diff  $tmp_dir/GpuWindowExpressionMeta.newdiff

sed -n  '/object GpuWindowUtil/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuWindowUtil_new.out
sed -n  '/object GpuWindowUtil/,/^}/{/^}/!p}'  ../sql-plugin/src/main/301until320-all/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuWindowUtil_old.out
diff  -c $tmp_dir/GpuWindowUtil_new.out $tmp_dir/GpuWindowUtil_old.out

sed -n  '/case class ParsedBoundary/p'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/ParsedBoundary_new.out
sed -n  '/case class ParsedBoundary/p'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowExec.scala > $tmp_dir/ParsedBoundary_old.out
diff -c $tmp_dir/ParsedBoundary_new.out $tmp_dir/ParsedBoundary_old.out

sed -n  '/class GpuWindowSpecDefinitionMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/v2/gpuWindows.scala > $tmp_dir/GpuWindowSpecDefinitionMeta_new.out
sed -n  '/class GpuWindowSpecDefinitionMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowExpression.scala > $tmp_dir/GpuWindowSpecDefinitionMeta_old.out
diff $tmp_dir/GpuWindowSpecDefinitionMeta_new.out $tmp_dir/GpuWindowSpecDefinitionMeta_old.out > $tmp_dir/GpuWindowSpecDefinitionMeta.newdiff || true
diff -c spark2diffs/GpuWindowSpecDefinitionMeta.diff  $tmp_dir/GpuWindowSpecDefinitionMeta.newdiff

sed -n  '/abstract class GpuBaseWindowExecMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowMeta.scala > $tmp_dir/GpuBaseWindowExecMeta_new.out
sed -n  '/abstract class GpuBaseWindowExecMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowExec.scala > $tmp_dir/GpuBaseWindowExecMeta_old.out
diff $tmp_dir/GpuBaseWindowExecMeta_new.out $tmp_dir/GpuBaseWindowExecMeta_old.out > $tmp_dir/GpuBaseWindowExecMeta.newdiff || true
diff -c spark2diffs/GpuBaseWindowExecMeta.diff $tmp_dir/GpuBaseWindowExecMeta.newdiff

sed -n  '/class GpuWindowExecMeta(/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowMeta.scala > $tmp_dir/GpuWindowExecMeta_new.out
sed -n  '/class GpuWindowExecMeta(/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuWindowExec.scala  > $tmp_dir/GpuWindowExecMeta_old.out
diff $tmp_dir/GpuWindowExecMeta_new.out $tmp_dir/GpuWindowExecMeta_old.out > $tmp_dir/GpuWindowExecMeta.newdiff || true
diff -c spark2diffs/GpuWindowExecMeta.diff  $tmp_dir/GpuWindowExecMeta.newdiff

sed -n  '/class GpuExpandExecMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuExpandMeta.scala > $tmp_dir/GpuExpandExecMeta_new.out
sed -n  '/class GpuExpandExecMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuExpandExec.scala > $tmp_dir/GpuExpandExecMeta_old.out
diff $tmp_dir/GpuExpandExecMeta_new.out $tmp_dir/GpuExpandExecMeta_old.out > $tmp_dir/GpuExpandExecMeta.newdiff || true
diff -c spark2diffs/GpuExpandExecMeta.diff $tmp_dir/GpuExpandExecMeta.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/RapidsMeta.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/RapidsMeta.scala > $tmp_dir/RapidsMeta.newdiff || true
diff -c spark2diffs/RapidsMeta.diff $tmp_dir/RapidsMeta.newdiff

sed -n  '/object GpuReadCSVFileFormat/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuReadCSVFileFormat.scala > $tmp_dir/GpuReadCSVFileFormat_new.out
sed -n  '/object GpuReadCSVFileFormat/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuReadCSVFileFormat.scala > $tmp_dir/GpuReadCSVFileFormat_old.out
diff  $tmp_dir/GpuReadCSVFileFormat_new.out $tmp_dir/GpuReadCSVFileFormat_old.out > $tmp_dir/GpuReadCSVFileFormat.newdiff || true
diff -c spark2diffs/GpuReadCSVFileFormat.diff $tmp_dir/GpuReadCSVFileFormat.newdiff 

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/RapidsConf.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/RapidsConf.scala > $tmp_dir/RapidsConf.newdiff || true
diff -c spark2diffs/RapidsConf.diff $tmp_dir/RapidsConf.newdiff

sed -n '/object GpuReadParquetFileFormat/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuParquetScanBase.scala > $tmp_dir/GpuReadParquetFileFormat_new.out
sed -n '/object GpuReadParquetFileFormat/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuReadParquetFileFormat.scala > $tmp_dir/GpuReadParquetFileFormat_old.out
diff $tmp_dir/GpuReadParquetFileFormat_new.out $tmp_dir/GpuReadParquetFileFormat_old.out > $tmp_dir/GpuReadParquetFileFormat.newdiff || true
diff -c spark2diffs/GpuReadParquetFileFormat.diff $tmp_dir/GpuReadParquetFileFormat.newdiff

sed -n '/object GpuParquetScanBase/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuParquetScanBase.scala > $tmp_dir/GpuParquetScanBase_new.out
sed -n '/object GpuParquetScanBase/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuParquetScanBase.scala > $tmp_dir/GpuParquetScanBase_old.out
diff $tmp_dir/GpuParquetScanBase_new.out $tmp_dir/GpuParquetScanBase_old.out > $tmp_dir/GpuParquetScanBase.newdiff || true
diff -c spark2diffs/GpuParquetScanBase.diff $tmp_dir/GpuParquetScanBase.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuBroadcastJoinMeta.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuBroadcastJoinMeta.scala > $tmp_dir/GpuBroadcastJoinMeta.newdiff || true
diff -c spark2diffs/GpuBroadcastJoinMeta.diff $tmp_dir/GpuBroadcastJoinMeta.newdiff

sed -n '/object AggregateUtils/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/AggregateUtils.scala > $tmp_dir/AggregateUtils_new.out
sed -n '/object AggregateUtils/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/aggregate.scala > $tmp_dir/AggregateUtils_old.out
diff $tmp_dir/AggregateUtils_new.out $tmp_dir/AggregateUtils_old.out > $tmp_dir/AggregateUtils.newdiff || true
diff -c spark2diffs/AggregateUtils.diff $tmp_dir/AggregateUtils.newdiff

sed -n '/final class CastExprMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuCastMeta.scala > $tmp_dir/CastExprMeta_new.out
sed -n '/final class CastExprMeta/,/override def convertToGpu/{/override def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuCast.scala > $tmp_dir/CastExprMeta_old.out
diff $tmp_dir/CastExprMeta_new.out $tmp_dir/CastExprMeta_old.out > $tmp_dir/CastExprMeta.newdiff || true
diff -c spark2diffs/CastExprMeta.diff $tmp_dir/CastExprMeta.newdiff

sed -n '/object GpuReadOrcFileFormat/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuReadOrcFileFormat.scala > $tmp_dir/GpuReadOrcFileFormat_new.out
sed -n '/object GpuReadOrcFileFormat/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuReadOrcFileFormat.scala > $tmp_dir/GpuReadOrcFileFormat_old.out
diff $tmp_dir/GpuReadOrcFileFormat_new.out $tmp_dir/GpuReadOrcFileFormat_old.out > $tmp_dir/GpuReadOrcFileFormat.newdiff || true
diff -c spark2diffs/GpuReadOrcFileFormat.diff $tmp_dir/GpuReadOrcFileFormat.newdiff

sed -n '/object GpuParquetFileFormat/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuParquetFileFormat.scala > $tmp_dir/GpuParquetFileFormat_new.out
sed -n '/object GpuParquetFileFormat/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuParquetFileFormat.scala > $tmp_dir/GpuParquetFileFormat_old.out
diff $tmp_dir/GpuParquetFileFormat_new.out $tmp_dir/GpuParquetFileFormat_old.out > $tmp_dir/GpuParquetFileFormat.newdiff || true
diff -c spark2diffs/GpuParquetFileFormat.diff $tmp_dir/GpuParquetFileFormat.newdiff

sed -n '/def asDecimalType/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/asDecimalType_new.out
sed -n '/def asDecimalType/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/asDecimalType_old.out
diff -c $tmp_dir/asDecimalType_new.out $tmp_dir/asDecimalType_old.out

sed -n '/def optionallyAsDecimalType/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/optionallyAsDecimalType_new.out
sed -n '/def optionallyAsDecimalType/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/optionallyAsDecimalType_old.out
diff $tmp_dir/optionallyAsDecimalType_new.out $tmp_dir/optionallyAsDecimalType_old.out > $tmp_dir/optionallyAsDecimalType.newdiff || true
diff -c spark2diffs/optionallyAsDecimalType.diff $tmp_dir/optionallyAsDecimalType.newdiff

sed -n '/def getPrecisionForIntegralType/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/getPrecisionForIntegralType_new.out
sed -n '/def getPrecisionForIntegralType/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/getPrecisionForIntegralType_old.out
diff $tmp_dir/getPrecisionForIntegralType_new.out $tmp_dir/getPrecisionForIntegralType_old.out > $tmp_dir/getPrecisionForIntegralType.newdiff || true
diff -c spark2diffs/getPrecisionForIntegralType.diff $tmp_dir/getPrecisionForIntegralType.newdiff

# not sure this diff works very well due to java vs scala and quite a bit different but should find any changes in those functions
sed -n '/def toRapidsStringOrNull/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/toRapidsStringOrNull_new.out
sed -n '/private static DType/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/java/com/nvidia/spark/rapids/GpuColumnVector.java > $tmp_dir/toRapidsStringOrNull_old.out
diff $tmp_dir/toRapidsStringOrNull_new.out $tmp_dir/toRapidsStringOrNull_old.out > $tmp_dir/toRapidsStringOrNull.newdiff || true
diff -c spark2diffs/toRapidsStringOrNull.diff $tmp_dir/toRapidsStringOrNull.newdiff

sed -n '/def createCudfDecimal/,/^  }/{/^  }/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/createCudfDecimal_new.out
sed -n '/def createCudfDecimal/,/^  }/{/^  }/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/DecimalUtil.scala > $tmp_dir/createCudfDecimal_old.out
diff $tmp_dir/createCudfDecimal_new.out $tmp_dir/createCudfDecimal_old.out > $tmp_dir/createCudfDecimal.newdiff || true
diff -c spark2diffs/createCudfDecimal.diff $tmp_dir/createCudfDecimal.newdiff

sed -n  '/abstract class ReplicateRowsExprMeta/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuReplicateRowsMeta.scala > $tmp_dir/ReplicateRowsExprMeta_new.out
sed -n  '/abstract class ReplicateRowsExprMeta/,/override final def convertToGpu/{/override final def convertToGpu/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGenerateExec.scala > $tmp_dir/ReplicateRowsExprMeta_old.out
diff $tmp_dir/ReplicateRowsExprMeta_new.out $tmp_dir/ReplicateRowsExprMeta_old.out > $tmp_dir/ReplicateRowsExprMeta.newdiff || true
diff -c spark2diffs/ReplicateRowsExprMeta.diff $tmp_dir/ReplicateRowsExprMeta.newdiff

diff  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala > $tmp_dir/DateUtils.newdiff || true
diff -c spark2diffs/DateUtils.diff $tmp_dir/DateUtils.newdiff

sed -n '/object CudfTDigest/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuApproximatePercentile.scala > $tmp_dir/CudfTDigest_new.out
sed -n '/object CudfTDigest/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuApproximatePercentile.scala > $tmp_dir/CudfTDigest_old.out
diff $tmp_dir/CudfTDigest_new.out $tmp_dir/CudfTDigest_old.out > $tmp_dir/CudfTDigest.newdiff || true
diff -c spark2diffs/CudfTDigest.diff $tmp_dir/CudfTDigest.newdiff

sed -n  '/sealed trait TimeParserPolicy/p'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/TimeParserPolicy_new.out
sed -n  '/sealed trait TimeParserPolicy/p'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/TimeParserPolicy_old.out
diff -c $tmp_dir/TimeParserPolicy_new.out $tmp_dir/TimeParserPolicy_old.out

sed -n  '/object LegacyTimeParserPolicy/p'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/LegacyTimeParserPolicy_new.out
sed -n  '/object LegacyTimeParserPolicy/p'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/LegacyTimeParserPolicy_old.out
diff -c $tmp_dir/LegacyTimeParserPolicy_new.out $tmp_dir/LegacyTimeParserPolicy_old.out

sed -n  '/object ExceptionTimeParserPolicy/p'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/ExceptionTimeParserPolicy_new.out
sed -n  '/object ExceptionTimeParserPolicy/p'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/ExceptionTimeParserPolicy_old.out
diff -c $tmp_dir/ExceptionTimeParserPolicy_new.out $tmp_dir/ExceptionTimeParserPolicy_old.out

sed -n  '/object CorrectedTimeParserPolicy/p'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/CorrectedTimeParserPolicy_new.out
sed -n  '/object CorrectedTimeParserPolicy/p'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/datetimeExpressions.scala > $tmp_dir/CorrectedTimeParserPolicy_old.out
diff -c $tmp_dir/CorrectedTimeParserPolicy_new.out $tmp_dir/CorrectedTimeParserPolicy_old.out

sed -n '/object GpuFloorCeil/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/mathExpressions.scala > $tmp_dir/GpuFloorCeil_new.out
sed -n '/object GpuFloorCeil/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/mathExpressions.scala > $tmp_dir/GpuFloorCeil_old.out
diff -c $tmp_dir/GpuFloorCeil_new.out $tmp_dir/GpuFloorCeil_old.out

sed -n '/object GpuFileSourceScanExec/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuFileSourceScanExec.scala > $tmp_dir/GpuFileSourceScanExec_new.out
sed -n '/object GpuFileSourceScanExec/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuFileSourceScanExec.scala > $tmp_dir/GpuFileSourceScanExec_old.out
diff $tmp_dir/GpuFileSourceScanExec_new.out $tmp_dir/GpuFileSourceScanExec_old.out > $tmp_dir/GpuFileSourceScanExec.newdiff || true
diff -c spark2diffs/GpuFileSourceScanExec.diff $tmp_dir/GpuFileSourceScanExec.newdiff

sed -n '/object GpuReadJsonFileFormat/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/catalyst/json/rapids/GpuReadJsonFileFormat.scala > $tmp_dir/GpuReadJsonFileFormat_new.out
sed -n '/object GpuReadJsonFileFormat/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/catalyst/json/rapids/GpuReadJsonFileFormat.scala > $tmp_dir/GpuReadJsonFileFormat_old.out
diff $tmp_dir/GpuReadJsonFileFormat_new.out $tmp_dir/GpuReadJsonFileFormat_old.out > $tmp_dir/GpuReadJsonFileFormat.newdiff || true
diff -c spark2diffs/GpuReadJsonFileFormat.diff $tmp_dir/GpuReadJsonFileFormat.newdiff

sed -n '/object GpuJsonScan/,/^}/{/^}/!p}'  ../spark2-sql-plugin/src/main/scala/org/apache/spark/sql/catalyst/json/rapids/GpuJsonScan.scala > $tmp_dir/GpuJsonScan_new.out
sed -n '/object GpuJsonScan/,/^}/{/^}/!p}'  ../sql-plugin/src/main/scala/org/apache/spark/sql/catalyst/json/rapids/GpuJsonScan.scala > $tmp_dir/GpuJsonScan_old.out
diff $tmp_dir/GpuJsonScan_new.out $tmp_dir/GpuJsonScan_old.out > $tmp_dir/GpuJsonScan.newdiff || true
diff -c spark2diffs/GpuJsonScan.diff $tmp_dir/GpuJsonScan.newdiff

echo "Done running Diffs of spark2.x files"
rm -r $tmp_dir
