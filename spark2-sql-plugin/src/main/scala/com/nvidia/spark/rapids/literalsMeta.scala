/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.BigInteger
import java.util
import java.util.{List => JList, Objects}
import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Literal, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class LiteralExprMeta(
    lit: Literal,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _]],
    r: DataFromReplacementRule) extends ExprMeta[Literal](lit, conf, p, r) {

  def withNewLiteral(newLiteral: Literal): LiteralExprMeta =
    new LiteralExprMeta(newLiteral, conf, p, r)

  // There are so many of these that we don't need to print them out, unless it
  // will not work on the GPU
  override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
      super.print(append, depth, all)
    }
  }
}
