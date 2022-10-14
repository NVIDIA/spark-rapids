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

package org.apache.spark.sql.rapids.zorder

import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuExpression, GpuRangePartitioner, GpuSorter, RepeatingParamCheck, ShimLoader, TypeSig, UnaryExprMeta}
import com.nvidia.spark.rapids.GpuOverrides.{expr, pluginSupportedOrderableSig}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, SortOrder, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering

/**
 * These are rules to be able to replace zorder operators. For now they all come from deltalake,
 * but not all versions of deltalake support zordering so the simplest solution is to pick them
 * apart with reflection.  That way if they exist on the classpath we can replace them.
 */
object ZOrderRules {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val interleaveClazz =
        ShimLoader.loadClass("org.apache.spark.sql.delta.expressions.InterleaveBits")
            .asInstanceOf[Class[Expression]]
      val interleaveRule = expr[Expression](
        "Interleave bit as a part of deltalake zorder",
        ExprChecks.projectOnly(
          TypeSig.BINARY,
          TypeSig.BINARY,
          repeatingParamCheck =
            Some(RepeatingParamCheck("input",
              TypeSig.INT,
              TypeSig.INT))),
        (a, conf, p, r) => new ExprMeta[Expression](a, conf, p, r) {
          override def convertToGpu(): GpuExpression =
            GpuInterleaveBits(childExprs.map(_.convertToGpu()))
        })

      val partExprClass =
        ShimLoader.loadClass("org.apache.spark.sql.delta.expressions.PartitionerExpr")
            .asInstanceOf[Class[Expression]]
      val partExprRule = expr[UnaryExpression](
        "Partitioning on a single column for deltalake zorder",
        ExprChecks.unaryProject(
          TypeSig.INT,
          TypeSig.INT,
          // Same as RangePartitioningExec
          (pluginSupportedOrderableSig + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested(),
          TypeSig.orderable),
        (a, conf, p, r) => new UnaryExprMeta[UnaryExpression](a, conf, p, r) {
          def getBoundsNOrder: (Array[InternalRow], SortOrder) = {
            // We have to pull pick apart the partitioner apart, but we have to use reflection
            // to do it, because what we want is private
            val parterField = partExprClass.getDeclaredField("partitioner")
            parterField.setAccessible(true)
            val parter = parterField.get(a)
            val partClass = parter.getClass

            val rangeBoundsField = partClass.getDeclaredField("rangeBounds")
            rangeBoundsField.setAccessible(true)
            val rangeBounds = rangeBoundsField.get(parter).asInstanceOf[Array[InternalRow]]

            val orderingField = partClass.getDeclaredField("ordering")
            orderingField.setAccessible(true)
            val ordering = orderingField.get(parter).asInstanceOf[LazilyGeneratedOrdering]
            val singleOrder = ordering.ordering.head
            (rangeBounds, singleOrder)
          }

          override def tagExprForGpu(): Unit = {
            try {
              // Run this first to be sure that we can get what we need to convert
              getBoundsNOrder
            } catch {
              case _: NoSuchFieldError | _: SecurityException | _: IllegalAccessException =>
                willNotWorkOnGpu("The version of the partitioner does not have " +
                    "the expected fields in it.")
            }
          }

          override def convertToGpu(child: Expression): GpuExpression = {
            val (rangeBounds, singleOrder) = getBoundsNOrder
            // We need to create a GpuSorter, but where we are at will not have a schema/etc
            // so we need to make one up. We know the type and that there will only ever be
            // one column, so
            val sortCol = AttributeReference("sort_col", child.dataType, child.nullable)()
            val ourSortOrder =
              SortOrder(sortCol, singleOrder.direction, singleOrder.nullOrdering, Seq.empty)
            val sorter = new GpuSorter(Seq(ourSortOrder), Array(sortCol))
            val gpuPart = new GpuRangePartitioner(rangeBounds, sorter)
            GpuPartitionerExpr(child, gpuPart)
          }
        })
      Map(interleaveClazz -> interleaveRule,
        partExprClass -> partExprRule)
    } catch {
      case _: ClassNotFoundException =>
        Map.empty
    }
  }
}
