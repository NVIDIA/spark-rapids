/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import scala.util.control.NonFatal

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.jni.BloomFilter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.aggregate.{CpuToGpuAggregateBufferConverter,
  CpuToGpuBloomFilterBufferConverter, GpuBloomFilterAggregate,
  GpuToCpuAggregateBufferConverter, GpuToCpuBloomFilterBufferConverter}

object BloomFilterShims extends Logging {

  // Probe-side leaf node emitted by the optional planner module. The execution layer discovers it
  // by FQCN to extract the `bfId` for accumulator wiring. If the planner module is absent, the
  // lookup silently returns None and the probe path runs without instrumentation.
  private val TryReadBFRegistryExecClassName =
    "com.nvidia.spark.rapids.optimizer.cubloomfilter.TryReadBFRegistryExec"

  lazy val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[BloomFilterMightContain](
        "Bloom filter query",
        ExprChecks.binaryProject(
          TypeSig.BOOLEAN,
          TypeSig.BOOLEAN,
          ("lhs", TypeSig.BINARY + TypeSig.NULL, TypeSig.BINARY + TypeSig.NULL),
          ("rhs", TypeSig.LONG + TypeSig.NULL, TypeSig.LONG + TypeSig.NULL)),
        (a, conf, p, r) => new BinaryExprMeta[BloomFilterMightContain](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            val (bfId, probeUpdater) = resolveProbeWiring(a.bloomFilterExpression)
            GpuBloomFilterMightContain(lhs, rhs, bfId, probeUpdater)
          }
        }),
      GpuOverrides.expr[BloomFilterAggregate](
        "Bloom filter build",
        ExprChecksImpl(Map(
          (ReductionAggExprContext,
            ContextChecks(TypeSig.BINARY, TypeSig.BINARY,
              Seq(ParamCheck("child", TypeSig.LONG, TypeSig.LONG),
                ParamCheck("estimatedItems",
                  TypeSig.lit(TypeEnum.LONG), TypeSig.lit(TypeEnum.LONG)),
                ParamCheck("numBits",
                  TypeSig.lit(TypeEnum.LONG), TypeSig.lit(TypeEnum.LONG))))))),
        (a, conf, p, r) => new TypedImperativeAggExprMeta[BloomFilterAggregate](a, conf, p, r) {
          private lazy val estimatedNumItems =
            GpuBloomFilterAggregate.clampEstimatedNumItems(
              a.estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue)

          private lazy val numBits =
            GpuBloomFilterAggregate.clampNumBits(
              a.numBitsExpression.eval().asInstanceOf[Number].longValue)

          override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
            GpuBloomFilterAggregate(
              childExprs.head,
              a.estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue,
              a.numBitsExpression.eval().asInstanceOf[Number].longValue,
              BloomFilterConstantsShims.BLOOM_FILTER_FORMAT_VERSION,
              BloomFilter.DEFAULT_SEED)
          }

          override def aggBufferAttribute: AttributeReference = {
            val aggBuffer = a.aggBufferAttributes.head
            aggBuffer.copy(dataType = a.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
          }

          // This is a defensive correctness fix for the rare mixed CPU/GPU bridge path.
          // BloomFilterAggregate crosses the CPU/GPU boundary as BinaryType in both directions,
          // but empty GPU partial buffers can be null while Spark CPU final expects a serialized
          // empty bloom filter. We still need a converter even though the runtime type
          // is unchanged.
          override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
            CpuToGpuBloomFilterBufferConverter()

          override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
            GpuToCpuBloomFilterBufferConverter(estimatedNumItems, numBits)

          override val supportBufferConversion: Boolean = true
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }

  private def resolveProbeWiring(
      bloomFilterExpression: Expression
  ): (Option[String], Option[BloomFilterPredicateUpdater]) = {
    if (!CuBFFeedbackFlags.isEnabled(SQLConf.get)) {
      (None, None)
    } else {
      val bfIdOpt = extractBfId(bloomFilterExpression)
      val updaterOpt = for {
        bfId <- bfIdOpt
        spark <- SparkSession.getActiveSession
      } yield BloomFilterProbeAccumulator.driverGetOrCreate(spark.sparkContext, bfId)
      (bfIdOpt, updaterOpt)
    }
  }

  private def extractBfId(expr: Expression): Option[String] = {
    var found: Option[String] = None
    expr.foreach {
      case e: ExecSubqueryExpression if found.isEmpty =>
        found = findBfIdInPlan(e.plan)
      case _ =>
    }
    found
  }

  private[shims] def findBfIdInPlan(plan: SparkPlan): Option[String] = {
    var found: Option[String] = None
    def visit(p: SparkPlan): Unit = {
      if (found.isEmpty && p != null) {
        if (p.getClass.getName == TryReadBFRegistryExecClassName) {
          found = readBfId(p)
        }
        if (found.isEmpty) {
          p match {
            case s: BaseSubqueryExec => visit(s.child)
            case _ =>
          }
        }
        if (found.isEmpty) {
          tryAqePlanFields(p).foreach(visit)
        }
        if (found.isEmpty) {
          p.children.foreach(visit)
        }
      }
    }
    visit(plan)
    found
  }

  private def readBfId(plan: SparkPlan): Option[String] = {
    try {
      Option(plan.getClass.getMethod("bfId").invoke(plan).asInstanceOf[String])
        .filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }
  }

  private val AqePlanFields: Seq[String] =
    Seq("executedPlan", "currentPhysicalPlan", "initialPlan", "inputPlan")

  private[shims] def tryAqePlanFields(p: SparkPlan): Seq[SparkPlan] = {
    if (!p.getClass.getName.contains("AdaptiveSparkPlanExec")) {
      Seq.empty
    } else {
      AqePlanFields.flatMap { name =>
        try {
          val m = p.getClass.getMethod(name)
          Option(m.invoke(p).asInstanceOf[SparkPlan])
        } catch {
          case NonFatal(_) => None
        }
      }
    }
  }
}
