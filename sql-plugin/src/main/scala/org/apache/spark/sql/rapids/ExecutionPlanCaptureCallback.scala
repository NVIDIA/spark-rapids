/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.DataFrame

trait ExecutionPlanCaptureCallbackBase {
  def captureIfNeeded(qe: QueryExecution): Unit
  def startCapture(): Unit
  def startCapture(timeoutMillis: Long): Unit
  def getResultsWithTimeout(timeoutMs: Long = 10000): Array[SparkPlan]
  def extractExecutedPlan(plan: SparkPlan): SparkPlan
  def assertContains(gpuPlan: SparkPlan, className: String): Unit
  def assertContains(df: DataFrame, gpuClass: String): Unit
  def assertContainsAnsiCast(df: DataFrame): Unit
  def assertNotContain(gpuPlan: SparkPlan, className: String): Unit
  def assertNotContain(df: DataFrame, gpuClass: String): Unit
  def assertDidFallBack(gpuPlan: SparkPlan, fallbackCpuClass: String): Unit
  def assertDidFallBack(df: DataFrame, fallbackCpuClass: String): Unit
  def didFallBack(plan: SparkPlan, fallbackCpuClass: String): Boolean
  def assertSchemataMatch(cpuDf: DataFrame, gpuDf: DataFrame, expectedSchema: String): Unit
}

object ExecutionPlanCaptureCallback extends ExecutionPlanCaptureCallbackBase {
  lazy val impl = ShimLoader.newExecutionPlanCaptureCallbackBase()

  override def captureIfNeeded(qe: QueryExecution): Unit =
    impl.captureIfNeeded(qe)

  override def startCapture(): Unit =
    impl.startCapture()

  override def startCapture(timeoutMillis: Long): Unit =
    impl.startCapture(timeoutMillis)

  override def getResultsWithTimeout(timeoutMs: Long = 10000): Array[SparkPlan] =
    impl.getResultsWithTimeout(timeoutMs)

  override def extractExecutedPlan(plan: SparkPlan): SparkPlan =
    impl.extractExecutedPlan(plan)

  override def assertContains(gpuPlan: SparkPlan, className: String): Unit =
    impl.assertContains(gpuPlan, className)

  override def assertContains(df: DataFrame, gpuClass: String): Unit =
    impl.assertContains(df, gpuClass)

  override def assertContainsAnsiCast(df: DataFrame): Unit =
    impl.assertContainsAnsiCast(df)

  override def assertNotContain(gpuPlan: SparkPlan, className: String): Unit =
    impl.assertNotContain(gpuPlan, className)

  override def assertNotContain(df: DataFrame, gpuClass: String): Unit =
    impl.assertNotContain(df, gpuClass)

  override def assertDidFallBack(gpuPlan: SparkPlan, fallbackCpuClass: String): Unit =
    impl.assertDidFallBack(gpuPlan, fallbackCpuClass)

  override def assertDidFallBack(df: DataFrame, fallbackCpuClass: String): Unit =
    impl.assertDidFallBack(df, fallbackCpuClass)

  override def didFallBack(plan: SparkPlan, fallbackCpuClass: String): Boolean =
    impl.didFallBack(plan, fallbackCpuClass)

  def assertSchemataMatch(cpuDf: DataFrame, gpuDf: DataFrame, expectedSchema: String): Unit =
    impl.assertSchemataMatch(cpuDf, gpuDf, expectedSchema)
}

/**
 * Used as a part of testing to capture the executed query plan.
 */
class ExecutionPlanCaptureCallback extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    ExecutionPlanCaptureCallback.captureIfNeeded(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    ExecutionPlanCaptureCallback.captureIfNeeded(qe)
}

trait AdaptiveSparkPlanHelperShim {
  def collect[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Seq[B]
}