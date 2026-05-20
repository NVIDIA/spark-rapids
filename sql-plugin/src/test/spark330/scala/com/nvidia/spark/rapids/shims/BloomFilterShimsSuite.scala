/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/

// Two top-level packages declared with the explicit-block syntax.
// The first contributes a test-only stub class under the FQCN that
// `BloomFilterShims` looks up reflectively at runtime. The production
// `com.nvidia.spark.rapids.optimizer.cubloomfilter.TryReadBFRegistryExec`
// is unavailable on the test classpath, so this file owns that FQCN
// for the unit-test scope only. At runtime in a packaged dist jar,
// the test class never loads.
package com.nvidia.spark.rapids.optimizer.cubloomfilter {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.Attribute
  import org.apache.spark.sql.execution.LeafExecNode

  /**
   * Test-only stub whose FQCN matche `BloomFilterShims.TryReadBFRegistryExecClassName`.
   * Exposes a no-arg `bfId` method (Scala case-class field accessor) that the production
   * reflection path invokes.
   * LIFECYCLE NOTE: This stub exists because the planner module
   * (optimizer.cubloomfilter.TryReadBFRegistryExec) ships in a separate JAR that is not on
   * the test classpath today. When the planner module lands as a dependency, this stub must
   * be deleted — the real class will satisfy the reflection path directly. Until then,
   * this stub is the only way to exercise the production FQCN lookup in unit tests.
   */
  case class TryReadBFRegistryExec(bfId: String) extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("test stub")
  }
}

package com.nvidia.spark.rapids.shims {

  import org.scalatest.funsuite.AnyFunSuite

  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.Attribute
  import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
  // Same-file synthetic class brings the FQCN into the test classpath.
  import com.nvidia.spark.rapids.optimizer.cubloomfilter.TryReadBFRegistryExec

  /**
   * Regression coverage for `BloomFilterShims`'s AQE-aware
   * `findBfIdInPlan`. AQE can wrap subquery plans in nodes whose
   * `children` returns `Seq.empty`, so the helper probes common AQE
   * accessors reflectively before falling back to normal child
   * traversal.
   */
  class BloomFilterShimsSuite extends AnyFunSuite {

    // --- Synthetic plan stubs (no SparkSession required) ---

    /**
     * Stub whose class name contains `AdaptiveSparkPlanExec` and
     * exposes `executedPlan` returning the wrapped inner plan.
     * `children` is `Seq.empty` per the AQE quirk that the helper
     * defends against. Triggers the substring match in
     * `tryAqePlanFields`.
     */
    private case class FakeAdaptiveSparkPlanExec(inner: SparkPlan)
        extends LeafExecNode {
      override def output: Seq[Attribute] = Seq.empty
      def executedPlan: SparkPlan = inner
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
    }

    /**
     * Stub whose class name contains `AdaptiveSparkPlanExec` but
     * exposes only `currentPhysicalPlan` (a different AQE accessor
     * than `executedPlan`). Verifies the helper probes all 4
     * accessor names in priority order.
     */
    private case class FakeAdaptiveSparkPlanExecAlt(inner: SparkPlan)
        extends LeafExecNode {
      override def output: Seq[Attribute] = Seq.empty
      def currentPhysicalPlan: SparkPlan = inner
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
    }

    /** A non-AQE plain leaf for negative-test coverage. */
    private case class PlainLeafExec() extends LeafExecNode {
      override def output: Seq[Attribute] = Seq.empty
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
    }

    /** A simple UnaryExecNode wrapping a child, exercising standard
     *  `children` traversal (the path NOT going through
     *  `tryAqePlanFields`). */
    private case class WrapExec(child: SparkPlan) extends UnaryExecNode {
      override def output: Seq[Attribute] = Seq.empty
      override protected def doExecute(): RDD[InternalRow] =
        throw new UnsupportedOperationException("test stub")
      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)
    }

    // --- tryAqePlanFields helper coverage ---

    test("tryAqePlanFields: returns Seq.empty for non-AQE plans") {
      val plain = PlainLeafExec()
      assert(BloomFilterShims.tryAqePlanFields(plain).isEmpty,
        "non-AQE plan must not trigger reflective field probing")
    }

    test("tryAqePlanFields: returns inner plan for AQE-named class with executedPlan") {
      val inner = PlainLeafExec()
      val aqe = FakeAdaptiveSparkPlanExec(inner)
      val extracted = BloomFilterShims.tryAqePlanFields(aqe)
      assert(extracted.nonEmpty,
        "AQE-named plan with executedPlan must surface its inner plan")
      assert(extracted.head eq inner,
        "extracted reference must be the same instance the stub returned")
    }

    test("tryAqePlanFields: probes all 4 accessor names in priority order") {
      // Stub exposes only `currentPhysicalPlan` (priority #2), not
      // `executedPlan` (#1). Helper should still surface the inner
      // plan via the second-priority accessor.
      val inner = PlainLeafExec()
      val aqe = FakeAdaptiveSparkPlanExecAlt(inner)
      val extracted = BloomFilterShims.tryAqePlanFields(aqe)
      assert(extracted.nonEmpty,
        "fallback to currentPhysicalPlan must work when executedPlan is absent")
      assert(extracted.contains(inner),
        "currentPhysicalPlan's return value must be in the extracted list")
    }

    // --- findBfIdInPlan integration coverage ---

    test("findBfIdInPlan: AQE-wrapped subquery regression case") {
      // Synthetic plan: AdaptiveSparkPlanExec wraps an inner plan
      // whose only leaf is a TryReadBFRegistryExec. The standard
      // tree walk would miss this (AQE.children = Seq.empty); the
      // AQE traversal helper must surface the leaf reflectively.
      val tryRead = TryReadBFRegistryExec(bfId = "cubf-aqe-regression-test")
      val aqeWrapped = FakeAdaptiveSparkPlanExec(tryRead)
      val result = BloomFilterShims.findBfIdInPlan(aqeWrapped)
      assert(result === Some("cubf-aqe-regression-test"),
        "bfId must be discoverable through AQE wrapping")
    }

    test("findBfIdInPlan: non-AQE direct child path still works") {
      // Standard `children` traversal: no AQE wrapping.
      // findBfIdInPlan should walk directly to the
      // TryReadBFRegistryExec via children.
      val tryRead = TryReadBFRegistryExec(bfId = "cubf-non-aqe-path")
      val wrapped = WrapExec(tryRead)
      val result = BloomFilterShims.findBfIdInPlan(wrapped)
      assert(result === Some("cubf-non-aqe-path"),
        "non-AQE path must remain working; the AQE helper must not " +
          "break the standard children traversal")
    }

    test("findBfIdInPlan: leaf TryReadBFRegistryExec at root") {
      // Trivial case: plan is exactly TryReadBFRegistryExec.
      val tryRead = TryReadBFRegistryExec(bfId = "cubf-root")
      assert(BloomFilterShims.findBfIdInPlan(tryRead) === Some("cubf-root"))
    }

    test("findBfIdInPlan: returns None when no TryReadBFRegistryExec is reachable") {
      // Plan tree without the magic-named leaf. findBfIdInPlan must
      // return None cleanly so resolveProbeWiring degrades to
      // (None, None) and the GPU operator runs without instrumentation.
      val plain = PlainLeafExec()
      val aqeWrapped = FakeAdaptiveSparkPlanExec(plain)
      assert(BloomFilterShims.findBfIdInPlan(aqeWrapped).isEmpty,
        "absence of TryReadBFRegistryExec must yield None, not throw")
    }
  }
}
