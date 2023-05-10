/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

// import org.apache.spark.sql.types.{DataType, DataTypes, Decimal, DecimalType, StructType}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.scalatest.FunSuite

class CanonicalizeSuite extends FunSuite {
  test("SPARK-40362: Commutative operator under BinaryComparison") {
    Seq(GpuEqualTo, GpuEqualNullSafe, GpuGreaterThan,
        GpuLessThan, GpuGreaterThanOrEqual, GpuLessThanOrEqual)
      .foreach( bc => {
        assert(bc(GpuAdd($"a", $"b", true), Literal(10))
            .semanticEquals(bc(GpuAdd($"b", $"a", true), Literal(10))))
      })
  }
}
