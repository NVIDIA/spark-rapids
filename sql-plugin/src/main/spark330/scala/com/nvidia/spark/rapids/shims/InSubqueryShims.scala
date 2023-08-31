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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.InSubqueryExec


object InSubqueryShims {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[InSubqueryExec](
        "Evaluates to true if values are in a subquery's result set",
        ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.comparable),
        (a, conf, p, r) => new InSubqueryExecMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
