/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

/**
 * This trait is used to implement a hash join that does not support codegen.
 * It is in the org.apache.spark.sql.execution.joins package to access the
 * `HashedRelationInfo` class which is private to that package but needed by
 * any class implementing `HashJoin`.
 */
trait HashJoinWithoutCodegen extends HashJoin {
  override def supportCodegen: Boolean = false

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException("inputRDDs is used by codegen which we don't support")
  }

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    throw new UnsupportedOperationException(
      "prepareRelation is used by codegen which is not supported for this join")
  }
}
