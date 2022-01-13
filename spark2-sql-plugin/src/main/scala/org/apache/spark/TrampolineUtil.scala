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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

object TrampolineUtil {

  // package has to be different to access 2.x HashedRelationBroadcastMode
  def isSupportedRelation(mode: BroadcastMode): Boolean = {
    mode match {
      case _ : HashedRelationBroadcastMode => true
      case IdentityBroadcastMode => true
      case _ => false
    }
  }

  /**
   * Return true if the provided predicate function returns true for any
   * type node within the datatype tree.
   */
  def dataTypeExistsRecursively(dt: DataType, f: DataType => Boolean): Boolean = {
    dt.existsRecursively(f)
  }

  /** Get the simple name of a class with fixup for any Scala internal errors */
  def getSimpleName(cls: Class[_]): String = {
    Utils.getSimpleName(cls)
  }
}
