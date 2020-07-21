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

package com.nvidia.spark.rapids

import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager

class ShuffleBufferCatalogSuite extends FunSuite with MockitoSugar {
  test("registered shuffles should be active") {
    val catalog = mock[RapidsBufferCatalog]
    val rapidsDiskBlockManager = mock[RapidsDiskBlockManager]
    val shuffleCatalog = new ShuffleBufferCatalog(catalog, rapidsDiskBlockManager)

    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.registerShuffle(123)
    assertResult(true)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.unregisterShuffle(123)
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
  }
}
