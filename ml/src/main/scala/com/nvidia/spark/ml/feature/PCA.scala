/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.ml.feature

import org.apache.spark.ml.feature.RapidsPCA
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}

/**
 * PCA trains a model to project vectors to a lower dimensional space of the top `PCA!.k`
 * principal components.
 */
class PCA(override val uid: String) extends RapidsPCA {

  def this() = this(Identifiable.randomUID("pca"))

  override def copy(extra: ParamMap): PCA = defaultCopy(extra)
}

object PCA extends DefaultParamsReadable[PCA] {

  override def load(path: String): PCA = super.load(path)
}
