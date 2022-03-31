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

package org.apache.spark.deploy.history.rapids.tool

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.ApplicationInfoWrapper
import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.util.kvstore.KVStore


object SHSUtils {
  def loadAppWrapper(storeListing: KVStore,
                     appId: String, conf: SparkConf): ApplicationInfoWrapper = {
    storeListing.read(classOf[ApplicationInfoWrapper], appId)
  }

  def getApplicationInfo(storeListing: KVStore,
                         appId: String, conf: SparkConf): ApplicationInfo = {
    loadAppWrapper(storeListing, appId, conf).toApplicationInfo()
  }

//  def getApplicationInfo(storeListing: KVStore,
//                         appId: String, conf: SparkConf): Option[ApplicationInfo] = {
//    try {
//      Some(loadAppWrapper(storeListing, appId, conf).toApplicationInfo())
//    } catch {
//      case _: NoSuchElementException =>
//        None
//    }
//  }
}
