/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayStack

class RapidsStack[T] extends Proxy {
  private val stack = new ArrayStack[T]()

  override def self = stack

  def push(elem1: T): Unit = {
    self.push(elem1)
  }

  def pop(): T = {
    self.pop()
  }

  def isEmpty: Boolean = {
    self.isEmpty
  }

  def nonEmpty: Boolean = {
    self.nonEmpty
  }

  def size: Int = {
    self.size
  }

  def toSeq: Seq[T] = {
    self.toSeq
  }

  def clear(): Unit = {
    self.clear()
  }
}
