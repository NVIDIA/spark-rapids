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

package com.nvidia.spark.rapids

import java.util.concurrent.{Callable, Executors}

import org.scalatest.FunSuite

class ThreadFactoryBuilderTest extends FunSuite {

  test("test thread factory builder") {
    var pool = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("Thread %s").setDaemon(true).build())

    var ret = pool.submit(new Callable[String] {
      override def call(): String = {
        assert(Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName() == "Thread 0")
        ""
      }
    })
    // waits and retrieves the result, if above asserts failed, will get execution exception
    ret.get()
    ret = pool.submit(new Callable[String] {
      override def call(): String = {
        assert(Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName() == "Thread 1")
        ""
      }
    })
    ret.get()
    pool.shutdown()

    pool = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("Thread %d").build())
    pool.submit(new Callable[String] {
      override def call(): String = {
        assert(!Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName() == "Thread 0")
        ""
      }
    })
    ret.get()
    pool.submit(new Callable[String] {
      override def call(): String = {
        assert(!Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName() == "Thread 1")
        ""
      }
    })
    ret.get()
    pool.shutdown()
  }
}
