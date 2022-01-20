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

import java.util.concurrent.Executors

import org.scalatest.FunSuite

class ThreadFactoryBuilderTest extends FunSuite {

  test("test thread factory builder") {
    var pool = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("Thread %s").setDaemon(true).build())

    pool.submit(new Runnable {
      override def run(): Unit = {
        assert(Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName == "Thread 0")
      }
    })

    pool.submit(new Runnable {
      override def run(): Unit = {
        assert(Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName == "Thread 1")
      }
    })

    pool = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("Thread %s").build())
    pool.submit(new Runnable {
      override def run(): Unit = {
        assert(!Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName == "Thread 0")
      }
    })

    pool.submit(new Runnable {
      override def run(): Unit = {
        assert(!Thread.currentThread().isDaemon)
        assert(Thread.currentThread().getName == "Thread 1")
      }
    })
  }
}
