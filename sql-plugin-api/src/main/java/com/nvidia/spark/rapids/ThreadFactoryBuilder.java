/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is similar to Guava ThreadFactoryBuilder. Avoid using Guava as it is a messy dependency
 * in practice.
 */
public class ThreadFactoryBuilder {
  private String nameFormat;
  private Boolean daemon;

  public ThreadFactoryBuilder setNameFormat(String nameFormat) {
    String.format(nameFormat, 0);
    this.nameFormat = nameFormat;
    return this;
  }

  public ThreadFactoryBuilder setDaemon(boolean daemon) {
    this.daemon = daemon;
    return this;
  }

  public ThreadFactory build() {
    AtomicLong count = nameFormat == null ? null : new AtomicLong(0);
    return new ThreadFactory() {
      private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = defaultThreadFactory.newThread(runnable);
        if (nameFormat != null) {
          thread.setName(String.format(nameFormat, count.getAndIncrement()));
        }
        if (daemon != null) {
          thread.setDaemon(daemon);
        }
        return thread;
      }
    };
  }
}
