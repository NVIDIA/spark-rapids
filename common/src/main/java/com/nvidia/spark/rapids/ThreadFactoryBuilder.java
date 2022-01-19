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

package com.nvidia.spark.rapids;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

// This is copied from Guava.
// Guava has versions, avoid to use Guava.
public final class ThreadFactoryBuilder {
  private String nameFormat = null;
  private Boolean daemon = null;
  private Integer priority = null;
  private UncaughtExceptionHandler uncaughtExceptionHandler = null;
  private ThreadFactory backingThreadFactory = null;

  public ThreadFactoryBuilder() {
  }

  public ThreadFactoryBuilder setNameFormat(String nameFormat) {
    String.format(nameFormat, 0);
    this.nameFormat = nameFormat;
    return this;
  }

  public ThreadFactoryBuilder setDaemon(boolean daemon) {
    this.daemon = daemon;
    return this;
  }

  public ThreadFactoryBuilder setPriority(int priority) {
    Utils.checkArgument(priority >= 1, String.format("Thread priority (%s) must be >= %s", priority, 1));
    Utils.checkArgument(priority <= 10, String.format("Thread priority (%s) must be <= %s", priority, 10));
    this.priority = priority;
    return this;
  }

  public ThreadFactoryBuilder setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.uncaughtExceptionHandler = Utils.checkNotNull(uncaughtExceptionHandler);
    return this;
  }

  public ThreadFactoryBuilder setThreadFactory(ThreadFactory backingThreadFactory) {
    this.backingThreadFactory = Utils.checkNotNull(backingThreadFactory);
    return this;
  }

  public ThreadFactory build() {
    return build(this);
  }

  private static ThreadFactory build(ThreadFactoryBuilder builder) {
    final String nameFormat = builder.nameFormat;
    final Boolean daemon = builder.daemon;
    final Integer priority = builder.priority;
    final UncaughtExceptionHandler uncaughtExceptionHandler = builder.uncaughtExceptionHandler;
    final ThreadFactory backingThreadFactory =
      builder.backingThreadFactory != null ? builder.backingThreadFactory : Executors.defaultThreadFactory();
    final AtomicLong count = nameFormat != null ? new AtomicLong(0L) : null;
    return runnable -> {
      Thread thread = backingThreadFactory.newThread(runnable);
      if (nameFormat != null) {
        thread.setName(String.format(nameFormat, count.getAndIncrement()));
      }

      if (daemon != null) {
        thread.setDaemon(daemon);
      }

      if (priority != null) {
        thread.setPriority(priority);
      }

      if (uncaughtExceptionHandler != null) {
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
      }

      return thread;
    };
  }
}
