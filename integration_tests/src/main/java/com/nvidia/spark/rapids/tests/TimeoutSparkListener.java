/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeoutSparkListener extends SparkListener {
  private final Logger LOG = LoggerFactory.getLogger(TimeoutSparkListener.class);
  private final JavaSparkContext sparkContext;
  private final int timeoutSeconds;
  private final ScheduledExecutorService runner = Executors.newScheduledThreadPool(1);
  private final Map<Integer,ScheduledFuture<?>> cancelJobMap = new ConcurrentHashMap<>();
  
  boolean registered;

  public TimeoutSparkListener(int timeoutSeconds, JavaSparkContext sparkContext) {
    super();
    this.timeoutSeconds = timeoutSeconds;
    this.sparkContext = sparkContext;
  }

  public synchronized void register() {
    if (!registered) {
      LOG.debug("Adding TimeoutSparkListener to kill hung jobs");
      sparkContext.sc().addSparkListener(this);
      registered = true;
    }
  }

  public void onJobStart(SparkListenerJobStart jobStart) {
    LOG.debug("JobStart: registering timeout for Job {}", jobStart.jobId());
    final ScheduledFuture<?> scheduledFuture = runner.schedule(() -> {
      LOG.error("Job {} has timed out, cancelling!!!", jobStart.jobId());
      sparkContext.sc().cancelJob(jobStart.jobId());
    }, timeoutSeconds, TimeUnit.SECONDS);
    cancelJobMap.put(jobStart.jobId(), scheduledFuture);
  }

  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    LOG.debug("JobEnd: cancelling timeout for Job {}", jobEnd.jobId());
    final ScheduledFuture<?> cancelFuture = cancelJobMap.remove(jobEnd.jobId());
    cancelFuture.cancel(false);
  }

  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    for (
      final Iterator<Entry<Integer,ScheduledFuture<?>>> it = cancelJobMap.entrySet().iterator(); 
      it.hasNext(); 
    ) {
      final Entry<Integer,ScheduledFuture<?>> cancelEntry = it.next();
      LOG.debug("ApplicationEnd: cancelling pending Spark job {}", cancelEntry.getKey());
      cancelEntry.getValue().cancel(false);
      it.remove();
    }
  }
}

