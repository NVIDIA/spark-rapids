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

package org.apache.spark.rapids.tests;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This code helps accelerate root cause investigations for pipeline hangs
 * and prevents other failures from being masked by the hangs in the meantime.
 *
 * This class implements a SparkListener to keep track of Spark Actions (Jobs)
 * taking longer than expected / hanging perpetually leading to the CI pipeline
 * holding on to expensive Compute Cloud resources. Stuck Spark actions are killed
 * with the diagnostic message and a thread dump.
 *
 * Without this class, the pipeline is eventually killed by CI without sufficient diagnostics.
 * One needs to parse the log to keep track which
 * tests are scheduled to run but do not have the corresponding FAILED/SUCCEEDED
 * messages. Out of these tests most pytest items might be false positives because
 * xdist assigns them in batches to workers that become blocked by only one of them
 */
public class TimeoutSparkListener extends SparkListener {
  private static final Logger LOG = LoggerFactory.getLogger(TimeoutSparkListener.class);
  private final JavaSparkContext sparkContext;
  private final int timeoutSeconds;
  private final boolean shouldDumpThreads;
  private final ScheduledExecutorService runner = Executors.newScheduledThreadPool(1,
    runnable -> {
      final Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("spark-job-timeout-thread-" + t.hashCode());
      return t;
    }
  );
  private final Map<Integer,ScheduledFuture<?>> cancelJobMap = new ConcurrentHashMap<>();

  boolean registered;

  public TimeoutSparkListener(JavaSparkContext sparkContext,
    int timeoutSeconds,
    boolean shouldDumpThreads) {
    super();
    this.sparkContext = sparkContext;
    this.timeoutSeconds = timeoutSeconds;
    this.shouldDumpThreads = shouldDumpThreads;
  }

  public synchronized void register() {
    if (!registered) {
      LOG.debug("Adding TimeoutSparkListener to kill hung jobs");
      sparkContext.sc().addSparkListener(this);
      registered = true;
    }
  }

  public synchronized void unregister() {
    if (registered) {
      sparkContext.sc().removeSparkListener(this);
      registered = false;
    }
  }

  public void onJobStart(SparkListenerJobStart jobStart) {
    final int jobId = jobStart.jobId();
    LOG.debug("JobStart: registering timeout for Job {}", jobId);
    final ScheduledFuture<?> scheduledFuture = runner.schedule(() -> {
      final String message = "RAPIDS Integration Test Job " + jobId + " exceeded the timeout of " +
        timeoutSeconds + " seconds, cancelling. " +
        "Look into fixing the test or reducing its execution time. " +
        "If necessary, adjust the timeout using the marker " +
        "pytest.mark.spark_job_timeout(condition, seconds,dump_threads)";
      if (shouldDumpThreads) {
        LOG.error(message + " Driver thread dump follows");
        dumpThreads();
      }
      sparkContext.sc().cancelJob(jobId, message);
    }, timeoutSeconds, TimeUnit.SECONDS);
    cancelJobMap.put(jobId, scheduledFuture);
  }

  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final int jobId = jobEnd.jobId();
    LOG.debug("JobEnd: cancelling timeout for Job {}", jobId);
    final ScheduledFuture<?> cancelFuture = cancelJobMap.remove(jobId);
    cancelFuture.cancel(false);
  }

  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    runner.shutdownNow();
  }

  private static void dumpThreads() {
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo threadInfo : threadMXBean.dumpAllThreads(true, true)) {
      LOG.warn(threadInfo.toString());
    }
  }
}

