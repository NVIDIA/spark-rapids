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
  private static JavaSparkContext sparkContext;
  private static int timeoutSeconds;
  private static boolean shouldDumpThreads;
  private static final ScheduledExecutorService runner = Executors.newScheduledThreadPool(1,
    runnable -> {
      final Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("spark-job-timeout-thread-" + t.hashCode());
      return t;
    }
  );
  private static final Map<Integer,ScheduledFuture<?>> cancelJobMap = new ConcurrentHashMap<>();
  private static final TimeoutSparkListener SINGLETON = new TimeoutSparkListener();

  public TimeoutSparkListener() {
    super();
  }

  public static synchronized void init(JavaSparkContext sc) {
    if (sparkContext == null) {
      sparkContext = sc;
      sparkContext.sc().addSparkListener(SINGLETON);
    }
  }

  private static synchronized void unregister() {
    if (sparkContext != null) {
      sparkContext.sc().removeSparkListener(SINGLETON);
      sparkContext = null;
    }
  }

  private static synchronized void cancelJob(int jobId, String message) {
    if (sparkContext != null) {
      sparkContext.sc().cancelJob(jobId, message);
    }
    LOG.error(message + ". Shutting down the Driver JVM; xdist worker will stop as well per " +
"https://github.com/NVIDIA/spark-rapids/pull/12455. Pending tests will be re-executed " +
"in the replacement worker");
    System.exit(timeoutSeconds);
  }

  public void onJobStart(SparkListenerJobStart jobStart) {
    final int jobId = jobStart.jobId();
    LOG.debug("JobStart: registering timeout for Job {}", jobId);
    // create a task config snapshot
    final boolean taskShouldDumpThreads = shouldDumpThreads;
    final int taskTimeout = timeoutSeconds;
    final ScheduledFuture<?> scheduledFuture = runner.schedule(() -> {
      final String message = "RAPIDS Integration Test Job " + jobId + " exceeded the timeout of " +
        timeoutSeconds + " seconds, cancelling. " +
        "Look into fixing the test or reducing its execution time. " +
        "If necessary, adjust the timeout using the marker " +
        "pytest.mark.spark_job_timeout(seconds,dump_threads)";
      if (taskShouldDumpThreads) {
        LOG.error(message + " Driver thread dump follows");
        dumpThreads();
      }
      cancelJob(jobId, message);
    }, taskTimeout, TimeUnit.SECONDS);
    cancelJobMap.put(jobId, scheduledFuture);
  }

  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final int jobId = jobEnd.jobId();
    LOG.debug("JobEnd: cancelling timeout for Job {}", jobId);
    final ScheduledFuture<?> cancelFuture = cancelJobMap.remove(jobId);
    if (cancelFuture != null) {
      cancelFuture.cancel(false);
    } else {
      LOG.debug("Timeout task for Job {} not found", jobId);
    }
  }

  public static void setSparkJobTimeout(int ts, boolean dumpThreads) {
    timeoutSeconds = ts;
    shouldDumpThreads = dumpThreads;
  }

  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    unregister();
    // no new work
    runner.shutdownNow();
    cancelJobMap.clear();
  }

  private static void dumpThreads() {
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo threadInfo : threadMXBean.dumpAllThreads(true, true)) {
      LOG.warn(threadInfo.toString());
    }
  }
}

