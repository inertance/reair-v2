package com.airbnb.reair.utils;

import org.apache.log4j.Logger;

/**
 * Utility to re-run a method in case it throws transient exception. E.g. when inserting into a DB,
 * the DB might temporarily be down or there might be network issues.
 */
public class RetryingTaskRunner {
  public static Logger LOG = Logger.getLogger(RetryingTaskRunner.class);

  public static final int DEFAULT_NUM_ATTEMPTS = 10;
  public static final int DEFAULT_BASE_SLEEP_INTERVAL = 2;
  public static final int DEFAULT_MAX_SLEEP_INTERVAL = 10 * 60;

  private int numAttempts;
  private int baseSleepInterval;
  private int maxSleepInterval;


  public RetryingTaskRunner(int numAttempts, int baseSleepInterval) {
    this.numAttempts = numAttempts;
    this.baseSleepInterval = baseSleepInterval;
  }

  /**
   * @param defaultNumAttempts number of total attempts to run task
   * @param baseSleepInterval wait {@code baseSleepInterval}*2^(attempt no.) between attempts.
   * @param maxSleepInterval wait {@code }
   */
  public RetryingTaskRunner(int defaultNumAttempts, int baseSleepInterval, int maxSleepInterval) {
    this.numAttempts = defaultNumAttempts;
    this.baseSleepInterval = baseSleepInterval;
    this.maxSleepInterval = maxSleepInterval;
  }

  public RetryingTaskRunner() {
    this(DEFAULT_NUM_ATTEMPTS, DEFAULT_BASE_SLEEP_INTERVAL, DEFAULT_MAX_SLEEP_INTERVAL);
  }

  /**
   * Run a task, retrying a fixed number of times if there is a failure.
   *
   * @param task the task to run.
   *
   * @throws Exception if there's an error with running the task
   */
  public void runWithRetries(RetryableTask task) throws Exception {
    boolean maxSleepIntervalHit = false;
    for (int i = 0; i < numAttempts; i++) {
      try {
        task.run();
        return;
      } catch (Exception e) {
        if (i == numAttempts - 1) {
          // We ran out of attempts - propagate up
          throw e;
        }
        // Otherwise, retry after a little bit
        int sleepTime;
        if (maxSleepIntervalHit) {
          sleepTime = maxSleepInterval;
        } else {
          sleepTime = baseSleepInterval * (int) Math.pow(2, i);
        }
        LOG.error("Got an exception! Sleeping for " + sleepTime + " seconds and retrying.", e);

        try {
          Thread.sleep(sleepTime * 1000);
        } catch (InterruptedException ie) {
          LOG.error("Unexpected interruption!", ie);
          throw ie;
        }
      }
    }
  }

  /**
   * Run a task and retry until it succeeds.
   *
   * @param task the task to run
   */
  public void runUntilSuccessful(RetryableTask task) {
    boolean maxSleepIntervalHit = false;
    int numAttempts = 0;
    while (true) {
      try {
        task.run();
        return;
      } catch (Exception e) {
        // Otherwise, retry after a little bit
        int sleepTime;
        if (maxSleepIntervalHit) {
          sleepTime = maxSleepInterval;
        } else {
          sleepTime = baseSleepInterval * (int) Math.pow(2, numAttempts);
          if (sleepTime > maxSleepInterval) {
            sleepTime = maxSleepInterval;
            maxSleepIntervalHit = true;
          }
        }
        LOG.error("Got an exception! Sleeping for " + sleepTime + " seconds and retrying.", e);

        try {
          Thread.sleep(sleepTime * 1000);
        } catch (InterruptedException ie) {
          LOG.error("Unexpected interruption!", ie);
        }
      }
      numAttempts++;
    }
  }
}
