package com.airbnb.reair.utils;

/**
 * A task that can be retried.
 */
public interface RetryableTask {
  void run() throws Exception;
}
