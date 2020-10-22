package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Runs a process, retrying if necessary.
 */
public class RetryingProcessRunner {
  private static final Log LOG = LogFactory.getLog(RetryingProcessRunner.class);

  private static final int DEFAULT_NUM_RETRIES = 3;
  private static final long RETRY_SLEEP_TIME = 10 * 60 * 1000;
  private int retries;

  public RetryingProcessRunner() {
    this.retries = DEFAULT_NUM_RETRIES;
  }

  public RetryingProcessRunner(int retries) {
    this.retries = retries;
  }

  /**
   * Run a shell command.
   *
   * @param args shell arguments to call
   * @return the result of running the command
   *
   * @throws ProcessRunException if there's an error running the process
   */
  public RunResult run(List<String> args) throws ProcessRunException {

    for (int i = 0; i < retries; i++) {

      LOG.debug("Running: " + args);
      ProcessRunner runner = new ProcessRunner(args);
      RunResult result = runner.run();
      if (result.getReturnCode() == 0) {
        return result;
      }

      LOG.error("Error running command! Got return code: " + result.getReturnCode());

      if (i + 1 == retries) {
        return result;
      }

      try {
        LOG.debug("Sleeping for " + RETRY_SLEEP_TIME / 1000 + "s");
        Thread.sleep(RETRY_SLEEP_TIME);
      } catch (InterruptedException e) {
        throw new RuntimeException("Shouldn't happen!");
      }
    }
    throw new RuntimeException("Shouldn't happen");
  }
}
