package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

/**
 * Runs a process while streaming stdout and stderr to log4j.
 */
public class ProcessRunner {

  private static final Log LOG = LogFactory.getLog(ProcessRunner.class);

  private List<String> args;

  public ProcessRunner(List<String> args) {
    this.args = args;
  }

  /**
   * TODO.
   *
   * @return TODO
   *
   * @throws ProcessRunException TODO
   */
  public RunResult run() throws ProcessRunException {
    try {
      LOG.debug("Running: " + Arrays.asList(args));
      Process process = new ProcessBuilder(args).start();
      printPid(process);
      String currentThreadName = Thread.currentThread().getName();
      StreamLogger stdoutLogger =
          new StreamLogger(currentThreadName + "-child-stdout", process.getInputStream(), true);
      StreamLogger stderrLogger =
          new StreamLogger(currentThreadName + "-child-stderr", process.getErrorStream(), false);

      stdoutLogger.start();
      stderrLogger.start();

      stdoutLogger.join();
      stderrLogger.join();

      int returnCode = process.waitFor();
      return new RunResult(returnCode, stdoutLogger.getStreamAsString());
    } catch (IOException e) {
      throw new ProcessRunException(e);
    } catch (InterruptedException e) {
      throw new ProcessRunException("Shouldn't be interrupted!", e);
    }
  }

  private static void printPid(Process process) {
    // There's no legit way to get the PID
    if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
      try {
        Field field = process.getClass().getDeclaredField("pid");
        field.setAccessible(true);
        long pid = field.getInt(process);
        LOG.debug("PID is " + pid);
      } catch (Throwable e) {
        LOG.error("Unable to get PID!");
      }
    }
  }
}
