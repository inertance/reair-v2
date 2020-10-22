package com.airbnb.reair.common;

/**
 * Result object passed from running a process.
 */
public class RunResult {

  private final int returnCode;
  private final String stdout;

  public RunResult(int returnCode, String stdout) {
    this.returnCode = returnCode;
    this.stdout = stdout;
  }

  public String getStdout() {
    return stdout;
  }

  public int getReturnCode() {

    return returnCode;
  }
}
