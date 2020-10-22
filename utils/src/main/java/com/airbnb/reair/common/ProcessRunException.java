package com.airbnb.reair.common;

/**
 * Exception related to running a (Linux) process.
 */
public class ProcessRunException extends Exception {

  public ProcessRunException(String message) {
    super(message);
  }

  public ProcessRunException(Exception exception) {
    super(exception);
  }

  public ProcessRunException(String message, Exception exception) {
    super(message, exception);
  }
}
