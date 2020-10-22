package com.airbnb.reair.common;

/**
 * An exception thrown when there's an error running DistCp.
 */
public class DistCpException extends Exception {

  public DistCpException(String message) {
    super(message);
  }

  public DistCpException(String message, Throwable cause) {
    super(message, cause);
  }

  public DistCpException(Throwable cause) {
    super(cause);
  }
}
