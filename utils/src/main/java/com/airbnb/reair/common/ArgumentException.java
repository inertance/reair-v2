package com.airbnb.reair.common;

/**
 * Exception thrown when there is an error with the supplied arguments.
 */
public class ArgumentException extends Exception {
  public ArgumentException(String message) {
    super(message);
  }

  public ArgumentException(String message, Throwable cause) {
    super(message, cause);
  }

  public ArgumentException(Throwable cause) {
    super(cause);
  }
}
