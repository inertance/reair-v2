package com.airbnb.reair.incremental;

/**
 * Exception thrown when there is an error updating the state of a job.
 */
public class StateUpdateException extends Exception {
  public StateUpdateException() {
    super();
  }

  public StateUpdateException(String message) {
    super(message);
  }

  public StateUpdateException(String message, Throwable cause) {
    super(message, cause);
  }

  public StateUpdateException(Throwable cause) {
    super(cause);
  }
}
