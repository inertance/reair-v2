package com.airbnb.reair.hive.hooks;

/**
 * Exception thrown when there is an error when processing an Entity.
 */
public class EntityException extends Exception {
  public EntityException(Throwable cause) {
    super(cause);
  }

  public EntityException(String message) {
    super(message);
  }

  public EntityException(String message, Throwable cause) {
    super(message, cause);
  }
}
