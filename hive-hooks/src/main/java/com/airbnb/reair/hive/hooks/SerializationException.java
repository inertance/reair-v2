package com.airbnb.reair.hive.hooks;

/**
 * Exception related to serializing data to insert into a DB.
 */
public class SerializationException extends Exception {
  public SerializationException(String message) {
    super(message);
  }

  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SerializationException(Throwable cause) {
    super(cause);
  }
}
