package com.airbnb.reair.hive.hooks;

/**
 * An exception thrown when there is an error with the configuration.
 */
public class ConfigurationException extends Exception {
  public ConfigurationException() {}

  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigurationException(Throwable cause) {
    super(cause);
  }
}
