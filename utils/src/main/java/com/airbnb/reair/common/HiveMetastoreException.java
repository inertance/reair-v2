package com.airbnb.reair.common;

/**
 * Exception thrown when there is an issue with the Hive metastore.
 */
public class HiveMetastoreException extends Exception {

  public HiveMetastoreException(String message) {
    super(message);
  }

  public HiveMetastoreException(Throwable throwable) {
    super(throwable);
  }
}
