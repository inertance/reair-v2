package com.airbnb.reair.incremental.db;

/**
 * Constants used for DB operations.
 */
public class DbConstants {
  // Default number of retries to run when a DB operation fails
  public static final int DEFAULT_NUM_RETRIES = 9;
  // When sleeping between retries, use this base number when calculating the exponential delay
  public static final int DEFAULT_RETRY_EXPONENTIAL_BASE = 2;
}
