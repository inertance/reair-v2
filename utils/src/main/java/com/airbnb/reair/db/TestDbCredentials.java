package com.airbnb.reair.db;

import com.airbnb.reair.db.DbCredentials;

/**
 * Credentials for connecting to the EmbeddedMySqlDb.
 */
public class TestDbCredentials implements DbCredentials {
  @Override

  public void refreshCredsIfNecessary() {
  }

  @Override
  public String getReadWriteUsername() {
    return "root";
  }

  @Override
  public String getReadWritePassword() {
    return "";
  }
}
