package com.airbnb.reair.hive.hooks;

import com.airbnb.reair.db.DbCredentials;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class ConfigurationDbCredentials implements DbCredentials {

  private Configuration conf;
  private String usernameKey;
  private String passwordKey;

  /**
   * Constructor.
   *
   * @param conf configuration object that contain the credentials
   * @param usernameKey the key to use for fetching the username
   * @param passwordKey the key to use for feting the password
   */
  public ConfigurationDbCredentials(Configuration conf, String usernameKey, String passwordKey) {
    this.conf = conf;
    this.usernameKey = usernameKey;
    this.passwordKey = passwordKey;
  }

  @Override
  public void refreshCredsIfNecessary() throws IOException {}

  @Override
  public String getReadWriteUsername() throws IOException {
    String username = conf.get(usernameKey);
    if (username == null) {
      throw new IOException("Key missing value: " + usernameKey);
    }
    return username;
  }

  @Override
  public String getReadWritePassword() throws IOException {
    String password = conf.get(passwordKey);
    if (password == null) {
      throw new IOException("Key missing value: " + passwordKey);
    }
    return password;
  }
}
