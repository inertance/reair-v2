package com.airbnb.reair.db;

import java.io.IOException;

/**
 * Interface for classes that can return username / passwords for connecting to a DB.
 */
public interface DbCredentials {

  /**
   * Called if the credentials should be refreshed (e.g. re-read from file).
   *
   * @throws IOException if there's an error reading the credentials
   */
  void refreshCredsIfNecessary() throws IOException;

  /**
   * Get the username that has read and write privileges.
   *
   * @return the username that has read / write access to the DB
   *
   * @throws IOException if there an error reading the credentials
   */
  String getReadWriteUsername() throws IOException;

  /**
   * Get the password associated with the user that has read / write access to the DB.
   *
   * @return the password for the user that has read / write access to the DB
   *
   * @throws IOException if there's an error reading the credentials
   */
  String getReadWritePassword() throws IOException;
}
