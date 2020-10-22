package com.airbnb.reair.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for a factory that returns connections to a DB.
 */
public interface DbConnectionFactory {

  Connection getConnection() throws SQLException;

}

