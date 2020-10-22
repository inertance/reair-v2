package com.airbnb.reair.db;

import com.airbnb.reair.common.Container;
import com.airbnb.reair.utils.RetryableTask;
import com.airbnb.reair.utils.RetryingTaskRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * A simple string key/value store using a DB.
 */
public class DbKeyValueStore {

  private static final Log LOG = LogFactory.getLog(DbKeyValueStore.class);

  private DbConnectionFactory dbConnectionFactory;
  private String dbTableName;
  private RetryingTaskRunner retryingTaskRunner = new RetryingTaskRunner();

  /**
   * Constructor.
   *
   * @param dbConnectionFactory connection factory to use for connecting to the DB
   * @param dbTableName name of the table containing the keys and values
   */
  public DbKeyValueStore(DbConnectionFactory dbConnectionFactory, String dbTableName) {

    this.dbTableName = dbTableName;
    this.dbConnectionFactory = dbConnectionFactory;
  }

  /**
   * Get the create table command for the key/value table.
   *
   * @param tableName name of the table
   * @return SQL that can be executed to create the table
   */
  public static String getCreateTableSql(String tableName) {
    return String.format("CREATE TABLE `%s` (\n"
        + "  `update_time` timestamp NOT NULL DEFAULT "
        + "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "  `key_string` varchar(256) NOT NULL,\n"
        + "  `value_string` varchar(4000) DEFAULT NULL,\n" + "  PRIMARY KEY (`key_string`)\n"
        + ") ENGINE=InnoDB", tableName);
  }

  /**
   * Get the value of the key.
   *
   * @param key name of the key
   * @return the value associated with they key
   *
   * @throws SQLException if there's an error querying the DB
   */
  public Optional<String> get(String key) throws SQLException {
    Connection connection = dbConnectionFactory.getConnection();
    String query =
        String.format("SELECT value_string FROM %s " + "WHERE key_string = ? LIMIT 1", dbTableName);
    PreparedStatement ps = connection.prepareStatement(query);
    try {
      ps.setString(1, key);
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        return Optional.ofNullable(rs.getString(1));
      } else {
        return Optional.empty();
      }
    } finally {
      ps.close();
      ps = null;
    }
  }

  /**
   * Sets the value for a key, retrying if necessary.
   *
   * @param key the key to set
   * @param value the value to associate with the key
   */
  public void resilientSet(final String key, final String value) {
    retryingTaskRunner.runUntilSuccessful(new RetryableTask() {
      @Override
      public void run() throws Exception {
        set(key, value);
      }
    });
  }

  /**
   * Sets the value for a key.
   *
   * @param key the key to set
   * @param value the value to associate with the key
   *
   * @throws SQLException if there's an error querying the DB
   */
  public void set(String key, String value) throws SQLException {
    LOG.debug("Setting " + key + " to " + value);
    Connection connection = dbConnectionFactory.getConnection();
    String query = String.format("INSERT INTO %s (key_string, value_string) "
        + "VALUE (?, ?) ON DUPLICATE KEY UPDATE value_string = ?", dbTableName);

    PreparedStatement ps = connection.prepareStatement(query);
    try {
      ps.setString(1, key);
      ps.setString(2, value);
      ps.setString(3, value);
      ps.executeUpdate();
    } finally {
      ps.close();
      ps = null;
    }
  }
}
