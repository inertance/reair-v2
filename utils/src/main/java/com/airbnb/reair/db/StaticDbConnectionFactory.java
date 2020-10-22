package com.airbnb.reair.db;

import com.airbnb.reair.utils.RetryableTask;
import com.airbnb.reair.utils.RetryingTaskRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A factory that creates connections to a DB based on connection information supplied in the
 * constructor.
 */
public class StaticDbConnectionFactory implements DbConnectionFactory {

  private static final Log LOG = LogFactory.getLog(StaticDbConnectionFactory.class);

  private String jdbcUrl;
  private String username;
  private String password;

  private Connection connection;
  private RetryingTaskRunner retryingTaskRunner;

  /**
   * Constructor using specified connection information.
   *
   * @param jdbcUrl the JDBC connection URL
   * @param username the username
   * @param password the password associated with the username
   */
  public StaticDbConnectionFactory(String jdbcUrl, String username, String password) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.retryingTaskRunner = new RetryingTaskRunner();

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
    } catch (ClassNotFoundException e) {
      LOG.error(e);
    } catch (IllegalAccessException e) {
      LOG.error(e);
    } catch (InstantiationException e) {
      LOG.error(e);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    retryingTaskRunner.runUntilSuccessful(new RetryableTask() {
      @Override
      public void run() throws Exception {
        if (connection == null || !connection.isValid(5)) {
          LOG.debug("Connecting to " + jdbcUrl);
          connection = DriverManager.getConnection(jdbcUrl, username, password);
        }
      }
    });
    return connection;

  }
}
