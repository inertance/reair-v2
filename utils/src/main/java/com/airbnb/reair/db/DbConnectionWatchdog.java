package com.airbnb.reair.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Periodically checks to see if it's possible to make a connection to the DB. If not, it exits this
 * process. This is to handle a case where for some reason, the MySQL JDBC driver gets in a bad
 * state and is no longer able to make connections. Further debugging is pending.
 */
public class DbConnectionWatchdog extends Thread {

  private static final Log LOG = LogFactory.getLog(DbConnectionWatchdog.class);

  private static final long DB_CONNECTION_CHECK_INTERVAL = 10 * 1000;
  private static final long WATCHDOG_TIMER_LIMIT = 100 * 100;
  private static final String TEST_QUERY = "SELECT 1";

  private long lastSuccessfulConnectionTime = 0;
  private DbConnectionFactory dbConnectionFactory;

  /**
   * Constructor.
   *
   * @param dbConnectionFactory the connection factory to use for making test connections
   */
  public DbConnectionWatchdog(DbConnectionFactory dbConnectionFactory) {
    this.dbConnectionFactory = dbConnectionFactory;
    this.setDaemon(true);
    this.setName(this.getClass().getSimpleName() + "-" + this.getId());
  }

  @Override
  public void run() {
    lastSuccessfulConnectionTime = System.currentTimeMillis();

    while (true) {
      try {
        Connection connection = dbConnectionFactory.getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT 1");
        ps.execute();
        LOG.debug("Successfully executed " + TEST_QUERY);
        lastSuccessfulConnectionTime = System.currentTimeMillis();
      } catch (SQLException e) {
        LOG.error("Got an exception when executing " + TEST_QUERY, e);
      }

      // If too long has passed since a last successful query, exit the
      // server so that it can restart.
      long timeSinceLastSuccessfulConnection =
          System.currentTimeMillis() - lastSuccessfulConnectionTime;
      if (timeSinceLastSuccessfulConnection > WATCHDOG_TIMER_LIMIT) {
        LOG.error(String.format(
            "Too much time has elapsed since the " + "last successful DB connection (Elapsed: %sms "
                + "Limit: %sms). Exiting...",
            timeSinceLastSuccessfulConnection, WATCHDOG_TIMER_LIMIT));
        System.exit(-1);
      }
      try {
        Thread.sleep(DB_CONNECTION_CHECK_INTERVAL);
      } catch (InterruptedException e) {
        LOG.error("Got interrupted! Exiting...", e);
        System.exit(-1);
      }
    }
  }
}
