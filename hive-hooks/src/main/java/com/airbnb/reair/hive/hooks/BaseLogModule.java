package com.airbnb.reair.hive.hooks;

import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.Connection;

/**
 * Base class for audit log modules that write to the database. Audit log modules are run by the
 * audit log hook.
 */
public abstract class BaseLogModule {

  protected final SessionStateLite sessionStateLite;
  // The database connection that audit information to be written via
  protected final Connection connection;
  // The table that audit information should be rewritten to
  protected final String tableName;

  /**
   * Base constructor for log modules.
   *
   * @param connection database connection to write the logs to
   * @param tableNameKey the config key for the table name to write the logs to
   * @param sessionStateLite the session state that contains relevant config
   *
   * @throws ConfigurationException when the table name is not defined in the configuration
   */
  public BaseLogModule(final Connection connection,
                       final String tableNameKey,
                       final SessionStateLite sessionStateLite)
           throws ConfigurationException {
    this.connection = connection;
    this.sessionStateLite = sessionStateLite;

    // Ensure the table name is set in the config, and fetch it
    final HiveConf conf = sessionStateLite.getConf();
    tableName = conf.get(tableNameKey);
    if (tableName == null) {
      throw new ConfigurationException(
          String.format("%s is not defined in the conf!", tableNameKey));
    }
  }
}
