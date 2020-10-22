package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Filters out objects from the audit log using regular expressions specified in the configuration.
 */
public class RegexReplicationFilter implements ReplicationFilter {

  private static final Log LOG = LogFactory.getLog(RegexReplicationFilter.class);

  public static final String WHITELIST_REGEX_KEY = "airbnb.reair.whitelist.regex";
  public static final String BLACKLIST_REGEX_KEY = "airbnb.reair.blacklist.regex";

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean accept(AuditLogEntry entry) {
    System.out.println("this is RegexReplicationFilter accept entry renturn true");
    return true;
  }

  @Override
  public boolean accept(Table table) {
    return accept(table, null);
  }

  @Override
  public boolean accept(Table table, NamedPartition partition) {
    return matchesRegex(table.getDbName(), table.getTableName(),
        partition == null ? null : partition.getName());
  }

  private boolean matchesRegex(String dbName, String tableName, String partitionName) {
    HiveObjectSpec spec = new HiveObjectSpec(dbName, tableName, partitionName);
    String objectName = spec.toString();
    LOG.info("RegexFilter accept matchesRegex:" + objectName);
    String whitelistRegex = conf.get(WHITELIST_REGEX_KEY);
    if (whitelistRegex == null) {
      LOG.warn("Missing value for whitelist key: " + WHITELIST_REGEX_KEY);
      return false;
    }
    if (!objectName.matches(whitelistRegex)) {
      LOG.info("objectName no matches whitelistRegex return false");
      return false;
    }
    String blacklistRegex = conf.get(BLACKLIST_REGEX_KEY);
    if (blacklistRegex == null) {
      LOG.warn("Missing value for blacklist key: " + BLACKLIST_REGEX_KEY);
      // It can be accepted since it passed the whitelist
      return true;
    }
    return !objectName.matches(blacklistRegex);
  }
}
