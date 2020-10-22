package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Filter that passes everything.
 */
public class PassThoughReplicationFilter implements ReplicationFilter {
  @Override
  public void setConf(Configuration conf) {
    return;
  }

  @Override
  public boolean accept(AuditLogEntry entry) {
    System.out.println("this is PassThoughReplicationFilter accept entry renturn true");
    return true;
  }

  @Override
  public boolean accept(Table table) {
    return true;
  }

  @Override
  public boolean accept(Table table, NamedPartition partition) {
    return true;
  }
}
