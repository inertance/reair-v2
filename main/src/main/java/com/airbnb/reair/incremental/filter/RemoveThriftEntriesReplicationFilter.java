package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * To filter out Thrift events from the audit log.
 */
public class RemoveThriftEntriesReplicationFilter implements ReplicationFilter {

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean accept(AuditLogEntry entry) {
    System.out.println("this is RemoveThriftEntriesReplicationFilter accept entry go to swith entry.getCommandType");
    switch (entry.getCommandType()) {
      case THRIFT_ADD_PARTITION:
          System.out.println("commandtype is THRIFT_ADD_PARTITION go to end");
      case THRIFT_ALTER_PARTITION:
          System.out.println("commandtype is THRIFT_ALTER_PARTITION go to end");
      case THRIFT_ALTER_TABLE:
          System.out.println("commandtype is THRIFT_ADD_TABLE go to end");
      case THRIFT_CREATE_DATABASE:
          System.out.println("commandtype is THRIFT_CREATE_DATABASE go to end");
      case THRIFT_CREATE_TABLE:
          System.out.println("commandtype is THRIFT_CREATE_TABLE go to end");
      case THRIFT_DROP_DATABASE:
          System.out.println("commandtype is THRIFT_DROP_DATABASE go to end");
      case THRIFT_DROP_PARTITION:
          System.out.println("commandtype is THRIFT_DROP_PARTITION go to end");
      case THRIFT_DROP_TABLE:
          System.out.println("commandtype is THRIFT_DROP_TABLE go to end");
      // for completeness we need to replicate exchange partition from
      // the normal audit log besides the thrift events
      case ALTERTABLE_EXCHANGEPARTITION:
          System.out.println("commandtype is THRIFT_ADD_PARTITION return false");
        return true;

      default:
          System.out.println("commandtype is default end return true");
        return false;
    }
  }

  @Override
  public boolean accept(Table table) {
    return accept(table, null);
  }

  @Override
  public boolean accept(Table table, NamedPartition partition) {
    return true;
  }
}
