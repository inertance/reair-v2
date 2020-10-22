package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Interface for filtering out objects to replicate.
 */
public interface ReplicationFilter {
  void setConf(Configuration conf);

  /**
   * Check to see if the given entry should be replicated.
   *
   * @param entry audit log entry to examine
   * @return whether or not the given audit log entry should be accepted
   */
  boolean accept(AuditLogEntry entry);

  /**
   * Check to see if the given entry should be replicated.
   *
   * @param table Hive Thrift table object to examine
   * @return whether or not the given table should be accepted
   */
  boolean accept(Table table);

  /**
   * Check to see if the given entry should be replicated.
   *
   * @param table table associated with the partition
   * @param partition partition to examine
   * @return whether or not the partition should be accepted
   */
  boolean accept(Table table, NamedPartition partition);
}
