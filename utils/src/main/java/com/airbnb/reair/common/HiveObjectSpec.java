package com.airbnb.reair.common;

import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Specification for a Hive object (table or partition). Used because having 3 arguments (db, table,
 * partition) for every function gets old.
 */
public class HiveObjectSpec {
  private String dbName = null;

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  private String tableName = null;
  private String partitionName = null;

  /**
   * Constructor using a Thrift Hive table.
   *
   * @param table Hive table
   */
  public HiveObjectSpec(Table table) {
    this(table.getDbName(), table.getTableName());
  }

  /**
   * Constructor using a Thrift Hive partition.
   *
   * @param namedPartition Hive partition
   */
  public HiveObjectSpec(NamedPartition namedPartition) {
    this(
        namedPartition.getPartition().getDbName(),
        namedPartition.getPartition().getTableName(),
        namedPartition.getName());
  }

  public HiveObjectSpec(String dbName, String tableName) {
    this.dbName = dbName;
    this.tableName = tableName;
  }

  /**
   * Constructor using specified names.
   *
   * @param dbName Hive database name
   * @param tableName Hive table name
   * @param partitionName Hive partition name
   */
  public HiveObjectSpec(String dbName, String tableName, String partitionName) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.partitionName = partitionName;
  }

  public boolean isPartition() {
    return this.partitionName != null;
  }

  @Override
  public String toString() {
    if (partitionName == null) {
      return String.format("%s.%s", dbName, tableName);
    } else {
      return String.format("%s.%s/%s", dbName, tableName, partitionName);
    }
  }

  /**
   * When this specifies a partition, return the specification for the table that this partition
   * resides in.
   *
   * @return specification for this partition's table
   */
  public HiveObjectSpec getTableSpec() {
    if (!isPartition()) {
      throw new RuntimeException("Should only be called for " + "partition specs!");
    }

    return new HiveObjectSpec(dbName, tableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    HiveObjectSpec that = (HiveObjectSpec) obj;

    if (!dbName.equals(that.dbName)) {
      return false;
    }
    if (partitionName != null ? !partitionName.equals(that.partitionName)
        : that.partitionName != null) {
      return false;
    }
    if (!tableName.equals(that.tableName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = dbName.hashCode();
    result = 31 * result + tableName.hashCode();
    result = 31 * result + (partitionName != null ? partitionName.hashCode() : 0);
    return result;
  }
}
