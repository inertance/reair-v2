package com.airbnb.reair.incremental.auditlog;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.hive.hooks.HiveOperation;

import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class AuditLogEntry {

  // The audit log has more fields, but only these are relevant for
  // replication.
  private long id;
  private Timestamp createTime;
  private String command;
  private HiveOperation commandType;
  private List<String> outputDirectories;
  private List<Table> referenceTables;
  private List<Table> outputTables;
  private List<NamedPartition> outputPartitions;
  private Table inputTable;
  private NamedPartition inputPartition;

  /**
   * Constructs AuditLogEntry using specific values.
   *
   * @param id ID of the row in the DB
   * @param createTime time that the audit log entry was created
   * @param commandType type of Hive command e.g QUERY
   * @param command the command string e.g. 'CREATE TABLE...'
   * @param outputDirectories for queries that write to directories, the directories that were
   *                          written
   * @param referenceTables the partition's table if the outputs include partitions
   * @param outputTables tables that were changed
   * @param outputPartitions partitions that were changed
   * @param inputTable if renaming a table, the table that was renamed from
   * @param inputPartition if renaming a partition, the partition that was renamed from.
   */
  public AuditLogEntry(
      long id,
      Timestamp createTime,
      HiveOperation commandType,
      String command,
      List<String> outputDirectories,
      List<Table> referenceTables,
      List<Table> outputTables,
      List<NamedPartition> outputPartitions,
      Table inputTable,
      NamedPartition inputPartition) {
    this.id = id;
    this.createTime = createTime;
    this.commandType = commandType;
    this.command = command;
    this.referenceTables = referenceTables;
    this.outputDirectories = outputDirectories;
    this.outputTables = outputTables;
    this.outputPartitions = outputPartitions;
    this.inputTable = inputTable;
    this.inputPartition = inputPartition;
  }

  public long getId() {
    return id;
  }

  public Timestamp getCreateTime() {
    return createTime;
  }

  public HiveOperation getCommandType() {
    return commandType;
  }

  @Override
  public String toString() {

    List<String> outputTableStrings = new ArrayList<>();
    for (Table table : outputTables) {
      outputTableStrings.add(new HiveObjectSpec(table).toString());
    }
    List<String> outputPartitionStrings = new ArrayList<>();
    for (NamedPartition pwn : outputPartitions) {
      outputPartitionStrings.add(new HiveObjectSpec(pwn).toString());
    }

    List<String> referenceTableStrings = new ArrayList<>();
    for (Table t : referenceTables) {
      referenceTableStrings.add(new HiveObjectSpec(t).toString());
    }
    return "AuditLogEntry{" + "id=" + id + ", createTime=" + createTime + ", commandType="
        + commandType + ", outputDirectories=" + outputDirectories + ", referenceTables="
        + referenceTableStrings + ", outputTables=" + outputTableStrings + ", outputPartitions="
        + outputPartitionStrings + ", inputTable=" + inputTable + ", inputPartition="
        + inputPartition + '}';
  }

  public List<String> getOutputDirectories() {
    return outputDirectories;
  }

  public List<Table> getOutputTables() {
    return outputTables;
  }

  public List<NamedPartition> getOutputPartitions() {
    return outputPartitions;
  }

  public List<Table> getReferenceTables() {
    return referenceTables;
  }

  public Table getInputTable() {
    return inputTable;
  }

  public NamedPartition getInputPartition() {
    return inputPartition;
  }

  public String getCommand() {
    return command;
  }
}
