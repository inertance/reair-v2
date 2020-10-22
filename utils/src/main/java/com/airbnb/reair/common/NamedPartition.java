package com.airbnb.reair.common;

import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Composite class that combines the Hive Partition thrift object with the associated name.
 */
public class NamedPartition {
  private String name;
  private Partition partition;

  public NamedPartition(String name, Partition partition) {
    this.name = name;
    this.partition = partition;
  }

  public String getName() {
    return name;
  }

  public Partition getPartition() {
    return partition;
  }

  /**
   * Convert a collection of NamedPartitions to a list of Partitions.
   *
   * @param collection collection of partitions to convert
   * @return a list of converted partitions
   */
  public static List<Partition> toPartitions(Collection<NamedPartition> collection) {
    List<Partition> partitions = new ArrayList<>();
    for (NamedPartition pwn : collection) {
      partitions.add(pwn.getPartition());
    }
    return partitions;
  }

  /**
   * Convert a list of NamedPartitions to partition names.
   *
   * @param collection collection of partitions to convert
   * @return a list of partitio names
   */
  public static List<String> toNames(Collection<NamedPartition> collection) {
    List<String> partitionNames = new ArrayList<>();
    for (NamedPartition pwn : collection) {
      partitionNames.add(pwn.getName());
    }
    return partitionNames;
  }
}
