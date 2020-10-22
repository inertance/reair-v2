package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveParameterKeys;
import com.airbnb.reair.incremental.ReplicationUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Optional;

/**
 * Handles cases when there is an existing table or partition on the destination cluster. This class
 * can later be configured to be user-defined.
 */
public class ObjectConflictHandler implements Configurable {

  private static final Log LOG = LogFactory.getLog(ObjectConflictHandler.class);

  private Optional<Configuration> conf;

  public void setConf(Configuration conf) {
    this.conf = Optional.ofNullable(conf);
  }

  public Configuration getConf() {
    return conf.orElse(null);
  }

  /**
   * Handle a conflict on the destination cluster when a table with the same DB and name already
   * exists. If the conflict was successfully handled so that the caller can go ahead with
   * copying/overwriting the table, this will return true.
   *
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param srcTable source table
   * @param existingDestTable the Hive Thift table object corresponding to the conflicting object on
   *                          the destination cluster
   * @return whether or not the conflict was resolved and the table can be copied
   *
   * @throws HiveMetastoreException if there an error making a metastore call
   */
  public boolean handleCopyConflict(
      Cluster srcCluster,
      Cluster destCluster,
      Table srcTable,
      Table existingDestTable) throws HiveMetastoreException {
    HiveObjectSpec spec =
        new HiveObjectSpec(existingDestTable.getDbName(), existingDestTable.getTableName());

    if (existingDestTable.getParameters().get(HiveParameterKeys.SRC_CLUSTER) != null
        && !existingDestTable.getParameters().get(HiveParameterKeys.SRC_CLUSTER)
        .equals(srcCluster.getName())) {
      LOG.warn("Table " + spec + " exists on destination, and it's "
          + "missing tags that indicate that it was replicated.");
      // This might indicate that someone created a table with the same
      // name on the destination cluster. Instead of dropping and
      // overwriting, a rename can be done here to save the table with a
      // *_conflict name.
    }

    // If the partitioning keys don't match, then it will have to be
    // dropped.
    if (!srcTable.getPartitionKeys().equals(existingDestTable.getPartitionKeys())) {
      // Table exists on destination, but it's partitioned. It'll have to
      // be dropped since Hive doesn't support changing of partition
      // columns. Instead of dropping the table, the table on the dest
      // cluster could be renamed to something else for further
      // inspection.
      LOG.warn(String.format(
          "For %s, there is a mismatch in the " + "partitioning keys. src: %s dest: %s", spec,
          srcTable.getPartitionKeys(), existingDestTable.getPartitionKeys()));

      boolean dropData = !locationOnS3(existingDestTable.getSd());
      LOG.warn("Not dropping data at location " + ReplicationUtils.getLocation(existingDestTable));
      HiveMetastoreClient destMs = destCluster.getMetastoreClient();

      LOG.debug(String.format("Dropping %s on destination (delete " + "data: %s)", spec, dropData));
      destMs.dropTable(spec.getDbName(), spec.getTableName(), dropData);
      LOG.debug("Dropped " + spec);
    }

    return true;
  }

  /**
   * Handle a conflict on the destination cluster when a table with the same DB and name exists
   * already. If the conflict was successfully handled so that the caller can go ahead with
   * copying the table, this will return true.
   *
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param srcPartition source partition
   * @param existingDestPartition the Hive Thift table partition corresponding to the conflicting
   *                              object on the destination cluster
   * @return whether or not conflict was resolved and the partition can be copied
   *
   * @throws HiveMetastoreException if there an error making a metastore call
   */
  public boolean handleCopyConflict(
      Cluster srcCluster,
      Cluster destCluster,
      Partition srcPartition,
      Partition existingDestPartition) {
    // Partitions can be usually overwritten without issues
    return true;
  }

  private boolean locationOnS3(StorageDescriptor sd) {
    String location = sd.getLocation();

    return location != null && (location.startsWith("s3n") || location.startsWith("s3a"));
  }
}
