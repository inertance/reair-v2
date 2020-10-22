package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveParameterKeys;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.multiprocessing.Lock;
import com.airbnb.reair.multiprocessing.LockSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.Optional;

/**
 * Task that drops a partition. The expected modified time for the partition is used like a hash to
 * ensure that the right partition is dropped.
 */
public class DropPartitionTask implements ReplicationTask {
  private static final Log LOG = LogFactory.getLog(DropPartitionTask.class);

  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<String> srcTldt;

  /**
   * Constructor for a task that drops a partition.
   *
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param spec specification for the Hive table to drop
   * @param srcTldt The expected modified time for the table to drop. This should be the
   *                transient_lastDdlTime value in the parameters field of the Thrift object. If the
   *                time does not match, the task will not drop the table.
   */
  public DropPartitionTask(
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<String> srcTldt) {
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.srcTldt = srcTldt;
    this.spec = spec;
  }

  @Override
  public RunInfo runTask() throws HiveMetastoreException {
    LOG.debug("Looking to drop: " + spec);
    LOG.debug("Source object TLDT is: " + srcTldt);

    if (!srcTldt.isPresent()) {
      LOG.error("For safety, failing drop job since source object " + " TLDT is missing!");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }
    String expectedTldt = srcTldt.get();

    HiveMetastoreClient ms = destCluster.getMetastoreClient();
    Partition destPartition =
        ms.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());

    if (destPartition == null) {
      LOG.warn("Missing " + spec + " on destination, so can't drop!");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    LOG.debug("Destination object is: " + destPartition);
    String destTldt = destPartition.getParameters().get(HiveParameterKeys.TLDT);

    if (expectedTldt.equals(destTldt)) {
      LOG.debug(String.format("Destination partition %s matches expected" + " TLDT (%s)", spec,
          destTldt));
      LOG.debug("Dropping " + spec);
      ms.dropPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName(), true);
      LOG.debug("Dropped " + spec);
      return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);
    } else {
      LOG.debug(
          String.format("Not dropping %s as source(%s) and " + "destination(%s) TLDT's don't match",
              spec.toString(), srcTldt, destTldt));
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.SHARED, spec.getTableSpec().toString()));
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
