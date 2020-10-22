package com.airbnb.reair.incremental;

import com.airbnb.reair.incremental.db.PersistedJobInfo;
import com.airbnb.reair.incremental.thrift.TReplicationJob;
import com.airbnb.reair.incremental.thrift.TReplicationOperation;
import com.airbnb.reair.incremental.thrift.TReplicationStatus;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

public class ThriftObjectUtils {
  private static TReplicationOperation convert(ReplicationOperation op) {
    switch (op) {
      case COPY_UNPARTITIONED_TABLE:
        return TReplicationOperation.COPY_UNPARTITIONED_TABLE;
      case COPY_PARTITIONED_TABLE:
        return TReplicationOperation.COPY_PARTITIONED_TABLE;
      case COPY_PARTITION:
        return TReplicationOperation.COPY_PARTITION;
      case COPY_PARTITIONS:
        return TReplicationOperation.COPY_PARTITIONS;
      case DROP_TABLE:
        return TReplicationOperation.DROP_TABLE;
      case DROP_PARTITION:
        return TReplicationOperation.DROP_PARTITION;
      case RENAME_TABLE:
        return TReplicationOperation.RENAME_TABLE;
      case RENAME_PARTITION:
        return TReplicationOperation.RENAME_PARTITION;
      default:
        throw new UnsupportedOperationException("Unhandled operation: " + op);
    }
  }

  private static TReplicationStatus convert(ReplicationStatus status) {
    switch (status) {
      case PENDING:
        return TReplicationStatus.PENDING;
      case RUNNING:
        return TReplicationStatus.RUNNING;
      case SUCCESSFUL:
        return TReplicationStatus.SUCCESSFUL;
      case FAILED:
        return TReplicationStatus.FAILED;
      case NOT_COMPLETABLE:
        return TReplicationStatus.NOT_COMPLETABLE;
      default:
        throw new RuntimeException("Unhandled case: " + status);
    }
  }

  /**
   * Convert a ReplicationJob to the Thrift equivalent.
   *
   * @param job the job to convert
   * @return the corresponding Thrift replication job
   */
  public static TReplicationJob convert(ReplicationJob job) {
    PersistedJobInfo jobInfo = job.getPersistedJobInfo();
    List<Long> parentJobIds = new ArrayList<>(job.getParentJobIds());

    return new TReplicationJob(job.getId(), job.getCreateTime(),
        // TODO: Maybe fetch the update time from the DB?
        0,
        convert(jobInfo.getOperation()), convert(jobInfo.getStatus()),
        jobInfo.getSrcPath() == null ? null : jobInfo.getSrcPath().toString(),
        jobInfo.getSrcClusterName(), jobInfo.getSrcDbName(), jobInfo.getSrcTableName(),
        jobInfo.getSrcPartitionNames() == null ? new ArrayList<>() : jobInfo.getSrcPartitionNames(),
        jobInfo.getSrcObjectTldt().orElse(null), jobInfo.getRenameToDb().orElse(null),
        jobInfo.getRenameToTable().orElse(null),
        jobInfo.getRenameToPath().map(Path::toString).orElse(null), jobInfo.getExtras(),
        parentJobIds);



  }
}
