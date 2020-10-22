namespace java com.airbnb.reair.incremental.thrift

enum TReplicationOperation {
  COPY_UNPARTITIONED_TABLE,
  COPY_PARTITIONED_TABLE,
  COPY_PARTITION,
  COPY_PARTITIONS,
  DROP_TABLE,
  DROP_PARTITION,
  RENAME_TABLE,
  RENAME_PARTITION,
}

enum TReplicationStatus {
  PENDING,
  RUNNING,
  SUCCESSFUL,
  FAILED,
  NOT_COMPLETABLE,
}

struct TReplicationJob {
  1: i64 id,
  2: i64 createTime,
  3: i64 updateTime,
  4: TReplicationOperation operation,
  5: TReplicationStatus status,
  6: string srcPath,
  7: string srcCluster,
  8: string srcDb,
  9: string srcTable,
  10: list<string> srcPartitions,
  11: string srcModifiedTime,
  12: string renameToDb,
  13: string renameToTable,
  14: string renameToPath,
  15: map<string, string> extras,
  16: list<i64> waitingOnJobs,
}

service TReplicationService {
  // Gets the replication jobs in progress with ID's > afterId and up to
  // maxJobs
  list<TReplicationJob> getActiveJobs(1: i64 afterId, 2: i32 maxJobs);

  // Gets the replication jobs that are retired with ID's > afterId and up to
  // maxJobs
  list<TReplicationJob> getRetiredJobs(1: i64 afterId, 2: i32 maxJobs);

  map<i64, TReplicationJob> getJobs(1: list<i64> ids);

  // Pauses the replication process - stops jobs if necessary
  void pause();

  // Resume the replication process
  void resume();

  // Get the lag for replication process in ms
  i64 getLag();
}
