package com.airbnb.reair.incremental;

/**
 * Handler that fires when replication jobs change states.
 */
public interface OnStateChangeHandler {

  /**
   * Method to run when the job starts.
   *
   * @param replicationJob job that started
   *
   * @throws StateUpdateException if there's an error updating the state
   */
  void onStart(ReplicationJob replicationJob) throws StateUpdateException;

  /**
   * Method to run when the job completes.
   *
   * @param runInfo information about how the job ran
   * @param replicationJob the job that completed
   *
   * @throws StateUpdateException if there's an error updating the state
   */
  void onComplete(RunInfo runInfo, ReplicationJob replicationJob) throws StateUpdateException;
}
