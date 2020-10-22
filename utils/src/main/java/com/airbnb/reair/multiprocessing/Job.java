package com.airbnb.reair.multiprocessing;

import java.util.HashSet;
import java.util.Set;

/**
 * A Job is anything that needs to run, along with a set of pre-requisites. In this case, the
 * prerequisites are represented as a set of shared/exclusive locks.
 */
public abstract class Job {

  /**
   * Before the Job runs, it needs to acquire a set of shared or exclusive locks. Multiple jobs can
   * have the same shared lock, but only one job can have an exclusive one.
   */
  public enum LockType {
    SHARED, EXCLUSIVE
  }

  // A list of jobs in progress that need to finish before this job can run.
  private Set<Job> parentJobs = new HashSet<>();
  // A set of jobs that are waiting for this job to finish before running
  private Set<Job> childJobs = new HashSet<>();

  // Method that gets called when this job should run
  public abstract int run();

  // // A set of locks that the job needs to get before running
  // abstract public Set<String> getRequiredExclusiveLocks();
  // // A set of shared locks that the job needs to get before running
  // abstract public Set<String> getRequiredSharedLocks();

  /**
   * Add the specified job as a parent job.
   *
   * @param parentJob the parent job
   */
  public void addParent(Job parentJob) {
    parentJobs.add(parentJob);
  }

  /**
   * Add the specified job as a child job.
   *
   * @param childJob the child job
   */
  public void addChild(Job childJob) {
    childJobs.add(childJob);
  }

  /**
   * @return a set of Jobs that need to finish before this job can run.
   */
  public Set<Job> getParentJobs() {
    return parentJobs;
  }

  /**
   * @return a set of jobs that require this job to finish before it runs.
   */
  public Set<Job> getChildJobs() {
    return childJobs;
  }

  /**
   * Removes a parent job from this job's set of parent jobs. This should be called when the parent
   * job has finished running.
   *
   * @param parentJob the parent job
   */
  public void removeParentJob(Job parentJob) {
    if (!parentJobs.contains(parentJob)) {
      throw new RuntimeException("Tried to remove job " + parentJob + " when it wasn't a parent");
    }
    boolean removed = parentJobs.remove(parentJob);
    if (!removed) {
      throw new RuntimeException("Shouldn't happen!");
    }
  }

  /**
   * Removes a child job from this job's set of child jobs. This should be called when the this job
   * has finished running and is being removed from the DAG.
   *
   * @param childJob the child job
   */
  public void removeChildJob(Job childJob) {
    if (!childJobs.contains(childJob)) {
      throw new RuntimeException("Tried to remove job " + childJob + " when it wasn't a child");
    }
    boolean removed = childJobs.remove(childJob);
    if (!removed) {
      throw new RuntimeException("Shouldn't happen!");
    }
  }

  /**
   * To handle concurrency issues, jobs should specify a set of locks so that two conflicting jobs
   * do not run at the same time.
   *
   * @return a set of locks that this job should acquire before running
   */
  public abstract LockSet getRequiredLocks();
}
