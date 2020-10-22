package com.airbnb.reair.multiprocessing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Given jobs (that need to get specific locks before running), this class helps create and manage a
 * DAG of jobs. For example, job 1 needs lock a, job 2 needs lock b and job 3 needs lock a and lock
 * b. The DAG would look like:
 *
 * <p>(job 1, job 2) -> job 3
 *
 * <p>job 1 and job 2 would run in parallel, and once those 2 are done, job 3 can run.
 */
public class JobDagManager {

  private static final Log LOG = LogFactory.getLog(JobDagManager.class);

  // Maps from a lock to the jobs holding the lock
  Map<String, HashSet<Job>> sharedLocksHeld = new HashMap<>();

  Map<String, Job> exclusiveLocksHeld = new HashMap<>();

  // A map of a lock to the jobs needing the resource, in the order that they
  // were submitted.
  Map<String, ArrayList<Job>> lockToJobsNeedingLock = new HashMap<>();

  Set<Job> jobsWithAllRequiredLocks = new HashSet<>();

  private boolean canGetAllLocks(Job job) {
    LockSet lockSet = job.getRequiredLocks();

    for (String exclusiveLock : lockSet.getExclusiveLocks()) {
      if (exclusiveLocksHeld.containsKey(exclusiveLock)) {
        return false;
      }
      if (lockToJobsNeedingLock.containsKey(exclusiveLock)) {
        for (Job j : lockToJobsNeedingLock.get(exclusiveLock)) {
          if (j.getRequiredLocks().contains(exclusiveLock)) {
            return false;
          }
        }
      }
    }

    for (String sharedLock : lockSet.getSharedLocks()) {
      if (exclusiveLocksHeld.containsKey(sharedLock)) {
        return false;
      }

      if (lockToJobsNeedingLock.containsKey(sharedLock)) {
        for (Job j : lockToJobsNeedingLock.get(sharedLock)) {
          if (j.getRequiredLocks().getExclusiveLocks().contains(sharedLock)) {
            return false;
          }

        }
      }
    }

    return true;
  }

  /**
   * To keep track of what jobs need what lock, update the internal data structures to indicate that
   * the supplied job needs the specified lock before running.
   *
   * @param lock the lock that the job needs
   * @param job the job that has the lock requirement
   */
  private void addLockToJobsNeedingLock(String lock, Job job) {
    ArrayList<Job> jobs = lockToJobsNeedingLock.get(lock);
    if (jobs == null) {
      jobs = new ArrayList<>();
      lockToJobsNeedingLock.put(lock, jobs);
    }
    jobs.add(job);
  }

  /**
   * Mark the lock as not being needed by the specified job in the internal data structures.
   *
   * @param lock the lock that is no longer needed
   * @param job the job that no longer needs the lock
   * @param shouldBeAtHead Whether the job should have been at the head of the queue for the log.
   *                       Note that the job at the head of queue represents the job that has the
   *                       lock. This is used as a sanity check only.
   */
  private void removeLockToJobsNeedingLock(String lock, Job job, boolean shouldBeAtHead) {
    ArrayList<Job> jobs = lockToJobsNeedingLock.get(lock);
    if (shouldBeAtHead && jobs.get(0) != job) {
      throw new RuntimeException("Tried to remove " + job + " but it "
          + "wasn't at the head of the list for lock " + lock + "! List is: " + jobs);
    }
    boolean removed = jobs.remove(job);
    if (!removed) {
      throw new RuntimeException("Didn't remove job " + job + " from list " + jobs);
    }
  }

  private void grantExclusiveLock(String lock, Job job) {
    if (exclusiveLocksHeld.containsKey(lock)) {
      throw new RuntimeException("Tried to give exclusive lock to " + job + " when it was held by "
          + exclusiveLocksHeld.get(lock));
    }
    exclusiveLocksHeld.put(lock, job);
  }

  private void grantSharedLock(String lock, Job job) {
    if (exclusiveLocksHeld.containsKey(lock)) {
      throw new RuntimeException("Tried to give shared lock " + lock + " to " + job
          + " when an exclusive lock was held by " + exclusiveLocksHeld.get(lock));
    }

    HashSet<Job> jobsHoldingSharedLock = sharedLocksHeld.get(lock);
    if (jobsHoldingSharedLock == null) {
      jobsHoldingSharedLock = new HashSet<>();
      sharedLocksHeld.put(lock, jobsHoldingSharedLock);
    }
    jobsHoldingSharedLock.add(job);
  }

  /**
   * Add the job to run.
   *
   * @param jobToAdd the job to add
   * @return true if the job that was added can be run immediately.
   */
  public synchronized boolean addJob(Job jobToAdd) {
    LOG.debug("Adding job " + jobToAdd + " requiring locks " + jobToAdd.getRequiredLocks());

    LockSet lockSet = jobToAdd.getRequiredLocks();

    // See if it can get all the locks
    if (canGetAllLocks(jobToAdd)) {
      // It can get the locks
      for (String exclusiveLock : lockSet.getExclusiveLocks()) {
        grantExclusiveLock(exclusiveLock, jobToAdd);
        addLockToJobsNeedingLock(exclusiveLock, jobToAdd);
      }
      for (String sharedLock : lockSet.getSharedLocks()) {
        grantSharedLock(sharedLock, jobToAdd);
        addLockToJobsNeedingLock(sharedLock, jobToAdd);
      }
      jobsWithAllRequiredLocks.add(jobToAdd);
      return true;
    }

    // Otherwise, it can't get all the locks. Find all the jobs that it
    // depends on. Parents is a set of jobs that need to complete before
    // this job can run.
    Set<Job> parents = new HashSet<>();
    // If this job needs an exclusive lock, it needs to wait for the last
    // jobs to require the same shared lock, or the job that last required
    // the exclusive lock.
    for (String exclusiveLockToGet : lockSet.getExclusiveLocks()) {
      // LOG.debug("Lock " + lockToGet + " is needed by " +
      // lockToJobsNeedingLock.get(lockToGet));
      if (!lockToJobsNeedingLock.containsKey(exclusiveLockToGet)) {
        // No need to do anything if no job is waiting for it
        continue;
      }

      List<Job> jobs = lockToJobsNeedingLock.get(exclusiveLockToGet);
      for (int i = jobs.size() - 1; i >= 0; i--) {
        Job otherJob = jobs.get(i);
        // The job that needs the same named lock could need it as
        // a shared lock or an exclusive lock
        Lock.Type requiredLockType = otherJob.getRequiredLocks().getType(exclusiveLockToGet);
        if (requiredLockType == Lock.Type.EXCLUSIVE) {
          parents.add(otherJob);
          break;
        } else if (requiredLockType == Lock.Type.SHARED) {
          parents.add(otherJob);
          // Don't break as it should depend on all the jobs needing
          // the same shared lock
        }
      }

    }

    for (String lockToGet : lockSet.getSharedLocks()) {
      if (!lockToJobsNeedingLock.containsKey(lockToGet)) {
        continue;
      }
      List<Job> jobs = lockToJobsNeedingLock.get(lockToGet);
      for (int i = jobs.size() - 1; i >= 0; i--) {
        Job otherJob = jobs.get(i);
        Lock.Type requiredLockType = otherJob.getRequiredLocks().getType(lockToGet);
        // A shared lock doesn't depend on other shared locks
        if (requiredLockType == Lock.Type.EXCLUSIVE) {
          parents.add(otherJob);
          break;
        }
      }
    }

    if (parents.size() == 0) {
      throw new RuntimeException("Shouldn't happen!");
    }

    // Now that you know the parents of the job to add, setup all parent
    // child relationships
    for (Job parent : parents) {
      jobToAdd.addParent(parent);
      parent.addChild(jobToAdd);
    }

    // Record all the locks that it needs
    for (String lock : lockSet.getExclusiveLocks()) {
      addLockToJobsNeedingLock(lock, jobToAdd);
    }
    for (String lock : lockSet.getSharedLocks()) {
      addLockToJobsNeedingLock(lock, jobToAdd);
    }

    LOG.debug("Added job " + jobToAdd + " with parents " + parents);
    return false;
  }

  private boolean hasExclusiveLock(String exclusiveLock, Job job) {
    return exclusiveLocksHeld.containsKey(exclusiveLock)
        && exclusiveLocksHeld.get(exclusiveLock) == job;
  }

  private boolean hasSharedLock(String sharedLock, Job job) {
    return sharedLocksHeld.containsKey(sharedLock) && sharedLocksHeld.get(sharedLock).contains(job);
  }

  private void removeExclusiveLock(String exclusiveLock, Job job) {
    if (!hasExclusiveLock(exclusiveLock, job)) {
      throw new RuntimeException("Job " + job + " was supposed to " + "have exclusive lock "
          + exclusiveLock + " but it didn't!");
    }
    Job removedJob = exclusiveLocksHeld.remove(exclusiveLock);
    if (removedJob != job) {
      throw new RuntimeException("Shouldn't happen!");
    }
  }

  private void removeSharedLock(String sharedLock, Job job) {
    if (!hasSharedLock(sharedLock, job)) {
      throw new RuntimeException("Job " + job + " was supposed to " + "have shared lock "
          + sharedLock + " but it didn't!");
    }
    Set<Job> jobsWithSharedLock = sharedLocksHeld.get(sharedLock);
    boolean removed = jobsWithSharedLock.remove(job);
    if (!removed) {
      throw new RuntimeException("Shouldn't happen!");
    }
    if (jobsWithSharedLock.size() == 0) {
      Set<Job> removedSet = sharedLocksHeld.remove(sharedLock);
      if (removedSet != jobsWithSharedLock) {
        throw new RuntimeException("Shouldn't happen!");
      }
    }
  }

  /**
   *
   * @param job The job to remove from the DAG
   * @return A set of jobs that can now run since the specified job was removed.
   */
  public synchronized Set<Job> removeJob(Job job) {
    if (!jobsWithAllRequiredLocks.contains(job)) {
      throw new RuntimeException("Trying to remove job without " + "having all the locks");
    }

    LockSet lockSet = job.getRequiredLocks();

    // Free up locks
    for (String exclusiveLock : lockSet.getExclusiveLocks()) {
      removeExclusiveLock(exclusiveLock, job);
      removeLockToJobsNeedingLock(exclusiveLock, job, true);
    }
    for (String sharedLock : lockSet.getSharedLocks()) {
      removeSharedLock(sharedLock, job);
      removeLockToJobsNeedingLock(sharedLock, job, false);
    }

    // Make a copy since we'll be removing from it
    Set<Job> childJobs = new HashSet<>(job.getChildJobs());
    // Remove self as a parent of the children
    for (Job child : childJobs) {
      child.removeParentJob(job);
    }
    // Remove children from self
    for (Job child : childJobs) {
      job.removeChildJob(child);
    }

    // If any of the child jobs have no parent jobs, that means they should
    // be run.
    Set<Job> newJobsWithRequiredLocks = new HashSet<>();
    for (Job child : childJobs) {
      LOG.debug("Job " + child + " has parents " + child.getParentJobs());
      if (child.getParentJobs().size() == 0) {
        LockSet childLockSet = child.getRequiredLocks();
        // Job is ready to run
        for (String lock : childLockSet.getExclusiveLocks()) {
          grantExclusiveLock(lock, child);
        }
        for (String lock : childLockSet.getSharedLocks()) {
          grantSharedLock(lock, child);
        }
        jobsWithAllRequiredLocks.add(child);
        newJobsWithRequiredLocks.add(child);
      }
    }
    return newJobsWithRequiredLocks;
  }
}
