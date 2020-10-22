package com.airbnb.reair.multiprocessing;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class LockSet {

  private Set<Lock> allLocks;

  private Set<String> exclusiveLockNames;
  private Set<String> sharedLockNames;

  /**
   * TODO.
   */
  public LockSet() {
    allLocks = new HashSet<>();
    exclusiveLockNames = new HashSet<>();
    sharedLockNames = new HashSet<>();
  }

  /**
   * TODO.
   *
   * @param lock TODO
   */
  public void add(Lock lock) {
    allLocks.add(lock);
    if (lock.getType() == Lock.Type.SHARED) {
      sharedLockNames.add(lock.getName());
    } else if (lock.getType() == Lock.Type.EXCLUSIVE) {
      exclusiveLockNames.add(lock.getName());
    } else {
      throw new RuntimeException("Unhandled lock type " + lock.getType());
    }
  }

  /**
   * TODO.
   *
   * @param lockName TODO
   * @return TODO
   */
  public Lock.Type getType(String lockName) {
    if (sharedLockNames.contains(lockName)) {
      return Lock.Type.SHARED;
    } else if (exclusiveLockNames.contains(lockName)) {
      return Lock.Type.EXCLUSIVE;
    } else {
      throw new RuntimeException("Unknown lock name " + lockName);
    }
  }

  public boolean contains(String lockName) {
    return exclusiveLockNames.contains(lockName) || sharedLockNames.contains(lockName);
  }

  public Set<String> getExclusiveLocks() {
    return Collections.unmodifiableSet(exclusiveLockNames);
  }

  public Set<String> getSharedLocks() {
    return Collections.unmodifiableSet(sharedLockNames);
  }

  @Override
  public String toString() {
    return allLocks.toString();
  }
}
