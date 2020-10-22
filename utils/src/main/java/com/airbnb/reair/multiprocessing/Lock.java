package com.airbnb.reair.multiprocessing;

/**
 * Represents a lock that jobs need to get before running. Shared locks are locks where multiple
 * jobs can acquire them where as exclusive locks can only be acquired by one job.
 */
public class Lock {

  public enum Type {
    SHARED, EXCLUSIVE
  }

  private Type type;
  private String name;

  public Lock(Type type, String name) {
    this.type = type;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("<Lock type: %s name: %s>", type, name);
  }
}
