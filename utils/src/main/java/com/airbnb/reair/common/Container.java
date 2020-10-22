package com.airbnb.reair.common;

/**
 * Container that is used to hold other objects to workaround use cases where an object or null
 * needs to be returned.
 *
 * @param <T> the type of the container
 */
public class Container<T> {
  private volatile T item;

  /**
   * Put the item into this container.
   *
   * @param item the item to put in
   */
  public void set(T item) {
    this.item = item;
  }

  /**
   * Get the item that was put into this container.
   * @return the item that was last put in
   */
  public T get() {
    return item;
  }
}
