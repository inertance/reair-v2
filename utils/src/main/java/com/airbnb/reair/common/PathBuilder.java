package com.airbnb.reair.common;

import org.apache.hadoop.fs.Path;

/**
 * Helps construct Path objects by allowing incremental element additions. For example, /a/b/c can
 * be formed by starting with /a, then adding b and c.
 */
public class PathBuilder {
  private Path currentPath;

  public PathBuilder(Path currentPath) {
    this.currentPath = currentPath;
  }

  /**
   * Add another path element.
   *
   * @param element the element to add
   * @return a PathBuilder that includes the added element
   */
  public PathBuilder add(String element) {
    currentPath = new Path(currentPath, element);
    return this;
  }

  /**
   * Convert the path elements accumulated so far into a single path.
   *
   * @return a Path that includes all added path elements
   */
  public Path toPath() {
    return currentPath;
  }
}
