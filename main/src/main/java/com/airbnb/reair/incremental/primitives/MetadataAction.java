package com.airbnb.reair.incremental.primitives;

/**
 * Enumeration to help simplify logic for running metadata actions in the replication tasks.
 */
public enum MetadataAction {
  // No operation
  NOOP,
  // Create new metadata
  CREATE,
  // Alter existing metadata
  ALTER
}
