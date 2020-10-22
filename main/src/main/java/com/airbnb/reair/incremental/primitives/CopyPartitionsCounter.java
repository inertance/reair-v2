package com.airbnb.reair.incremental.primitives;

/**
 * Counts the number of partitions copied by a replication task.
 */
public class CopyPartitionsCounter {
  long completionCount = 0;
  long bytesCopiedCount = 0;

  synchronized void incrementCompletionCount() {
    completionCount++;
  }

  synchronized long getCompletionCount() {
    return completionCount;
  }

  synchronized void incrementBytesCopied(long bytesCopied) {
    bytesCopiedCount += bytesCopied;
  }

  synchronized long getBytesCopied() {
    return bytesCopiedCount;
  }
}
