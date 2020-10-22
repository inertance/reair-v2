package com.airbnb.reair.incremental;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Periodically prints stats for the replication process to the log.
 */
public class StatsTracker {

  private static final Log LOG = LogFactory.getLog(StatsTracker.class);

  // By default print the stats every 10 seconds
  private static final long PRINT_TIME_INTERVAL = 10 * 1000;

  private ReplicationJobRegistry jobRegistry;
  private Timer timer = new Timer(true);
  private volatile long lastCalculatedLag = 0;

  /**
   * Constructor for a stats tracker.
   *
   * @param jobRegistry the job registry to query
   */
  public StatsTracker(ReplicationJobRegistry jobRegistry) {
    this.jobRegistry = jobRegistry;
  }

  /**
   * Start printing out stats about the oldest entry.
   */
  public void start() {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        ReplicationJob jobWithSmallestId = jobRegistry.getJobWithSmallestId();
        if (jobWithSmallestId == null) {
          LOG.debug("Oldest ID: N/A Age: 0");
          lastCalculatedLag = 0;
        } else {
          long currentTime = System.currentTimeMillis();
          long createTime = jobWithSmallestId.getCreateTime();
          String age = "N/A";
          if (createTime != 0) {
            age = String.format("%.2f hrs", (currentTime - createTime) / 3600.0 / 1000.0);
            lastCalculatedLag = currentTime - createTime;
          } else {
            lastCalculatedLag = 0;
          }
          LOG.debug(String.format("Oldest ID: %s Age: %s", jobWithSmallestId.getId(), age));
        }
      }
    }, 0, PRINT_TIME_INTERVAL);
  }

  /**
   * Get the most recently calculated lag. The lag is defined as the time difference between now and
   * the old entry that is not done.
   *
   * @return the calculated lag.
   */
  public long getLastCalculatedLag() {
    return lastCalculatedLag;
  }
}
