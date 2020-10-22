package com.airbnb.reair.multiprocessing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Executes a job in a thread. The job is required to return a return code of 0 or else an exception
 * will be thrown.
 */
public class Worker<T extends Job> extends Thread {

  private static final Log LOG = LogFactory.getLog(Worker.class);

  private static int nextWorkerId = 0;

  private int workerId;
  private BlockingQueue<T> inputQueue;
  private ParallelJobExecutor parallelJobExecutor;
  private Job job = null;

  /**
   * Constructor for a worker that gets and runs jobs from the input queue.
   *
   * @param inputQueue the queue to get jobs from
   * @param parallelJobExecutor the executor to notify when the job is done
   */
  public Worker(BlockingQueue<T> inputQueue, ParallelJobExecutor parallelJobExecutor) {
    this.inputQueue = inputQueue;
    this.workerId = nextWorkerId++;
    this.parallelJobExecutor = parallelJobExecutor;
    setName(Worker.class.getSimpleName() + "-" + workerId);
    setDaemon(true);
  }

  /**
   * Constructor for a worker that gets and runs jobs from the input queue with the option to
   * specify a prefix for the worker thread name.
   *
   * @param workerNamePrefix prefix for the thread name
   * @param inputQueue the queue to get jobs from
   * @param parallelJobExecutor the executor to notify when the job is done
   */
  public Worker(
      String workerNamePrefix,
      BlockingQueue<T> inputQueue,
      ParallelJobExecutor parallelJobExecutor) {
    this.inputQueue = inputQueue;
    this.workerId = nextWorkerId++;
    this.parallelJobExecutor = parallelJobExecutor;
    setName(workerNamePrefix + "-" + workerId);
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (true) {
        if (job == null) {
          LOG.debug("Waiting for a job");
          job = inputQueue.take();
        } else {
          LOG.debug("Using existing job");
        }
        LOG.debug("**** Running job: " + job + " ****");
        int ret = job.run();
        if (ret != 0) {
          LOG.error("Error running job " + job + " return code: " + ret);
          throw new RuntimeException(String.format("Job %s returned %s", job, ret));
        }
        LOG.debug("**** Done running job: " + job + " ****");
        parallelJobExecutor.notifyDone(job);
        job = null;
      }
    } catch (InterruptedException e) {
      LOG.debug("Got interrupted");
    } // Any other exception should cause the process to exit via uncaught exception handler
  }

  public Job getJob() {
    return job;
  }
}
