package org.apache.hadoop.tools.posum.simulation.core.daemon;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DaemonRunner {
  private final static Logger LOG = Logger.getLogger(DaemonRunner.class);


  private final DaemonQueue queue;
  private ThreadPoolExecutor executor;
  private TimeKeeperDaemon timeKeeper;

  @SuppressWarnings("unchecked")
  public DaemonRunner(int threadPoolSize) {
    queue = new DaemonQueue();
    timeKeeper = new TimeKeeperDaemon(queue);
    executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.MILLISECONDS, (BlockingQueue) queue);
    executor.prestartAllCoreThreads();
  }

  public void start() {
    queue.enqueue(timeKeeper);
  }

  public void stopNow() {
    executor.shutdownNow();
  }

  public void await(CountDownLatch latch) {
    try {
      latch.await();
      System.out.println("DaemonRunner shutting down");
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Could not shut down DaemonRunner", e);
    }
  }

  public void schedule(WorkerDaemon daemon) {
    queue.enqueue(daemon);
  }

  public long getCurrentTime() {
    return timeKeeper.getCurrentTime();
  }

  public void forget(WorkerDaemon daemon) {
    queue.evict(daemon);
  }
}
