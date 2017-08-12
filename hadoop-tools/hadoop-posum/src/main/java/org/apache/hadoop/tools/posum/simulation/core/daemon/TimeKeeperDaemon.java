package org.apache.hadoop.tools.posum.simulation.core.daemon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;

import javax.annotation.Nonnull;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class TimeKeeperDaemon implements Daemon {

  private static final Log LOG = LogFactory.getLog(TimeKeeperDaemon.class);

  private final DaemonQueue queue;
  private final SimulationContext simulationContext;


  private volatile boolean stopped = false;

  public TimeKeeperDaemon(SimulationContext simulationContext) {
    this.simulationContext = simulationContext;
    queue = simulationContext.getDaemonQueue();
    queue.markUntracked(this);
  }

  @Override
  public void run() {
    try {
      long nextExpiration = getNextExpiration();
      if (nextExpiration > 0) {
        waitForRunningDaemons();
        waitForSchedulerFinished();
        simulationContext.setCurrentTime(simulationContext.getCurrentTime() + nextExpiration);
      }
      if (!stopped) {
        queue.enqueue(this);
      }
    } catch (InterruptedException e) {
      if (!stopped)
        LOG.error("Time keeper was interrupted", e);
    }
  }

  private void waitForSchedulerFinished() throws InterruptedException {
    synchronized (simulationContext) {
      while (simulationContext.isAwaitingScheduler()) {
        simulationContext.wait();
      }
    }
  }

  private void waitForRunningDaemons() throws InterruptedException {
    // wait for currently running daemons to finish current step
    queue.awaitEmpty();
  }

  private long getNextExpiration() {
    Delayed first = queue.peek();
    return first == null ? 0 : first.getDelay(TimeUnit.MILLISECONDS);
  }

  public void stop() {
    stopped = true;
  }

  @Override
  public long getDelay(@Nonnull TimeUnit unit) {
    return 0;
  }

  @Override
  public int compareTo(@Nonnull Delayed o) {
    return -(int) o.getDelay(TimeUnit.MILLISECONDS);
  }

}
