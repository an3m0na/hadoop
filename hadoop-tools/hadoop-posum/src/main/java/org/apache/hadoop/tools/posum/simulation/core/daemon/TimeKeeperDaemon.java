package org.apache.hadoop.tools.posum.simulation.core.daemon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.yarn.event.AsyncDispatcher;

import javax.annotation.Nonnull;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class TimeKeeperDaemon implements Daemon {

  private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);

  private final DaemonQueue queue;
  private final SimulationContext simulationContext;


  private volatile boolean stopped = false;

  public TimeKeeperDaemon(SimulationContext simulationContext) {
    this.simulationContext = simulationContext;
    this.queue = simulationContext.getDaemonQueue();
  }

  @Override
  public void run() {
    try {
      long nextExpiration = getNextExpiration();
      if (nextExpiration > 0) {
        waitForRunningDaemons();
        simulationContext.setCurrentTime(simulationContext.getCurrentTime() + nextExpiration);
      }
      if (!stopped) {
        queue.add(this);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
    }
  }

  private void waitForRunningDaemons() throws InterruptedException {
    // wait for currently running daemons to finish current step
    while (queue.countRunning() > 1) {
      synchronized (queue) {
        queue.wait();
      }
    }
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
