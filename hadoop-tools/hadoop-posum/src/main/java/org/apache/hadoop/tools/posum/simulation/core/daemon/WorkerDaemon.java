package org.apache.hadoop.tools.posum.simulation.core.daemon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;

import javax.annotation.Nonnull;
import java.text.MessageFormat;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class WorkerDaemon implements Daemon {
  private static final Log LOG = LogFactory.getLog(WorkerDaemon.class);

  private long nextRun;
  private long startTime;
  private long repeatInterval;
  private static AtomicInteger nextId = new AtomicInteger(0);
  private int id = 0;
  protected final SimulationContext simulationContext;

  public WorkerDaemon(SimulationContext simulationContext) {
    this.simulationContext = simulationContext;
    id = nextId.getAndIncrement();
  }

  public void init(long startTime, long repeatInterval) {
    if (repeatInterval < 0) {
      throw new IllegalArgumentException(MessageFormat.format(
          "repeatInterval[{0}] cannot be less than 1", repeatInterval));
    }
    this.startTime = startTime;
    this.repeatInterval = repeatInterval;
    this.nextRun = startTime;
  }

  public String formatLog(String messageFormat, Object... messageArguments) {
    String prefix = MessageFormat.format("Sim={0} Worker={1} T={2}: ", simulationContext.getSchedulerClass().getSimpleName(), getId(), simulationContext.getCurrentTime());
    return prefix + MessageFormat.format(messageFormat, messageArguments);
  }

  public String getId() {
    return Integer.toString(id);
  }

  public abstract void doFirstStep() throws Exception;

  public abstract void doStep() throws Exception;

  public abstract void cleanUp() throws Exception;

  public abstract boolean isFinished();

  @Override
  public final void run() {
    try {
      if (nextRun == startTime) {
        doFirstStep();
      } else {
        doStep();
      }
      if (isFinished()) {
        cleanUp();
        simulationContext.getDaemonQueue().evict(this);
      } else {
        do {
          nextRun += repeatInterval;
        } while (nextRun <= simulationContext.getCurrentTime());
        simulationContext.getDaemonQueue().enqueue(this);
      }
    } catch (Exception e) {
      throw new PosumException("Error running worker daemon " + this, e);
    }
  }

  @Override
  public long getDelay(@Nonnull TimeUnit unit) {
    return unit.convert(nextRun - simulationContext.getCurrentTime(), TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(@Nonnull Delayed o) {
    return (int) Math.signum(getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WorkerDaemon daemon = (WorkerDaemon) o;

    return id == daemon.id;
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
        "id=" + id +
        ", nextRun=" + nextRun +
        ", startTime=" + startTime +
        ", repeatInterval=" + repeatInterval +
        '}';
  }
}
