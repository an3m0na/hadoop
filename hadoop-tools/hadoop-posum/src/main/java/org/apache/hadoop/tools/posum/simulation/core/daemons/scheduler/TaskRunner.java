package org.apache.hadoop.tools.posum.simulation.core.daemons.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.simulation.core.daemons.DaemonRunner;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Private
@Unstable
public class TaskRunner {
  private final static Logger LOG = Logger.getLogger(TaskRunner.class);

  @Private
  @Unstable
  public abstract static class Task implements Runnable, Delayed {
    private long start;
    private long end;
    private long nextRun;
    private long startTime;
    private long endTime;
    private long repeatInterval;
    private Queue<Task> queue;

    public Task() {
    }

    //values in milliseconds, start/end are milliseconds from now
    public void init(long startTime, long endTime, long repeatInterval) {
      if (endTime - startTime < 0) {
        throw new IllegalArgumentException(MessageFormat.format(
          "endTime[{0}] cannot be smaller than startTime[{1}]", endTime,
          startTime));
      }
      if (repeatInterval < 0) {
        throw new IllegalArgumentException(MessageFormat.format(
          "repeatInterval[{0}] cannot be less than 1", repeatInterval));
      }
      if ((endTime - startTime) % repeatInterval != 0) {
        throw new IllegalArgumentException(MessageFormat.format(
          "Invalid parameters: (endTime[{0}] - startTime[{1}]) " +
            "% repeatInterval[{2}] != 0",
          endTime, startTime, repeatInterval));
      }
      start = startTime;
      end = endTime;
      this.repeatInterval = repeatInterval;
    }

    private void timeRebase(long now) {
      startTime = now + start;
      endTime = now + end;
      this.nextRun = startTime;
    }

    //values in milliseconds, start is milliseconds from now
    //it only executes firstStep()
    public void init(long startTime) {
      init(startTime, startTime, 1);
    }

    private void setQueue(Queue<Task> queue) {
      this.queue = queue;
    }

    @Override
    public final void run() {
      try {
        if (nextRun == startTime) {
          firstStep();
          nextRun += repeatInterval;
          if (nextRun <= endTime) {
            queue.add(this);
          }
        } else if (nextRun < endTime) {
          middleStep();
          nextRun += repeatInterval;
          queue.add(this);
        } else {
          lastStep();
        }
      } catch (Exception e) {
        e.printStackTrace();
        Thread.getDefaultUncaughtExceptionHandler()
          .uncaughtException(Thread.currentThread(), e);
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(nextRun - DaemonRunner.getCurrentTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      if (!(o instanceof Task)) {
        throw new IllegalArgumentException("Parameter must be a Task instance");
      }
      Task other = (Task) o;
      return (int) Math.signum(nextRun - other.nextRun);
    }


    public abstract void firstStep() throws Exception;

    public abstract void middleStep() throws Exception;

    public abstract void lastStep() throws Exception;

    public void setEndTime(long et) {
      endTime = et;
    }
  }

  private DelayQueue queue;
  private int threadPoolSize;
  private ThreadPoolExecutor executor;
  private long startTimeMS = 0;

  public TaskRunner() {
    queue = new DelayQueue();
  }

  public void setQueueSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }

  @SuppressWarnings("unchecked")
  public void start() {
    if (executor != null) {
      throw new IllegalStateException("Already started");
    }
    DelayQueue preStartQueue = queue;

    queue = new DelayQueue();
    executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0,
      TimeUnit.MILLISECONDS, queue);
    executor.prestartAllCoreThreads();

    startTimeMS = DaemonRunner.getCurrentTime();
    for (Object d : preStartQueue) {
      schedule((Task) d, startTimeMS);
    }
  }

  public void stopNow() {
    executor.shutdownNow();

  }

  public void await(CountDownLatch latch) {
    try {
      latch.await();
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Could not shut down TaskRunner", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void schedule(Task task, long timeNow) {
    task.timeRebase(timeNow);
    task.setQueue(queue);
    queue.add(task);
  }

  public void schedule(Task task) {
    schedule(task, DaemonRunner.getCurrentTime());
  }

  public long getStartTimeMS() {
    return this.startTimeMS;
  }
}
