package org.apache.hadoop.tools.posum.simulation.core.daemon;

import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SIMULATION_RUNNER_POOL_SIZE;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SIMULATION_RUNNER_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.util.ShutdownThreadsHelper.shutdownExecutorService;

public class DaemonPool {
  private final static Logger LOG = Logger.getLogger(DaemonPool.class);

  private final DaemonQueue queue;
  private ThreadPoolExecutor executor;
  private TimeKeeperDaemon timeKeeper;

  @SuppressWarnings("unchecked")
  public DaemonPool(SimulationContext simulationContext) {
    queue = new DaemonQueue();
    simulationContext.setDaemonQueue(queue);

    timeKeeper = new TimeKeeperDaemon(simulationContext);

    int threadPoolSize = simulationContext.getConf().getInt(SIMULATION_RUNNER_POOL_SIZE, SIMULATION_RUNNER_POOL_SIZE_DEFAULT);
    executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.MILLISECONDS, (BlockingQueue) queue);
    executor.prestartAllCoreThreads();
  }

  public void start() {
    queue.enqueue(timeKeeper);
  }

  public void shutDown() {
    try {
      timeKeeper.stop();
      shutdownExecutorService(executor);
    } catch (InterruptedException e) {
      LOG.error("Daemon pool shutdown interrupted", e);
    }
  }

  public void schedule(WorkerDaemon daemon) {
    queue.enqueue(daemon);
  }

}
