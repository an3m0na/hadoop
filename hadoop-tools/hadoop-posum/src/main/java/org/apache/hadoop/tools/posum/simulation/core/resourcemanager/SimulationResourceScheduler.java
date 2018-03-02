package org.apache.hadoop.tools.posum.simulation.core.resourcemanager;

import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.util.InjectableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

public class SimulationResourceScheduler<T extends PluginPolicy> extends InjectableResourceScheduler<T> {
  private final SimulationContext simulationContext;

  public SimulationResourceScheduler(SimulationContext<T> simulationContext) {
    super(simulationContext.getSchedulerClass(), simulationContext);
    this.simulationContext = simulationContext;
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    try {
      super.handle(schedulerEvent);

      if (schedulerEvent.getType() == SchedulerEventType.APP_REMOVED
        && schedulerEvent instanceof AppRemovedSchedulerEvent) {
        simulationContext.getRemainingJobsCounter().countDown();
      }
    } finally {
      synchronized (simulationContext) {
        simulationContext.setAwaitingScheduler(false);
        simulationContext.notify();
      }
    }
  }
}

