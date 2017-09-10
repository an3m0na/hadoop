package org.apache.hadoop.tools.posum.simulation.core.resourcemanager;

import org.apache.hadoop.tools.posum.common.util.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

public class SimulationResourceScheduler extends InjectableResourceScheduler {
  private final SimulationContext simulationContext;

  public SimulationResourceScheduler(SimulationContext simulationContext) {
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

