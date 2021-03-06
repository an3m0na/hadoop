package org.apache.hadoop.tools.posum.simulation.core.resourcemanager;

import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.util.SimplifiedResourceManager;

public class SimulationResourceManager<T extends PluginPolicy> extends SimplifiedResourceManager<T> {
  public SimulationResourceManager(SimulationContext<T> simulationContext) {
    super(new SimulationResourceScheduler<>(simulationContext));
  }

  public long getClusterTimestamp() {
    return getClusterTimeStamp();
  }

  public T getPluginPolicy() {
    return getInjectableScheduler().getInjectedScheduler();
  }
}
