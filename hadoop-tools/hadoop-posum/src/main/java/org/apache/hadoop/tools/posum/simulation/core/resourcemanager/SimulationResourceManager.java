package org.apache.hadoop.tools.posum.simulation.core.resourcemanager;

import org.apache.hadoop.tools.posum.common.util.cluster.SimplifiedResourceManager;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;

public class SimulationResourceManager extends SimplifiedResourceManager {
  public SimulationResourceManager(SimulationContext simulationContext) {
    super(new SimulationResourceScheduler(simulationContext));
  }
}
