package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ContainerEvent extends AbstractEvent<ContainerEventType> {

  private final SimulatedContainer container;

  public ContainerEvent(ContainerEventType simulationEventType, SimulatedContainer container) {
    super(simulationEventType);
    this.container = container;
  }

  public SimulatedContainer getContainer() {
    return container;
  }

}
