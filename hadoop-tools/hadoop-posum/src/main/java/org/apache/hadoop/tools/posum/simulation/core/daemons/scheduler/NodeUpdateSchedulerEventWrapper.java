package org.apache.hadoop.tools.posum.simulation.core.daemons.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

@Private
@Unstable
public class NodeUpdateSchedulerEventWrapper extends NodeUpdateSchedulerEvent {

  public NodeUpdateSchedulerEventWrapper(NodeUpdateSchedulerEvent event) {
    super(new RMNodeWrapper(event.getRMNode()));
  }

}
