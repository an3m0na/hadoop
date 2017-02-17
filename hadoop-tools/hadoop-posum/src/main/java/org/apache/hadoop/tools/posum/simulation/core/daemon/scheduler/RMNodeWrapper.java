package org.apache.hadoop.tools.posum.simulation.core.daemon.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Private
@Unstable
public class RMNodeWrapper implements RMNode {
  private RMNode node;
  private List<UpdatedContainerInfo> updates;
  private boolean pulled = false;

  public RMNodeWrapper(RMNode node) {
    this.node = node;
    updates = node.pullContainerUpdates();
  }

  @Override
  public NodeId getNodeID() {
    return node.getNodeID();
  }

  @Override
  public String getHostName() {
    return node.getHostName();
  }

  @Override
  public int getCommandPort() {
    return node.getCommandPort();
  }

  @Override
  public int getHttpPort() {
    return node.getHttpPort();
  }

  @Override
  public String getNodeAddress() {
    return node.getNodeAddress();
  }

  @Override
  public String getHttpAddress() {
    return node.getHttpAddress();
  }

  @Override
  public String getHealthReport() {
    return node.getHealthReport();
  }

  @Override
  public long getLastHealthReportTime() {
    return node.getLastHealthReportTime();
  }

  @Override
  public Resource getTotalCapability() {
    return node.getTotalCapability();
  }

  @Override
  public String getRackName() {
    return node.getRackName();
  }

  @Override
  public Node getNode() {
    return node.getNode();
  }

  @Override
  public NodeState getState() {
    return node.getState();
  }

  @Override
  public List<ContainerId> getContainersToCleanUp() {
    return node.getContainersToCleanUp();
  }

  @Override
  public List<ApplicationId> getAppsToCleanup() {
    return node.getAppsToCleanup();
  }

  @Override
  public void updateNodeHeartbeatResponseForCleanup(
    NodeHeartbeatResponse nodeHeartbeatResponse) {
    node.updateNodeHeartbeatResponseForCleanup(nodeHeartbeatResponse);
  }

  @Override
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {
    return node.getLastNodeHeartBeatResponse();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> list = Collections.EMPTY_LIST;
    if (!pulled) {
      list = updates;
      pulled = true;
    }
    return list;
  }

  List<UpdatedContainerInfo> getContainerUpdates() {
    return updates;
  }

  @Override
  public String getNodeManagerVersion() {
    return node.getNodeManagerVersion();
  }

  @Override
  public Set<String> getNodeLabels() {
    return RMNodeLabelsManager.EMPTY_STRING_SET;
  }
}
