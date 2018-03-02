package org.apache.hadoop.tools.posum.simulation.util;

import org.apache.hadoop.tools.posum.simulation.core.nodemanager.NodeInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NMCore {

  private MasterKey masterKey;
  private RMNode node;
  private ResourceManager rm;
  private int RESPONSE_ID = 1;
  private NodeHeartbeatRequest nextRequest;

  public NMCore(ResourceManager rm, String rack, String hostname, int memory, int cores) {
    this.node = NodeInfo.newNodeInfo(rack, hostname, BuilderUtils.newResource(memory, cores));
    this.rm = rm;
  }

  public void registerWithRM() throws IOException, YarnException {
    RegisterNodeManagerRequest req =
      Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);
    RegisterNodeManagerResponse response = null;
    response = rm.getResourceTrackerService().registerNodeManager(req);
    masterKey = response.getNMTokenMasterKey();
  }

  public NodeId getNodeId() {
    return node.getNodeID();
  }

  public void prepareHeartBeat(List<ContainerStatus> containerStatusList) {
    nextRequest = Records.newRecord(NodeHeartbeatRequest.class);
    nextRequest.setLastKnownNMTokenMasterKey(masterKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);
    ns.setContainersStatuses(containerStatusList);
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID++);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));
    nextRequest.setNodeStatus(ns);
  }

  public NodeHeartbeatResponse sendHeartBeat() throws IOException, YarnException {
    return rm.getResourceTrackerService().nodeHeartbeat(nextRequest);
  }

  public String getHostName(){
    return node.getHostName();
  }

  public String getRackName(){
    return node.getRackName();
  }
}
