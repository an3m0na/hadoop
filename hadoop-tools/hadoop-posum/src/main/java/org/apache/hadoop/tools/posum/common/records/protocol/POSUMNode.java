package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Set;

/**
 * Created by ane on 4/5/16.
 */
public abstract class POSUMNode {
    public static POSUMNode newInstance(RMNode original) {
        POSUMNode node = Records.newRecord(POSUMNode.class);
        node.setNodeID(original.getNodeID());
        node.setHostName(original.getHostName());
        node.setCommandPort(original.getCommandPort());
        node.setHttpPort(original.getHttpPort());
        node.setHealthReport(original.getHealthReport());
        node.setLastHealthReportTime(original.getLastHealthReportTime());
        node.setLastNodeHeartBeatResponse(original.getLastNodeHeartBeatResponse());
        node.setNodeManagerVersion(original.getNodeManagerVersion());
        node.setTotalCapability(original.getTotalCapability());
        node.setRackName(original.getRackName());
        return node;
    }

    //TODO keep only accessors used by the schedulers

    public abstract NodeId getNodeID();

    public abstract String getHostName();

    public abstract int getCommandPort();

    public abstract int getHttpPort();

    public abstract String getHealthReport();

    public abstract long getLastHealthReportTime();

    public abstract  String getNodeManagerVersion();

    public abstract Resource getTotalCapability();

    public abstract String getRackName();

    public abstract List<UpdatedContainerInfo> pullContainerUpdates();

    public abstract void setNodeID(NodeId nodeId);

    public abstract void setHostName(String hostName);

    public abstract void setCommandPort(int commandPort);

    public abstract void setHttpPort(int httpPort);

    public abstract void setHealthReport(String healthReport);

    public abstract void setLastHealthReportTime(long lastHealthReportTime);

    public abstract void setNodeManagerVersion(String nodeManagerVersion);

    public abstract void setTotalCapability(Resource totalCapability);

    public abstract void setRackName(String rackName);

    public abstract void setNode(Node node);

    public abstract void setState(NodeState nodeState);

    public abstract void setContainersToCleanUp(List<ContainerId> containersToCleanUp);

    public abstract void setAppsToCleanup(List<ApplicationId> appsToCleanup);

    public abstract void setLastNodeHeartBeatResponse(NodeHeartbeatResponse nodeHeartBeatResponse);

    public abstract void pushContainerUpdates(List<UpdatedContainerInfo> containerInfos);

    public abstract void setNodeLabels(Set<String> labels);
}
