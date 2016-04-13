package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Set;

/**
 * Only a shell of the RMNode with the methods that are used outside the scheduler
 */
public abstract class POSUMNode implements RMNode {
    public static POSUMNode newInstance(RMNode original) {
        POSUMNode node = Records.newRecord(POSUMNode.class);
        node.setNodeID(original.getNodeID());
        node.setHostName(original.getHostName());
        node.setCommandPort(original.getCommandPort());
        node.setHttpPort(original.getHttpPort());
        node.setRackName(original.getRackName());
        node.setTotalCapability(original.getTotalCapability());
        node.pushContainerUpdates(original.pullContainerUpdates());
        node.setNodeLabels(original.getNodeLabels());
        return node;
    }

    @Override
    public abstract NodeId getNodeID();

    public abstract void setNodeID(NodeId nodeId);

    @Override
    public abstract String getHostName();

    public abstract void setHostName(String hostName);

    @Override
    public abstract int getCommandPort();

    public abstract void setCommandPort(int commandPort);

    @Override
    public abstract int getHttpPort();

    public abstract void setHttpPort(int httpPort);

    @Override
    public abstract String getNodeAddress();

    @Override
    public abstract String getHttpAddress();

    @Override
    public String getHealthReport() {
        throw new NotImplementedException();
    }

    @Override
    public long getLastHealthReportTime() {
        throw new NotImplementedException();
    }

    @Override
    public String getNodeManagerVersion() {
        throw new NotImplementedException();
    }

    @Override
    public abstract Resource getTotalCapability();

    public abstract void setTotalCapability(Resource capability);

    @Override
    public abstract String getRackName();

    public abstract void setRackName(String rackName);

    @Override
    public Node getNode() {
        throw new NotImplementedException();
    }

    @Override
    public NodeState getState() {
        throw new NotImplementedException();
    }

    @Override
    public List<ContainerId> getContainersToCleanUp() {
        throw new NotImplementedException();
    }

    @Override
    public List<ApplicationId> getAppsToCleanup() {
        throw new NotImplementedException();
    }

    @Override
    public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response) {
        throw new NotImplementedException();
    }

    @Override
    public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {
        throw new NotImplementedException();
    }

    @Override
    public abstract List<UpdatedContainerInfo> pullContainerUpdates();

    public abstract void pushContainerUpdates(List<UpdatedContainerInfo> updates);


    @Override
    public abstract Set<String> getNodeLabels();

    public abstract void setNodeLabels(Set<String> nodeLabels);
}
