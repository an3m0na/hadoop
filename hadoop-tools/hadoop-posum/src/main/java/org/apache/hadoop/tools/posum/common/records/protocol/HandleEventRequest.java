package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 4/5/16.
 */
public abstract class HandleEventRequest {

    public static HandleEventRequest newInstance(NodeAddedSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getAddedRMNode()));
        request.setContainerReports(event.getContainerReports());
        return request;
    }

    public static HandleEventRequest newInstance(NodeRemovedSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getRemovedRMNode()));
        return request;
    }

    public static HandleEventRequest newInstance(NodeResourceUpdateSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getRMNode()));
        request.setResourceOption(event.getResourceOption());
        return request;
    }

    public static HandleEventRequest newInstance(NodeUpdateSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getRMNode()));
        return request;
    }

    public static HandleEventRequest newInstance(AppAddedSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationId());
        request.setUser(event.getUser());
        request.setFlag(event.getIsAppRecovering());
        return request;
    }

    public static HandleEventRequest newInstance(AppRemovedSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationID());
        request.setFinalState(event.getFinalState().name());
        return request;
    }

    public static HandleEventRequest newInstance(AppAttemptAddedSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationAttemptId().getApplicationId());
        request.setAttemptId(event.getApplicationAttemptId().getAttemptId());
        request.setFlag(event.getIsAttemptRecovering());
        return request;
    }

    public static HandleEventRequest newInstance(AppAttemptRemovedSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationAttemptID().getApplicationId());
        request.setAttemptId(event.getApplicationAttemptID().getAttemptId());
        request.setFinalState(event.getFinalAttemptState().name());
        request.setFlag(event.getKeepContainersAcrossAppAttempts());
        return request;
    }

    public static HandleEventRequest newInstance(ContainerExpiredSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getContainerId().getApplicationAttemptId().getApplicationId());
        request.setAttemptId(event.getContainerId().getApplicationAttemptId().getAttemptId());
        request.setContainerId(event.getContainerId().getContainerId());
        return request;
    }

    public static HandleEventRequest newInstance(NodeLabelsUpdateSchedulerEvent event) {
        HandleEventRequest request = newInstance(event.getType());
        request.setUpdatedNodeLabels(event.getUpdatedNodeToLabels());
        return request;
    }

    public abstract Map<NodeId, Set<String>> getUpdatedNodeLabels();

    public abstract void setUpdatedNodeLabels(Map<NodeId, Set<String>> updatedNodeToLabels);

    public static HandleEventRequest newInstance(SchedulerEvent event) {
        return newInstance(event.getType());
    }

    public static HandleEventRequest newInstance(SchedulerEventType type) {
        HandleEventRequest request = Records.newRecord(HandleEventRequest.class);
        request.setEventType(type);
        return request;
    }

    public abstract SchedulerEventType getEventType();

    public abstract void setEventType(SchedulerEventType type);

    public abstract POSUMNode getNode();

    public abstract void setNode(POSUMNode node);

    public abstract List<NMContainerStatus> getContainerReports();

    public abstract void setContainerReports(List<NMContainerStatus> reports);

    public abstract ResourceOption getResourceOption();

    public abstract void setResourceOption(ResourceOption option);

    public abstract boolean getFlag();

    public abstract void setFlag(boolean flag);

    public abstract ApplicationId getApplicationId();

    public abstract void setApplicationId(ApplicationId appId);

    public abstract String getQueue();

    public abstract void setQueue(String queue);

    public abstract String getUser();

    public abstract void setUser(String user);

    public abstract String getFinalState();

    public abstract void setFinalState(String state);

    public abstract int getAttemptId();

    public abstract void setAttemptId(int attemptId);

    public abstract long getContainerId();

    public abstract void setContainerId(long containerId);

    public abstract ReservationId getReservationId();

    public abstract void setReservationId(ReservationId reservationId);

}

