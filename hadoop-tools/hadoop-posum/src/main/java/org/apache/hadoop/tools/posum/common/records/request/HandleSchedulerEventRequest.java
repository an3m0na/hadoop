package org.apache.hadoop.tools.posum.common.records.request;

import org.apache.hadoop.tools.posum.common.records.field.POSUMNode;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 4/5/16.
 */
public abstract class HandleSchedulerEventRequest {

    public static HandleSchedulerEventRequest newInstance(NodeAddedSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getAddedRMNode()));
        request.setContainerReports(event.getContainerReports());
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(NodeRemovedSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getRemovedRMNode()));
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(NodeResourceUpdateSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getRMNode()));
        request.setResourceOption(event.getResourceOption());
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(NodeUpdateSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setNode(POSUMNode.newInstance(event.getRMNode()));
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(AppAddedSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationId());
        request.setUser(event.getUser());
        request.setFlag(event.getIsAppRecovering());
        request.setReservationId(event.getReservationID());
        request.setQueue(event.getQueue());
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(AppRemovedSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationID());
        request.setFinalState(event.getFinalState().name());
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(AppAttemptAddedSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationAttemptId().getApplicationId());
        request.setAttemptId(event.getApplicationAttemptId().getAttemptId());
        request.setFlag(event.getIsAttemptRecovering());
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(AppAttemptRemovedSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getApplicationAttemptID().getApplicationId());
        request.setAttemptId(event.getApplicationAttemptID().getAttemptId());
        request.setFinalState(event.getFinalAttemptState().name());
        request.setFlag(event.getKeepContainersAcrossAppAttempts());

        return request;
    }

    public static HandleSchedulerEventRequest newInstance(ContainerExpiredSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setApplicationId(event.getContainerId().getApplicationAttemptId().getApplicationId());
        request.setAttemptId(event.getContainerId().getApplicationAttemptId().getAttemptId());
        request.setContainerId(event.getContainerId().getContainerId());
        return request;
    }

    public static HandleSchedulerEventRequest newInstance(NodeLabelsUpdateSchedulerEvent event) {
        HandleSchedulerEventRequest request = newInstance(event.getType());
        request.setUpdatedNodeLabels(event.getUpdatedNodeToLabels());
        return request;
    }

    public abstract Map<NodeId, Set<String>> getUpdatedNodeLabels();

    public abstract void setUpdatedNodeLabels(Map<NodeId, Set<String>> updatedNodeToLabels);

    public static HandleSchedulerEventRequest newInstance(SchedulerEvent event) {
        return newInstance(event.getType());
    }

    public static HandleSchedulerEventRequest newInstance(SchedulerEventType type) {
        HandleSchedulerEventRequest request = Records.newRecord(HandleSchedulerEventRequest.class);
        request.setEventType(type);
        return request;
    }

    public abstract SchedulerEvent getInterpretedEvent();

    public abstract void setEventType(SchedulerEventType type);

    public abstract void setNode(POSUMNode node);

    public abstract void setContainerReports(List<NMContainerStatus> reports);

    public abstract void setResourceOption(ResourceOption option);

    public abstract void setFlag(boolean flag);

    public abstract void setApplicationId(ApplicationId appId);

    public abstract void setQueue(String queue);

    public abstract void setUser(String user);

    public abstract void setFinalState(String state);

    public abstract void setAttemptId(int attemptId);

    public abstract void setContainerId(long containerId);

    public abstract void setReservationId(ReservationId reservationId);

}

