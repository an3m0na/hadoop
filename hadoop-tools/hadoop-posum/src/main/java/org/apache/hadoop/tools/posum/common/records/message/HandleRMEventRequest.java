package org.apache.hadoop.tools.posum.common.records.message;

import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMoveEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.util.Records;


/**
 * Created by ane on 4/5/16.
 */
public abstract class HandleRMEventRequest {

    public static HandleRMEventRequest newInstance(RMAppEvent event) {
        HandleRMEventRequest request = Records.newRecord(HandleRMEventRequest.class);
        request.setEventClass("RMAppEvent");
        request.setEventType(event.getType().name());
        request.setApplicationId(event.getApplicationId());
        return request;
    }

    public static HandleRMEventRequest newInstance(RMAppMoveEvent event) {
        HandleRMEventRequest request = Records.newRecord(HandleRMEventRequest.class);
        request.setEventClass("RMAppMoveEvent");
        request.setEventType(event.getType().name());
        request.setQueue(event.getTargetQueue());
        return request;
    }

    public static HandleRMEventRequest newInstance(RMNodeCleanContainerEvent event) {
        HandleRMEventRequest request = Records.newRecord(HandleRMEventRequest.class);
        request.setEventClass("RMNodeCleanContainerEvent");
        request.setEventType(event.getType().name());
        request.setNodeId(event.getNodeId());
        ContainerId containerId = event.getContainerId();
        ApplicationAttemptId attemptId = containerId.getApplicationAttemptId();
        request.setApplicationId(attemptId.getApplicationId());
        request.setAttemptId(attemptId.getAttemptId());
        request.setContainerId(event.getContainerId().getContainerId());
        return request;
    }

    public static HandleRMEventRequest newInstance(Event event) {
        throw new POSUMException("Event serialization not defined in the system for " + event.getType());
    }

    public abstract Event getInterpretedEvent();

    public abstract void setEventType(String type);

    public abstract void setEventClass(String eventClass);

    public abstract void setNodeId(NodeId node);

    public abstract void setApplicationId(ApplicationId appId);

    public abstract void setQueue(String queue);

    public abstract void setAttemptId(int attemptId);

    public abstract void setContainerId(long containerId);

}

