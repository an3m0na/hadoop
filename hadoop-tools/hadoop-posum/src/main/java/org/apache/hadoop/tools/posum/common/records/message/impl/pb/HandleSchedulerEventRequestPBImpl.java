package org.apache.hadoop.tools.posum.common.records.message.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.message.HandleSchedulerEventRequest;
import org.apache.hadoop.tools.posum.common.records.message.POSUMNode;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToLabelsResponsePBImpl;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceOptionPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleSchedulerEventRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleSchedulerEventRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NMContainerStatusPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;

import java.util.*;

/**
 * Created by ane on 3/20/16.
 */
public class HandleSchedulerEventRequestPBImpl extends HandleSchedulerEventRequest {
    private HandleSchedulerEventRequestProto proto = HandleSchedulerEventRequestProto.getDefaultInstance();
    private HandleSchedulerEventRequestProto.Builder builder = null;
    private boolean viaProto = false;

    private List<NMContainerStatus> containerReports;

    public HandleSchedulerEventRequestPBImpl() {
        builder = HandleSchedulerEventRequestProto.newBuilder();
    }

    public HandleSchedulerEventRequestPBImpl(HandleSchedulerEventRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public HandleSchedulerEventRequestProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(getProto());
    }

    private void mergeLocalToBuilder() {
        maybeInitBuilder();
        builder.clearContainerReports();
        if (containerReports != null) {
            final Iterable<NMContainerStatusProto> iterable =
                    new Iterable<NMContainerStatusProto>() {

                        @Override
                        public Iterator<NMContainerStatusProto> iterator() {
                            return new Iterator<NMContainerStatusProto>() {

                                Iterator<NMContainerStatus> iterator = containerReports.iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public NMContainerStatusProto next() {
                                    return ((NMContainerStatusPBImpl) iterator.next()).getProto();
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }
                            };
                        }
                    };
            builder.addAllContainerReports(iterable);
        }
    }

    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = HandleSchedulerEventRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Map<NodeId, Set<String>> getUpdatedNodeLabels() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new GetNodesToLabelsResponsePBImpl(p.getUpdatedNodeLabels()).getNodeToLabels();
    }

    @Override
    public void setUpdatedNodeLabels(Map<NodeId, Set<String>> updatedNodeToLabels) {
        maybeInitBuilder();
        builder.clearUpdatedNodeLabels();
        if (updatedNodeToLabels != null) {
            GetNodesToLabelsResponse wrapper = GetNodesToLabelsResponse.newInstance(updatedNodeToLabels);
            builder.setUpdatedNodeLabels(((GetNodesToLabelsResponsePBImpl) wrapper).getProto());
        }
    }

    public SchedulerEventType getEventType() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return SchedulerEventType.valueOf(p.getEventType().name().substring("EVENT_".length()));
    }

    @Override
    public void setEventType(SchedulerEventType type) {
        maybeInitBuilder();
        builder.setEventType(HandleSchedulerEventRequestProto.SchedulerEventTypeProto.valueOf("EVENT_" + type.name()));
    }

    public POSUMNode getNode() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new POSUMNodePBImpl(p.getNode());
    }

    @Override
    public void setNode(POSUMNode node) {
        maybeInitBuilder();
        builder.setNode(((POSUMNodePBImpl) node).getProto());
    }

    public List<NMContainerStatus> getContainerReports() {
        if (containerReports == null) {
            HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
            containerReports = new ArrayList<>(p.getContainerReportsCount());
            for (NMContainerStatusProto status : p.getContainerReportsList()) {
                containerReports.add(new NMContainerStatusPBImpl(status));
            }
        }
        return containerReports;
    }

    @Override
    public void setContainerReports(List<NMContainerStatus> reports) {
        this.containerReports = reports;
    }

    public ResourceOption getResourceOption() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ResourceOptionPBImpl(p.getResourceOption());
    }

    @Override
    public void setResourceOption(ResourceOption option) {
        maybeInitBuilder();
        builder.setResourceOption(((ResourceOptionPBImpl) option).getProto());
    }

    public boolean getFlag() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFlag();
    }

    @Override
    public void setFlag(boolean flag) {
        maybeInitBuilder();
        builder.setFlag(flag);
    }

    public ApplicationId getApplicationId() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ApplicationIdPBImpl(p.getApplicationId());
    }

    @Override
    public void setApplicationId(ApplicationId appId) {
        maybeInitBuilder();
        builder.setApplicationId(((ApplicationIdPBImpl) appId).getProto());
    }

    public String getQueue() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return builder.getQueue();
    }

    @Override
    public void setQueue(String queue) {
        maybeInitBuilder();
        builder.setQueue(queue);
    }

    public String getUser() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return builder.getUser();
    }

    @Override
    public void setUser(String user) {
        maybeInitBuilder();
        builder.setUser(user);
    }

    public String getFinalState() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinalState();
    }

    @Override
    public void setFinalState(String state) {
        maybeInitBuilder();
        builder.setFinalState(state);
    }

    public ApplicationAttemptId getAttemptId() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return ApplicationAttemptId.newInstance(getApplicationId(), p.getAttemptId());
    }

    @Override
    public void setAttemptId(int attemptId) {
        maybeInitBuilder();
        builder.setAttemptId(attemptId);
    }

    public ContainerId getContainerId() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return ContainerId.newContainerId(getAttemptId(), p.getContainerId());
    }

    @Override
    public void setContainerId(long containerId) {
        maybeInitBuilder();
        builder.setContainerId(containerId);
    }

    public ReservationId getReservationId() {
        HandleSchedulerEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ReservationIdPBImpl(p.getReservationId());
    }

    @Override
    public void setReservationId(ReservationId reservationId) {
        maybeInitBuilder();
        builder.setReservationId(((ReservationIdPBImpl) reservationId).getProto());
    }

    @Override
    public SchedulerEvent getInterpretedEvent() {
        SchedulerEventType type = getEventType();
        switch (type) {
            case NODE_ADDED:
                return new NodeAddedSchedulerEvent(getNode(), getContainerReports());
            case NODE_REMOVED:
                new NodeRemovedSchedulerEvent(getNode());
            case NODE_UPDATE:
                return new NodeUpdateSchedulerEvent(getNode());
            case NODE_RESOURCE_UPDATE:
                return new NodeResourceUpdateSchedulerEvent(getNode(), getResourceOption());
            case NODE_LABELS_UPDATE:
                return new NodeLabelsUpdateSchedulerEvent(getUpdatedNodeLabels());
            case APP_ADDED:
                return new AppAddedSchedulerEvent(
                        getApplicationId(),
                        getQueue(),
                        getUser(),
                        getFlag(),
                        getReservationId()
                );
            case APP_REMOVED:
                return new AppRemovedSchedulerEvent(getApplicationId(),
                        RMAppState.valueOf(getFinalState()));
            case APP_ATTEMPT_ADDED:
                return new AppAttemptAddedSchedulerEvent(
                        getAttemptId(),
                        getFlag()
                );
            case APP_ATTEMPT_REMOVED:
                return new AppAttemptRemovedSchedulerEvent(
                        getAttemptId(),
                        RMAppAttemptState.valueOf(getFinalState()),
                        getFlag()
                );
            case CONTAINER_EXPIRED:
                return new ContainerExpiredSchedulerEvent(getContainerId());
        }
        throw new POSUMException("Unrecognized event type reached POSUM Scheduler " + type);
    }
}
