package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.HandleEventRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMNode;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToLabelsResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceOptionPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleEventRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleEventRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NMContainerStatusPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

import java.util.*;

/**
 * Created by ane on 3/20/16.
 */
public class HandleEventRequestPBImpl extends HandleEventRequest {
    private HandleEventRequestProto proto = HandleEventRequestProto.getDefaultInstance();
    private HandleEventRequestProto.Builder builder = null;
    private boolean viaProto = false;

    private List<NMContainerStatus> containerReports;

    public HandleEventRequestPBImpl() {
        builder = HandleEventRequestProto.newBuilder();
    }

    public HandleEventRequestPBImpl(HandleEventRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public HandleEventRequestProto getProto() {
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
            builder = HandleEventRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }


    @Override
    protected Map<NodeId, Set<String>> getUpdatedNodeLabels() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new GetNodesToLabelsResponsePBImpl(p.getUpdatedNodeLabels()).getNodeToLabels();
    }

    @Override
    protected void setUpdatedNodeLabels(Map<NodeId, Set<String>> updatedNodeToLabels) {
        maybeInitBuilder();
        builder.clearUpdatedNodeLabels();
        if (updatedNodeToLabels != null) {
            GetNodesToLabelsResponse wrapper = GetNodesToLabelsResponse.newInstance(updatedNodeToLabels);
            builder.setUpdatedNodeLabels(((GetNodesToLabelsResponsePBImpl) wrapper).getProto());
        }
    }

    @Override
    public SchedulerEventType getEventType() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return SchedulerEventType.valueOf(p.getEventType().name().substring("EVENT_".length()));
    }

    @Override
    public void setEventType(SchedulerEventType type) {
        maybeInitBuilder();
        builder.setEventType(HandleEventRequestProto.EventTypeProto.valueOf("EVENT_" + type.name()));
    }

    @Override
    public POSUMNode getNode() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new POSUMNodePBImpl(p.getNode());
    }

    @Override
    public void setNode(POSUMNode node) {
        maybeInitBuilder();
        builder.setNode(((POSUMNodePBImpl) node).getProto());
    }

    @Override
    public List<NMContainerStatus> getContainerReports() {
        if (containerReports == null) {
            HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
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

    @Override
    public ResourceOption getResourceOption() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ResourceOptionPBImpl(p.getResourceOption());
    }

    @Override
    public void setResourceOption(ResourceOption option) {
        maybeInitBuilder();
        builder.setResourceOption(((ResourceOptionPBImpl) option).getProto());
    }

    @Override
    public boolean getFlag() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFlag();
    }

    @Override
    public void setFlag(boolean flag) {
        maybeInitBuilder();
        builder.setFlag(flag);
    }

    @Override
    public ApplicationId getApplicationId() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ApplicationIdPBImpl(p.getApplicationId());
    }

    @Override
    public void setApplicationId(ApplicationId appId) {
        maybeInitBuilder();
        builder.setApplicationId(((ApplicationIdPBImpl) appId).getProto());
    }

    @Override
    public String getQueue() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return builder.getQueue();
    }

    @Override
    public void setQueue(String queue) {
        maybeInitBuilder();
        builder.setQueue(queue);
    }

    @Override
    public String getUser() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return builder.getUser();
    }

    @Override
    public void setUser(String user) {
        maybeInitBuilder();
        builder.setUser(user);
    }

    @Override
    public String getFinalState() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinalState();
    }

    @Override
    public void setFinalState(String state) {
        maybeInitBuilder();
        builder.setFinalState(state);
    }

    @Override
    public int getAttemptId() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAttemptId();
    }

    @Override
    public void setAttemptId(int attemptId) {
        maybeInitBuilder();
        builder.setAttemptId(attemptId);
    }

    @Override
    public long getContainerId() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getContainerId();
    }

    @Override
    public void setContainerId(long containerId) {
        maybeInitBuilder();
        builder.setContainerId(containerId);
    }

    @Override
    public ReservationId getReservationId() {
        HandleEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ReservationIdPBImpl(p.getReservationId());
    }

    @Override
    public void setReservationId(ReservationId reservationId) {
        maybeInitBuilder();
        builder.setReservationId(((ReservationIdPBImpl) reservationId).getProto());
    }
}
