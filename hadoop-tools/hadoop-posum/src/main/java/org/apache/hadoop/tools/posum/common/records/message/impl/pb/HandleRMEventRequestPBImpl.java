package org.apache.hadoop.tools.posum.common.records.message.impl.pb;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.message.HandleRMEventRequest;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleRMEventRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleRMEventRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMoveEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;

/**
 * Created by ane on 3/20/16.
 */
public class HandleRMEventRequestPBImpl extends HandleRMEventRequest {
    private HandleRMEventRequestProto proto = HandleRMEventRequestProto.getDefaultInstance();
    private HandleRMEventRequestProto.Builder builder = null;
    private boolean viaProto = false;

    public HandleRMEventRequestPBImpl() {
        builder = HandleRMEventRequestProto.newBuilder();
    }

    public HandleRMEventRequestPBImpl(HandleRMEventRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public HandleRMEventRequestProto getProto() {
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
//        builder.clearContainerReports();
//        if (containerReports != null) {
//            final Iterable<NMContainerStatusProto> iterable =
//                    new Iterable<NMContainerStatusProto>() {
//
//                        @Override
//                        public Iterator<NMContainerStatusProto> iterator() {
//                            return new Iterator<NMContainerStatusProto>() {
//
//                                Iterator<NMContainerStatus> iterator = containerReports.iterator();
//
//                                @Override
//                                public void remove() {
//                                    throw new UnsupportedOperationException();
//                                }
//
//                                @Override
//                                public NMContainerStatusProto next() {
//                                    return ((NMContainerStatusPBImpl) iterator.next()).getProto();
//                                }
//
//                                @Override
//                                public boolean hasNext() {
//                                    return iterator.hasNext();
//                                }
//                            };
//                        }
//                    };
//            builder.addAllContainerReports(iterable);
//        }
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
            builder = HandleRMEventRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public void setEventClass(String eventClass) {
        maybeInitBuilder();
        builder.setEventClass(HandleRMEventRequestProto.RMEventClassProto.valueOf("RMEVENT_" + eventClass));
    }

    public NodeId getNodeId() {
        HandleRMEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new NodeIdPBImpl(p.getNodeId());
    }

    @Override
    public void setNodeId(NodeId node) {
        maybeInitBuilder();
        builder.setNodeId(((NodeIdPBImpl) node).getProto());
    }

    @Override
    public void setEventType(String type) {
        maybeInitBuilder();
        builder.setEventType(type);
    }

    public ApplicationId getApplicationId() {
        HandleRMEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ApplicationIdPBImpl(p.getApplicationId());
    }

    @Override
    public void setApplicationId(ApplicationId appId) {
        maybeInitBuilder();
        builder.setApplicationId(((ApplicationIdPBImpl) appId).getProto());
    }

    public String getQueue() {
        HandleRMEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getQueue();
    }

    @Override
    public void setQueue(String queue) {
        maybeInitBuilder();
        builder.setQueue(queue);
    }

    public ApplicationAttemptId getAttemptId() {
        HandleRMEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return ApplicationAttemptId.newInstance(getApplicationId(), p.getAttemptId());
    }

    @Override
    public void setAttemptId(int attemptId) {
        maybeInitBuilder();
        builder.setAttemptId(attemptId);
    }

    public ContainerId getContainerId() {
        HandleRMEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        return ContainerId.newContainerId(getAttemptId(), p.getContainerId());
    }

    @Override
    public void setContainerId(long containerId) {
        maybeInitBuilder();
        builder.setContainerId(containerId);
    }

    @Override
    public Event getInterpretedEvent() {
        HandleRMEventRequestProtoOrBuilder p = viaProto ? proto : builder;
        HandleRMEventRequestProto.RMEventClassProto eventClass = p.getEventClass();
        String type = p.getEventType();
        switch (eventClass) {
            case RMEVENT_RMNodeCleanContainerEvent:
                return new RMNodeCleanContainerEvent(getNodeId(), getContainerId());
            case RMEVENT_RMAppEvent:
                return new RMAppAttemptEvent(getAttemptId(), RMAppAttemptEventType.valueOf(type));
            case RMEVENT_RMAppMoveEvent:
                return new RMAppMoveEvent(getApplicationId(), getQueue(), SettableFuture.create());
        }
        throw new POSUMException("Unrecognized event type reached POSUM Scheduler " + type);
    }
}
