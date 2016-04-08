package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.HandleEventRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMNode;
import org.apache.hadoop.tools.posum.common.records.protocol.SchedulerAllocateRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.*;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SchedulerAllocateRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SchedulerAllocateRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NMContainerStatusPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public class SchedulerAllocateRequestPBImpl extends SchedulerAllocateRequest {
    private SchedulerAllocateRequestProto proto = SchedulerAllocateRequestProto.getDefaultInstance();
    private SchedulerAllocateRequestProto.Builder builder = null;
    private boolean viaProto = false;

    private List<ResourceRequest> resourceRequests;
    private List<ContainerId> releases;
    private List<String> blacklistAdditions;
    private List<String> blacklistRemovals;


    public SchedulerAllocateRequestPBImpl() {
        builder = SchedulerAllocateRequestProto.newBuilder();
    }

    public SchedulerAllocateRequestPBImpl(SchedulerAllocateRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SchedulerAllocateRequestProto getProto() {
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

    private void convertResourceRequests() {
        builder.clearResourceRequests();
        if (resourceRequests != null) {
            final Iterable<YarnProtos.ResourceRequestProto> iterable =
                    new Iterable<YarnProtos.ResourceRequestProto>() {

                        @Override
                        public Iterator<YarnProtos.ResourceRequestProto> iterator() {
                            return new Iterator<YarnProtos.ResourceRequestProto>() {

                                Iterator<ResourceRequest> iterator = resourceRequests.iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public YarnProtos.ResourceRequestProto next() {
                                    return ((ResourceRequestPBImpl) iterator.next()).getProto();
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }
                            };
                        }
                    };
            builder.addAllResourceRequests(iterable);
        }
    }

    private void convertReleases() {
        builder.clearReleases();
        if (releases != null) {
            final Iterable<YarnProtos.ContainerIdProto> iterable =
                    new Iterable<YarnProtos.ContainerIdProto>() {

                        @Override
                        public Iterator<YarnProtos.ContainerIdProto> iterator() {
                            return new Iterator<YarnProtos.ContainerIdProto>() {

                                Iterator<ContainerId> iterator = releases.iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public YarnProtos.ContainerIdProto next() {
                                    return ((ContainerIdPBImpl) iterator.next()).getProto();
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }
                            };
                        }
                    };
            builder.addAllReleases(iterable);
        }
    }

    private void mergeLocalToBuilder() {
        maybeInitBuilder();
        builder.clearBlacklistAdditions();
        if(blacklistAdditions != null)
            builder.addAllBlacklistAdditions(blacklistAdditions);
        builder.clearBlacklistRemovals();
        if(blacklistRemovals != null){
            builder.addAllBlacklistRemovals(blacklistRemovals);
        }
        convertResourceRequests();
        convertReleases();
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
            builder = SchedulerAllocateRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
        SchedulerAllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new ApplicationAttemptIdPBImpl(p.getApplicationAttemptId());
    }

    @Override
    public void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
        maybeInitBuilder();
        builder.setApplicationAttemptId(((ApplicationAttemptIdPBImpl) applicationAttemptId).getProto());
    }

    @Override
    public List<ResourceRequest> getResourceRequests() {
        if (resourceRequests == null) {
            SchedulerAllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
            resourceRequests = new ArrayList<>(p.getResourceRequestsCount());
            for (YarnProtos.ResourceRequestProto element : p.getResourceRequestsList()) {
                resourceRequests.add(new ResourceRequestPBImpl(element));
            }
        }
        return resourceRequests;
    }

    @Override
    public void setResourceRequests(List<ResourceRequest> resourceRequests) {
        if (resourceRequests == null)
            return;
        this.resourceRequests = new ArrayList<>(resourceRequests);
    }

    @Override
    public List<ContainerId> getReleases() {
        if (releases == null) {
            SchedulerAllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
            releases = new ArrayList<>(p.getReleasesCount());
            for (YarnProtos.ContainerIdProto element : p.getReleasesList()) {
                releases.add(new ContainerIdPBImpl(element));
            }
        }
        return releases;
    }

    @Override
    public void setReleases(List<ContainerId> releases) {
        if (releases == null)
            return;
        this.releases = new ArrayList<>(releases);
    }

    @Override
    public List<String> getBlacklistAdditions() {
        SchedulerAllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getBlacklistAdditionsList();
    }

    @Override
    public void setBlacklistAdditions(List<String> blacklistAdditions) {
        this.blacklistAdditions = blacklistAdditions;
    }

    @Override
    public List<String> getBlacklistRemovals() {
        SchedulerAllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getBlacklistRemovalsList();
    }

    @Override
    public void setBlacklistRemovals(List<String> blacklistRemovals) {
        this.blacklistRemovals = blacklistRemovals;
    }
}
