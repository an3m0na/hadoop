package org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.message.simple.SchedulerAllocatePayload;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.*;
import org.apache.hadoop.yarn.proto.POSUMProtos.SchedulerAllocatePayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SchedulerAllocatePayloadProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;

import java.util.*;

/**
 * Created by ane on 3/20/16.
 */
public class SchedulerAllocatePayloadPBImpl extends SchedulerAllocatePayload {
    private SchedulerAllocatePayloadProto proto = SchedulerAllocatePayloadProto.getDefaultInstance();
    private SchedulerAllocatePayloadProto.Builder builder = null;
    private boolean viaProto = false;

    private Allocation allocation;


    public SchedulerAllocatePayloadPBImpl() {
        builder = SchedulerAllocatePayloadProto.newBuilder();
    }

    public SchedulerAllocatePayloadPBImpl(SchedulerAllocatePayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SchedulerAllocatePayloadProto getProto() {
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

    private void addContainers() {
        builder.clearContainers();
        if (allocation.getContainers() != null) {
            final Iterable<YarnProtos.ContainerProto> iterable =
                    new Iterable<YarnProtos.ContainerProto>() {
                        @Override
                        public Iterator<YarnProtos.ContainerProto> iterator() {
                            return new Iterator<YarnProtos.ContainerProto>() {

                                Iterator<Container> iterator = allocation.getContainers().iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public YarnProtos.ContainerProto next() {
                                    return ((ContainerPBImpl) iterator.next()).getProto();
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }
                            };
                        }
                    };
            builder.addAllContainers(iterable);
        }
    }

    private void addResourceRequests() {
        builder.clearFungibleResources();
        if (allocation.getResourcePreemptions() != null) {
            final Iterable<YarnProtos.ResourceRequestProto> iterable =
                    new Iterable<YarnProtos.ResourceRequestProto>() {
                        @Override
                        public Iterator<YarnProtos.ResourceRequestProto> iterator() {
                            return new Iterator<YarnProtos.ResourceRequestProto>() {

                                Iterator<ResourceRequest> iterator = allocation.getResourcePreemptions().iterator();

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
            builder.addAllFungibleResources(iterable);
        }
    }

    private void addNMTokens() {
        builder.clearNmTokens();
        if (allocation.getNMTokens() != null) {
            final Iterable<YarnServiceProtos.NMTokenProto> iterable =
                    new Iterable<YarnServiceProtos.NMTokenProto>() {
                        @Override
                        public Iterator<YarnServiceProtos.NMTokenProto> iterator() {
                            return new Iterator<YarnServiceProtos.NMTokenProto>() {

                                Iterator<NMToken> iterator = allocation.getNMTokens().iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public YarnServiceProtos.NMTokenProto next() {
                                    return ((NMTokenPBImpl) iterator.next()).getProto();
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }
                            };
                        }
                    };
            builder.addAllNmTokens(iterable);
        }
    }

    private Iterable<YarnProtos.ContainerIdProto> convertContainerIds(final Set<ContainerId> ids) {
        return new Iterable<YarnProtos.ContainerIdProto>() {

            @Override
            public Iterator<YarnProtos.ContainerIdProto> iterator() {
                return new Iterator<YarnProtos.ContainerIdProto>() {

                    Iterator<ContainerId> iterator = ids.iterator();

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
    }

    private void mergeLocalToBuilder() {
        maybeInitBuilder();
        if(allocation == null)
            return;
        addContainers();
        builder.setResourceLimit(((ResourcePBImpl)allocation.getResourceLimit()).getProto());
        if(allocation.getStrictContainerPreemptions() != null){
            builder.addAllStrictContainers(convertContainerIds(allocation.getStrictContainerPreemptions()));
        }
        if(allocation.getContainerPreemptions() != null){
            builder.addAllFungibleContainers(convertContainerIds(allocation.getContainerPreemptions()));
        }
        addResourceRequests();
        addNMTokens();
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
            builder = SchedulerAllocatePayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Allocation getAllocation() {
        if (allocation == null) {
            SchedulerAllocatePayloadProtoOrBuilder p = viaProto ? proto : builder;
            List<Container> containers = new ArrayList<>(p.getContainersCount());
            for (YarnProtos.ContainerProto element : p.getContainersList()) {
                containers.add(new ContainerPBImpl(element));
            }
            Resource resourceLimit = new ResourcePBImpl(p.getResourceLimit());
            Set<ContainerId> strictContainers = new HashSet<>(p.getStrictContainersCount());
            for (YarnProtos.ContainerIdProto element : p.getStrictContainersList()) {
                strictContainers.add(new ContainerIdPBImpl(element));
            }
            Set<ContainerId> fungibleContainers = new HashSet<>(p.getFungibleContainersCount());
            for (YarnProtos.ContainerIdProto element : p.getFungibleContainersList()) {
                fungibleContainers.add(new ContainerIdPBImpl(element));
            }
            List<ResourceRequest> fungibleResources = new ArrayList<>(p.getFungibleResourcesCount());
            for (YarnProtos.ResourceRequestProto element : p.getFungibleResourcesList()) {
                fungibleResources.add(new ResourceRequestPBImpl(element));
            }
            List<NMToken> nmTokens = new ArrayList<>(p.getNmTokensCount());
            for (YarnServiceProtos.NMTokenProto element : p.getNmTokensList()) {
                nmTokens.add(new NMTokenPBImpl(element));
            }
            allocation = new Allocation(containers, resourceLimit, strictContainers,
                    fungibleContainers, fungibleResources, nmTokens);
        }
        return allocation;
    }

    @Override
    public void setAllocation(Allocation allocation) {
        this.allocation = allocation;
    }
}
