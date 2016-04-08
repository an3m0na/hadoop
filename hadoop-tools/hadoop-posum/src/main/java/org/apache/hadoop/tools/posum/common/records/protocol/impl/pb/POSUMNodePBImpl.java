package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMNode;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.POSUMNodeProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.UpdatedContainerInfoProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.POSUMNodeProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;

import java.util.*;

/**
 * Created by ane on 3/20/16.
 */
public class POSUMNodePBImpl extends POSUMNode {
    private POSUMNodeProto proto = POSUMNodeProto.getDefaultInstance();
    private POSUMNodeProto.Builder builder = null;
    private boolean viaProto = false;


    List<UpdatedContainerInfo> containerUpdates;
    Set<String> nodeLabels;

    public POSUMNodePBImpl() {
        builder = POSUMNodeProto.newBuilder();
    }

    public POSUMNodePBImpl(POSUMNodeProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public POSUMNodeProto getProto() {
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
        builder.clearNodeLabels();
        if (nodeLabels != null) {
            builder.addAllNodeLabels(nodeLabels);
        }
        if (containerUpdates != null) {
            final Iterable<UpdatedContainerInfoProto> iterable =
                    new Iterable<UpdatedContainerInfoProto>() {

                        @Override
                        public Iterator<UpdatedContainerInfoProto> iterator() {
                            return new Iterator<UpdatedContainerInfoProto>() {

                                Iterator<UpdatedContainerInfo> iterator = containerUpdates.iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public UpdatedContainerInfoProto next() {
                                    return convertToContainerInfoProto(iterator.next());
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }
                            };
                        }
                    };
            builder.addAllContainerUpdates(iterable);
        }
    }

    private class StatusIterable implements Iterable<YarnProtos.ContainerStatusProto> {
        Iterator<ContainerStatus> iterator;

        StatusIterable(List<ContainerStatus> base) {
            this.iterator = base.iterator();
        }

        @Override
        public Iterator<YarnProtos.ContainerStatusProto> iterator() {
            return new Iterator<YarnProtos.ContainerStatusProto>() {
                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public YarnProtos.ContainerStatusProto next() {
                    return ((ContainerStatusPBImpl) iterator.next()).getProto();
                }

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }
            };
        }
    }

    private UpdatedContainerInfoProto convertToContainerInfoProto(UpdatedContainerInfo info) {
        UpdatedContainerInfoProto.Builder protoBuilder = UpdatedContainerInfoProto.newBuilder();
        protoBuilder.addAllNewContainers(new StatusIterable(info.getNewlyLaunchedContainers()));
        protoBuilder.addAllCompletedContainers(new StatusIterable(info.getCompletedContainers()));
        return protoBuilder.build();
    }

    private List<ContainerStatus> convertFromStatusListProto(List<YarnProtos.ContainerStatusProto> protoList) {
        List<ContainerStatus> statuses = new ArrayList<>(protoList.size());
        for (YarnProtos.ContainerStatusProto proto : protoList) {
            statuses.add(new ContainerStatusPBImpl(proto));
        }
        return statuses;
    }

    private UpdatedContainerInfo convertFromContainerInfoProto(UpdatedContainerInfoProto infoProto) {
        return new UpdatedContainerInfo(
                convertFromStatusListProto(infoProto.getNewContainersList()),
                convertFromStatusListProto(infoProto.getCompletedContainersList())
        );
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
            builder = POSUMNodeProto.newBuilder(proto);
        }
        viaProto = false;
    }


    @Override
    public NodeId getNodeID() {
        POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
        return new NodeIdPBImpl(p.getNodeId());
    }

    @Override
    public void setNodeID(NodeId nodeId) {
        maybeInitBuilder();
        builder.setNodeId(((NodeIdPBImpl) nodeId).getProto());
    }

    @Override
    public String getHostName() {
        POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
        return p.getHostname();
    }

    @Override
    public void setHostName(String hostName) {
        maybeInitBuilder();
        builder.setHostname(hostName);
    }

    @Override
    public int getCommandPort() {
        POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
        return p.getCommandPort();

    }

    @Override
    public void setCommandPort(int commandPort) {
        maybeInitBuilder();
        builder.setCommandPort(commandPort);
    }

    @Override
    public int getHttpPort() {
        POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
        return p.getHttpPort();
    }

    @Override
    public void setHttpPort(int httpPort) {
        maybeInitBuilder();
        builder.setHttpPort(httpPort);
    }

    @Override
    public String getNodeAddress() {
        //like in RMNodeImpl
        return getHostName() + ":" + getCommandPort();
    }

    @Override
    public String getHttpAddress() {
        //like in RMNodeImpl
        return getHostName() + ":" + getHttpPort();
    }


    @Override
    public Resource getTotalCapability() {
        POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
        return new ResourcePBImpl(p.getTotalCapability());
    }

    @Override
    public void setTotalCapability(Resource capability) {
        maybeInitBuilder();
        builder.setTotalCapability(((ResourcePBImpl) capability).getProto());
    }

    @Override
    public String getRackName() {
        POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
        return p.getRackName();
    }

    @Override
    public void setRackName(String rackName) {
        maybeInitBuilder();
        builder.setRackName(rackName);
    }

    @Override
    public List<UpdatedContainerInfo> pullContainerUpdates() {
        if (containerUpdates == null) {
            POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
            containerUpdates = new ArrayList<>(p.getContainerUpdatesCount());
            for (UpdatedContainerInfoProto info : p.getContainerUpdatesList()) {
                containerUpdates.add(convertFromContainerInfoProto(info));
            }
        }
        return containerUpdates;
    }

    @Override
    public void pushContainerUpdates(List<UpdatedContainerInfo> updates) {
        if (updates == null)
            return;
        this.containerUpdates = new ArrayList<>(updates);
    }

    @Override
    public Set<String> getNodeLabels() {
        if (nodeLabels == null) {
            POSUMNodeProtoOrBuilder p = viaProto ? proto : builder;
            nodeLabels = new HashSet<>(p.getNodeLabelsList());
        }
        return nodeLabels;
    }

    @Override
    public void setNodeLabels(Set<String> nodeLabels) {
        if (nodeLabels == null)
            return;
        this.nodeLabels = new HashSet<>(nodeLabels);
    }
}