package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByIdCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.ByIdCallProto;
import org.apache.hadoop.yarn.proto.PosumProtos.ByIdCallProtoOrBuilder;

public class DeleteByIdCallPBImpl extends DeleteByIdCall implements PayloadPB {
    private ByIdCallProto proto = ByIdCallProto.getDefaultInstance();
    private ByIdCallProto.Builder builder = null;
    private boolean viaProto = false;

    public DeleteByIdCallPBImpl() {
        builder = ByIdCallProto.newBuilder();
    }

    public DeleteByIdCallPBImpl(ByIdCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public ByIdCallProto getProto() {
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
            builder = ByIdCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        ByIdCallProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getCollection().name().substring("COLL_".length()));
    }

    @Override
    public void setEntityCollection(DataEntityCollection type) {
        if (type == null)
            return;
        maybeInitBuilder();
        builder.setCollection(PosumProtos.EntityCollectionProto.valueOf("COLL_" + type.name()));
    }

    @Override
    public String getId() {
        ByIdCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = ByIdCallProto.parseFrom(data);
        viaProto = true;
    }
}