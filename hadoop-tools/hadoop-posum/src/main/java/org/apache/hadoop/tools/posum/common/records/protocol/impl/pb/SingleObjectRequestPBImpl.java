package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectRequest;
import org.apache.hadoop.tools.posum.proto.POSUMProtos.SingleObjectRequestProto;
import org.apache.hadoop.tools.posum.proto.POSUMProtos.SingleObjectRequestProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SingleObjectRequestPBImpl extends SingleObjectRequest {
    SingleObjectRequestProto proto = SingleObjectRequestProto.getDefaultInstance();
    SingleObjectRequestProto.Builder builder = null;
    boolean viaProto = false;

    String objectId;
    String objectClass;

    public SingleObjectRequestPBImpl() {
        builder = SingleObjectRequestProto.newBuilder();
    }

    public SingleObjectRequestPBImpl(SingleObjectRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleObjectRequestProto getProto() {
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
        builder.setObjectClass(objectClass);
        builder.setObjectId(objectId);
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
            builder = SingleObjectRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getObjectClass() {
        SingleObjectRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getObjectClass();
    }

    @Override
    public void setObjectClass(String objectClass) {
        maybeInitBuilder();
        builder.setObjectClass(objectClass);
    }

    @Override
    public String getObjectId() {
        SingleObjectRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getObjectId();
    }

    @Override
    public void setObjectId(String objectId) {
        maybeInitBuilder();
        builder.setObjectId(objectId);
    }
}