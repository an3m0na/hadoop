package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleEntityRequest;
import org.apache.hadoop.tools.posum.database.store.DataEntityType;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityRequestProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityRequestPBImpl extends SingleEntityRequest {
    SingleEntityRequestProto proto = SingleEntityRequestProto.getDefaultInstance();
    SingleEntityRequestProto.Builder builder = null;
    boolean viaProto = false;

    public SingleEntityRequestPBImpl() {
        builder = SingleEntityRequestProto.newBuilder();
    }

    public SingleEntityRequestPBImpl(SingleEntityRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleEntityRequestProto getProto() {
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
            builder = SingleEntityRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityType getType() {
        SingleEntityRequestProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataEntityType type) {
        maybeInitBuilder();
        builder.setType(POSUMProtos.EntityType.valueOf("TYPE_"+type.name()));
    }

    @Override
    public String getId() {
        SingleEntityRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

}