package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.request.EntityByIdPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.EntityByIdPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.EntityByIdPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class EntityByIdPayloadPBImpl extends EntityByIdPayload {
    private EntityByIdPayloadProto proto = EntityByIdPayloadProto.getDefaultInstance();
    private EntityByIdPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public EntityByIdPayloadPBImpl() {
        builder = EntityByIdPayloadProto.newBuilder();
    }

    public EntityByIdPayloadPBImpl(EntityByIdPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public EntityByIdPayloadProto getProto() {
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
            builder = EntityByIdPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityType getEntityType() {
        EntityByIdPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getEntityType().name().substring("TYPE_".length()));
    }

    @Override
    public void setEntityType(DataEntityType type) {
        maybeInitBuilder();
        builder.setEntityType(POSUMProtos.EntityTypeProto.valueOf("TYPE_"+type.name()));
    }

    @Override
    public String getId() {
        EntityByIdPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

}