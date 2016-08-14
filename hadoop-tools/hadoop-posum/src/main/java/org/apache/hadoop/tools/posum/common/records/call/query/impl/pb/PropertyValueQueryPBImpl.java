package org.apache.hadoop.tools.posum.common.records.call.query.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyValueQueryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyValueQueryProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class PropertyValueQueryPBImpl extends PropertyValueQuery implements PayloadPB {
    private PropertyValueQueryProto proto = PropertyValueQueryProto.getDefaultInstance();
    private PropertyValueQueryProto.Builder builder = null;
    private boolean viaProto = false;

    public PropertyValueQueryPBImpl() {
        builder = PropertyValueQueryProto.newBuilder();
    }

    public PropertyValueQueryPBImpl(PropertyValueQueryProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public PropertyValueQueryProto getProto() {
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
            builder = PropertyValueQueryProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Type getType() {
        PropertyValueQueryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasType())
            return null;
        return Type.valueOf(p.getType().name().substring("PQRY_".length()));
    }

    @Override
    public void setType(Type type) {
        maybeInitBuilder();
        if (type == null) {
            builder.clearType();
            return;
        }
        builder.setType(PosumProtos.PropertyValueQueryProto.TypeProto.valueOf("PQRY_" + type.name()));
    }

    @Override
    public SimplePropertyPayload getProperty() {
        PropertyValueQueryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasProperty())
            return null;
        return new SimplePropertyPayloadPBImpl(p.getProperty());
    }

    @Override
    public void setProperty(SimplePropertyPayload property) {
        maybeInitBuilder();
        if (property == null) {
            builder.clearProperty();
            return;
        }
        builder.setProperty(((SimplePropertyPayloadPBImpl) property).getProto());
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = PropertyValueQueryProto.parseFrom(data);
        viaProto = true;
    }
}
