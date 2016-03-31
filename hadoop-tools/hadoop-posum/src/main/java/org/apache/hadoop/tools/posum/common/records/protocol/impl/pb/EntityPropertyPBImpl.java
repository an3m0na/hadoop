package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.protocol.EntityProperty;
import org.apache.hadoop.yarn.proto.POSUMProtos.EntityPropertyProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.EntityPropertyProtoOrBuilder;
import java.io.IOException;

/**
 * Created by ane on 3/20/16.
 */
public class EntityPropertyPBImpl extends EntityProperty {
    private EntityPropertyProto proto = EntityPropertyProto.getDefaultInstance();
    private EntityPropertyProto.Builder builder = null;
    private boolean viaProto = false;

    public EntityPropertyPBImpl() {
        builder = EntityPropertyProto.newBuilder();
    }

    public EntityPropertyPBImpl(EntityPropertyProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public EntityPropertyProto getProto() {
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
            builder = EntityPropertyProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public PropertyType getType() {
        EntityPropertyProtoOrBuilder p = viaProto ? proto : builder;
        return PropertyType.valueOf(p.getType().name().substring("PROPERTY_".length()));
    }

    @Override
    public void setType(EntityProperty.PropertyType type) {
        maybeInitBuilder();
        builder.setType(EntityPropertyProto.PropertyTypeProto.valueOf("PROPERTY_" + type.name()));
    }

    @Override
    public Object getValue() {
        EntityPropertyProtoOrBuilder p = viaProto ? proto : builder;
        try {
            return getType().getReader().read(p.getValue());
        } catch (IOException e) {
            throw new POSUMException("Error reading property value ", e);
        }
    }

    @Override
    public void setValue(Object value) {
        maybeInitBuilder();
        if (value != null)
            builder.setValue(value.toString());
    }

    @Override
    public String getName() {
        EntityPropertyProtoOrBuilder p = viaProto ? proto : builder;
        return p.getName();
    }

    @Override
    public void setName(String name) {
        maybeInitBuilder();
        builder.setName(name);
    }


}