package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.protocol.EntityProperty;
import org.apache.hadoop.tools.posum.common.records.protocol.MultiEntityRequest;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityRequestProtoOrBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class MultiEntityRequestPBImpl extends MultiEntityRequest {

    private MultiEntityRequestProto proto = MultiEntityRequestProto.getDefaultInstance();
    private MultiEntityRequestProto.Builder builder = null;
    private boolean viaProto = false;

    public MultiEntityRequestPBImpl() {
        builder = MultiEntityRequestProto.newBuilder();
    }

    public MultiEntityRequestPBImpl(MultiEntityRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public MultiEntityRequestProto getProto() {
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
            builder = MultiEntityRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityType getType() {
        MultiEntityRequestProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataEntityType type) {
        maybeInitBuilder();
        builder.setType(POSUMProtos.EntityTypeProto.valueOf("TYPE_" + type.name()));
    }

    @Override
    public Map<String, Object> getProperties() {
        MultiEntityRequestProtoOrBuilder p = viaProto ? proto : builder;
        Map<String, Object> properties = new HashMap<>(p.getPropertiesCount());
        for (POSUMProtos.EntityPropertyProto propertyProto : p.getPropertiesList()) {
            EntityProperty property = new EntityPropertyPBImpl(propertyProto);
            properties.put(property.getName(), property.getValue());
        }
        return properties;
    }

    @Override
    public void setProperties(Map<String, Object> properties) {
        maybeInitBuilder();
        for (Map.Entry<String, Object> propertyEntry : properties.entrySet()) {
            EntityPropertyPBImpl property = new EntityPropertyPBImpl();
            property.setName(propertyEntry.getKey());
            property.setType(EntityProperty.PropertyType.getByClass(propertyEntry.getValue().getClass()));
            property.setValue(propertyEntry.getValue());
            builder.addProperties(property.getProto());
        }
    }
}
