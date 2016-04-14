package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.field.EntityProperty;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.EntityPropertyPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityRequestProtoOrBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class MultiEntityRequestPBImpl extends MultiEntityRequest {

    private MultiEntityRequestProto proto = MultiEntityRequestProto.getDefaultInstance();
    private MultiEntityRequestProto.Builder builder = null;
    private boolean viaProto = false;

    Map<String, Object> properties;

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
        maybeInitBuilder();
        builder.clearProperties();
        if (properties == null)
            return;
        Iterable<POSUMProtos.EntityPropertyProto> iterable =
                new Iterable<POSUMProtos.EntityPropertyProto>() {

                    @Override
                    public Iterator<POSUMProtos.EntityPropertyProto> iterator() {
                        return new Iterator<POSUMProtos.EntityPropertyProto>() {

                            Iterator<String> keyIter = properties.keySet().iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public POSUMProtos.EntityPropertyProto next() {
                                String key = keyIter.next();
                                Object value = properties.get(key);
                                EntityPropertyPBImpl property = new EntityPropertyPBImpl();
                                property.setName(key);
                                property.setType(EntityProperty.PropertyType.getByClass(value.getClass()));
                                property.setValue(value);
                                return property.getProto();
                            }

                            @Override
                            public boolean hasNext() {
                                return keyIter.hasNext();
                            }
                        };
                    }
                };
        builder.addAllProperties(iterable);
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
    public DataEntityType getEntityType() {
        MultiEntityRequestProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getEntityType().name().substring("TYPE_".length()));
    }

    @Override
    public void setEntityType(DataEntityType type) {
        maybeInitBuilder();
        builder.setEntityType(POSUMProtos.EntityTypeProto.valueOf("TYPE_" + type.name()));
    }

    @Override
    public Map<String, Object> getProperties() {
        if (this.properties == null) {
            MultiEntityRequestProtoOrBuilder p = viaProto ? proto : builder;
            this.properties = new HashMap<>(p.getPropertiesCount());
            for (POSUMProtos.EntityPropertyProto propertyProto : p.getPropertiesList()) {
                EntityProperty property = new EntityPropertyPBImpl(propertyProto);
                properties.put(property.getName(), property.getValue());
            }
        }
        return this.properties;
    }

    @Override
    public void setProperties(Map<String, Object> properties) {
        if (properties == null)
            return;
        this.properties = new HashMap<>();
        this.properties.putAll(properties);
    }
}
