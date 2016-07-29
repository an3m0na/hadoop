package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.field.EntityProperty;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.EntityPropertyPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.SearchRequest;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SearchRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SearchRequestProtoOrBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class SearchRequestPBImpl extends SearchRequest {

    private SearchRequestProto proto = SearchRequestProto.getDefaultInstance();
    private SearchRequestProto.Builder builder = null;
    private boolean viaProto = false;

    Map<String, Object> properties;

    public SearchRequestPBImpl() {
        builder = SearchRequestProto.newBuilder();
    }

    public SearchRequestPBImpl(SearchRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SearchRequestProto getProto() {
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
            builder = SearchRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }


    @Override
    public DataEntityDB getEntityDB() {
        SearchRequestProtoOrBuilder p = viaProto ? proto : builder;
        return new DataEntityDBPBImpl(p.getEntityDB());
    }

    @Override
    public void setEntityDB(DataEntityDB db) {
        maybeInitBuilder();
        builder.setEntityDB(((DataEntityDBPBImpl)db).getProto());
    }

    @Override
    public DataEntityCollection getEntityType() {
        SearchRequestProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getEntityType().name().substring("TYPE_".length()));
    }

    @Override
    public void setEntityType(DataEntityCollection type) {
        maybeInitBuilder();
        builder.setEntityType(POSUMProtos.EntityCollectionProto.valueOf("TYPE_" + type.name()));
    }

    @Override
    public Map<String, Object> getProperties() {
        if (this.properties == null) {
            SearchRequestProtoOrBuilder p = viaProto ? proto : builder;
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

    @Override
    public int getLimitOrZero() {
        SearchRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getLimit();
    }

    @Override
    public void setLimitOrZero(int limitOrZero) {
        maybeInitBuilder();
        builder.setLimit(limitOrZero);
    }

    @Override
    public int getOffsetOrZero() {
        SearchRequestProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOffset();
    }

    @Override
    public void setOffsetOrZero(int offsetOrZero) {
        maybeInitBuilder();
        builder.setOffset(offsetOrZero);
    }
}
