package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.records.call.FindByParamsCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.ByParamsProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.ByParamsProtoOrBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class FindByParamsCallPBImpl extends FindByParamsCall implements PayloadPB {
    private ByParamsProto proto = ByParamsProto.getDefaultInstance();
    private ByParamsProto.Builder builder = null;
    private boolean viaProto = false;

    private Map<String, Object> params;

    public FindByParamsCallPBImpl() {
        builder = ByParamsProto.newBuilder();
    }

    public FindByParamsCallPBImpl(ByParamsProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public ByParamsProto getProto() {
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
        if (params == null)
            return;
        Iterable<POSUMProtos.SimplePropertyPayloadProto> iterable =
                new Iterable<POSUMProtos.SimplePropertyPayloadProto>() {

                    @Override
                    public Iterator<POSUMProtos.SimplePropertyPayloadProto> iterator() {
                        return new Iterator<POSUMProtos.SimplePropertyPayloadProto>() {

                            Iterator<String> keyIter = params.keySet().iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public POSUMProtos.SimplePropertyPayloadProto next() {
                                String key = keyIter.next();
                                Object value = params.get(key);
                                SimplePropertyPayloadPBImpl property = new SimplePropertyPayloadPBImpl();
                                property.setName(key);
                                property.setType(SimplePropertyPayload.PropertyType.getByClass(value.getClass()));
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
            builder = ByParamsProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityDB getEntityDB() {
        ByParamsProtoOrBuilder p = viaProto ? proto : builder;
        return new DataEntityDBPBImpl(p.getEntityDB());
    }

    @Override
    public void setEntityDB(DataEntityDB db) {
        maybeInitBuilder();
        builder.setEntityDB(((DataEntityDBPBImpl) db).getProto());
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        ByParamsProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getCollection().name().substring("COLL_".length()));
    }

    @Override
    public void setEntityCollection(DataEntityCollection type) {
        if (type == null)
            return;
        maybeInitBuilder();
        builder.setCollection(POSUMProtos.EntityCollectionProto.valueOf("COLL_" + type.name()));
    }

    @Override
    public Map<String, Object> getParams() {
        if (this.params == null) {
            ByParamsProtoOrBuilder p = viaProto ? proto : builder;
            this.params = new HashMap<>(p.getPropertiesCount());
            for (POSUMProtos.SimplePropertyPayloadProto propertyProto : p.getPropertiesList()) {
                SimplePropertyPayload property = new SimplePropertyPayloadPBImpl(propertyProto);
                params.put(property.getName(), property.getValue());
            }
        }
        return this.params;
    }

    @Override
    public void setParams(Map<String, Object> properties) {
        if (properties == null)
            return;
        this.params = new HashMap<>();
        this.params.putAll(properties);
    }

    @Override
    public int getLimitOrZero() {
        ByParamsProtoOrBuilder p = viaProto ? proto : builder;
        return p.getLimit();
    }

    @Override
    public void setLimitOrZero(int limitOrZero) {
        maybeInitBuilder();
        builder.setLimit(limitOrZero);
    }

    @Override
    public int getOffsetOrZero() {
        ByParamsProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOffset();
    }

    @Override
    public void setOffsetOrZero(int offsetOrZero) {
        maybeInitBuilder();
        builder.setOffset(offsetOrZero);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = ByParamsProto.parseFrom(data);
        viaProto = true;
    }
}