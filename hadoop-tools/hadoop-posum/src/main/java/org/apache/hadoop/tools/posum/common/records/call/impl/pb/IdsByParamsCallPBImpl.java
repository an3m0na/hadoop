package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.IdsByParamsCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.ByParamsCallProto;
import org.apache.hadoop.yarn.proto.PosumProtos.ByParamsCallProtoOrBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class IdsByParamsCallPBImpl extends IdsByParamsCall implements PayloadPB {
    private ByParamsCallProto proto = ByParamsCallProto.getDefaultInstance();
    private ByParamsCallProto.Builder builder = null;
    private boolean viaProto = false;

    private Map<String, Object> params;

    public IdsByParamsCallPBImpl() {
        builder = ByParamsCallProto.newBuilder();
    }

    public IdsByParamsCallPBImpl(ByParamsCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public ByParamsCallProto getProto() {
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
        Iterable<PosumProtos.SimplePropertyPayloadProto> iterable =
                new Iterable<PosumProtos.SimplePropertyPayloadProto>() {

                    @Override
                    public Iterator<PosumProtos.SimplePropertyPayloadProto> iterator() {
                        return new Iterator<PosumProtos.SimplePropertyPayloadProto>() {

                            Iterator<String> keyIter = params.keySet().iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public PosumProtos.SimplePropertyPayloadProto next() {
                                String key = keyIter.next();
                                Object value = params.get(key);
                                SimplePropertyPayloadPBImpl property =
                                        (SimplePropertyPayloadPBImpl) SimplePropertyPayload.newInstance(key, value);
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
            builder = ByParamsCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        ByParamsCallProtoOrBuilder p = viaProto ? proto : builder;
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
    public Map<String, Object> getParams() {
        if (this.params == null) {
            ByParamsCallProtoOrBuilder p = viaProto ? proto : builder;
            this.params = new HashMap<>(p.getPropertiesCount());
            for (PosumProtos.SimplePropertyPayloadProto propertyProto : p.getPropertiesList()) {
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
    public Integer getLimitOrZero() {
        ByParamsCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getLimit();
    }

    @Override
    public void setLimitOrZero(int limitOrZero) {
        maybeInitBuilder();
        builder.setLimit(limitOrZero);
    }

    @Override
    public Integer getOffsetOrZero() {
        ByParamsCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOffset();
    }

    @Override
    public void setOffsetOrZero(int offsetOrZero) {
        maybeInitBuilder();
        builder.setOffset(offsetOrZero);
    }

    @Override
    public String getSortField() {
        ByParamsCallProtoOrBuilder p = viaProto ? proto : builder;
        if(!p.hasSortField())
            return null;
        return p.getSortField();
    }

    @Override
    public void setSortField(String field) {
        maybeInitBuilder();
        if(field == null){
            builder.clearSortField();
            return;
        }
        builder.setSortField(field);
    }

    @Override
    public Boolean getSortDescending() {
        ByParamsCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getSortDescending();
    }

    @Override
    public void setSortDescending(boolean descending) {
        maybeInitBuilder();
        builder.setSortDescending(descending);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = ByParamsCallProto.parseFrom(data);
        viaProto = true;
    }
}