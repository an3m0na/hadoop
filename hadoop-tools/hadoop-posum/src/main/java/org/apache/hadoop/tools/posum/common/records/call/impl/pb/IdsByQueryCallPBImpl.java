package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.impl.pb.DatabaseQueryWrapperPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;

/**
 * Created by ane on 3/20/16.
 */
public class IdsByQueryCallPBImpl extends IdsByQueryCall implements PayloadPB {
    private PosumProtos.ByQueryCallProto proto = PosumProtos.ByQueryCallProto.getDefaultInstance();
    private PosumProtos.ByQueryCallProto.Builder builder = null;
    private boolean viaProto = false;

    public IdsByQueryCallPBImpl() {
        builder = PosumProtos.ByQueryCallProto.newBuilder();
    }

    public IdsByQueryCallPBImpl(PosumProtos.ByQueryCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public PosumProtos.ByQueryCallProto getProto() {
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
            builder = PosumProtos.ByQueryCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        PosumProtos.ByQueryCallProtoOrBuilder p = viaProto ? proto : builder;
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
    public DatabaseQuery getQuery() {
        PosumProtos.ByQueryCallProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasQuery())
            return null;
        return new DatabaseQueryWrapperPBImpl(p.getQuery()).getQuery();
    }

    @Override
    public void setQuery(DatabaseQuery query) {
        maybeInitBuilder();
        if (query == null) {
            builder.clearQuery();
            return;
        }
        builder.setQuery(new DatabaseQueryWrapperPBImpl(query).getProto());
    }

    @Override
    public Integer getLimitOrZero() {
        PosumProtos.ByQueryCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getLimit();
    }

    @Override
    public void setLimitOrZero(int limitOrZero) {
        maybeInitBuilder();
        builder.setLimit(limitOrZero);
    }

    @Override
    public Integer getOffsetOrZero() {
        PosumProtos.ByQueryCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOffset();
    }

    @Override
    public void setOffsetOrZero(int offsetOrZero) {
        maybeInitBuilder();
        builder.setOffset(offsetOrZero);
    }

    @Override
    public String getSortField() {
        PosumProtos.ByQueryCallProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasSortField())
            return null;
        return p.getSortField();
    }

    @Override
    public void setSortField(String field) {
        maybeInitBuilder();
        if (field == null) {
            builder.clearSortField();
            return;
        }
        builder.setSortField(field);
    }

    @Override
    public Boolean getSortDescending() {
        PosumProtos.ByQueryCallProtoOrBuilder p = viaProto ? proto : builder;
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
        this.proto = PosumProtos.ByQueryCallProto.parseFrom(data);
        viaProto = true;
    }
}