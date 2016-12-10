package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.SingleEntityPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SingleEntityPayloadProtoOrBuilder;

public class StoreCallPBImpl extends StoreCall implements PayloadPB {
    private SingleEntityPayloadProto proto = SingleEntityPayloadProto.getDefaultInstance();
    private SingleEntityPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public StoreCallPBImpl() {
        builder = SingleEntityPayloadProto.newBuilder();
    }

    public StoreCallPBImpl(SingleEntityPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleEntityPayloadProto getProto() {
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
            builder = SingleEntityPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        SingleEntityPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getCollection().name().substring("COLL_".length()));
    }

    @Override
    public void setEntityCollection(DataEntityCollection type) {
        maybeInitBuilder();
        builder.setCollection(PosumProtos.EntityCollectionProto.valueOf("COLL_" + type.name()));
    }

    @Override
    public GeneralDataEntity getEntity() {
        SingleEntityPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasEntity()) {
            try {
                Class eClass = getEntityCollection().getMappedClass();
                return ((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(p.getEntity());
            } catch (Exception e) {
                throw new PosumException("Could not read object from byte string " + p.getEntity(), e);
            }
        }
        return null;
    }

    @Override
    public void setEntity(GeneralDataEntity entity) {
        maybeInitBuilder();
        if (entity != null)
            builder.setEntity(((GeneralDataEntityPBImpl) entity).getProto().toByteString());
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = SingleEntityPayloadProto.parseFrom(data);
        viaProto = true;
    }
}
