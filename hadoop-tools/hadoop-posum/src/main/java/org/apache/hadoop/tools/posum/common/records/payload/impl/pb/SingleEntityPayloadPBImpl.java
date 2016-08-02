package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityPayloadPBImpl extends SingleEntityPayload {
    private SingleEntityProto proto = SingleEntityProto.getDefaultInstance();
    private SingleEntityProto.Builder builder = null;
    private boolean viaProto = false;

    public SingleEntityPayloadPBImpl() {
        builder = SingleEntityProto.newBuilder();
    }

    public SingleEntityPayloadPBImpl(SingleEntityProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleEntityProto getProto() {
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
            builder = SingleEntityProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityCollection getEntityCollection() {
        SingleEntityProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getCollection().name().substring("COLL_".length()));
    }

    @Override
    public void setEntityCollection(DataEntityCollection type) {
        maybeInitBuilder();
        builder.setCollection(POSUMProtos.EntityCollectionProto.valueOf("COLL_" + type.name()));
    }

    @Override
    public GeneralDataEntity getEntity() {
        SingleEntityProtoOrBuilder p = viaProto ? proto : builder;
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
}
