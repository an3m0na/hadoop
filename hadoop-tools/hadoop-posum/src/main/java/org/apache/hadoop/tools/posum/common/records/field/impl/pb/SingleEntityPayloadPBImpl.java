package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.records.field.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityPayloadPBImpl extends SingleEntityPayload {
    private SingleEntityPayloadProto proto = SingleEntityPayloadProto.getDefaultInstance();
    private SingleEntityPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public SingleEntityPayloadPBImpl() {
        builder = SingleEntityPayloadProto.newBuilder();
    }

    public SingleEntityPayloadPBImpl(SingleEntityPayloadProto proto) {
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
    public DataEntityCollection getEntityType() {
        SingleEntityPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getEntityType().name().substring("TYPE_".length()));
    }

    @Override
    public void setEntityType(DataEntityCollection type) {
        maybeInitBuilder();
        builder.setEntityType(POSUMProtos.EntityCollectionProto.valueOf("TYPE_" + type.name()));
    }

    @Override
    public GeneralDataEntity getEntity() {
        SingleEntityPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasEntity()) {
            try {
                Class eClass = getEntityType().getMappedClass();
                return ((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(p.getEntity());
            } catch (Exception e) {
                throw new POSUMException("Could not read object from byte string " + p.getEntity(), e);
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
