package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleEntityResponse;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityResponseProtoOrBuilder;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityResponsePBImpl extends SingleEntityResponse {
    SingleEntityResponseProto proto = SingleEntityResponseProto.getDefaultInstance();
    SingleEntityResponseProto.Builder builder = null;
    boolean viaProto = false;

    public SingleEntityResponsePBImpl() {
        builder = SingleEntityResponseProto.newBuilder();
    }

    public SingleEntityResponsePBImpl(SingleEntityResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleEntityResponseProto getProto() {
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
            builder = SingleEntityResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityType getType() {
        SingleEntityResponseProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataEntityType type) {
        maybeInitBuilder();
        builder.setType(POSUMProtos.EntityType.valueOf("TYPE_" + type.name()));
    }

    @Override
    public GeneralDataEntity getEntity() {
        SingleEntityResponseProtoOrBuilder p = viaProto ? proto : builder;
        if (p.getEntity() != null) {
            try {
                Class eClass = DataEntityType.valueOf(p.getType().name().substring("TYPE_".length())).getMappedClass();
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
