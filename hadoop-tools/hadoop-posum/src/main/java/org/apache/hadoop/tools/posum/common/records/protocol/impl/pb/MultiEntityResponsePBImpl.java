package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.MultiEntityResponse;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityResponseProtoOrBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public class MultiEntityResponsePBImpl extends MultiEntityResponse {
    private MultiEntityResponseProto proto = MultiEntityResponseProto.getDefaultInstance();
    private MultiEntityResponseProto.Builder builder = null;
    private boolean viaProto = false;

    public MultiEntityResponsePBImpl() {
        builder = MultiEntityResponseProto.newBuilder();
    }

    public MultiEntityResponsePBImpl(MultiEntityResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public MultiEntityResponseProto getProto() {
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
            builder = MultiEntityResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityType getType() {
        MultiEntityResponseProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataEntityType type) {
        maybeInitBuilder();
        builder.setType(POSUMProtos.EntityTypeProto.valueOf("TYPE_" + type.name()));
    }

    @Override
    public List<GeneralDataEntity> getEntities() {
        MultiEntityResponseProtoOrBuilder p = viaProto ? proto : builder;
        List<GeneralDataEntity> entities = new ArrayList<>(p.getEntitiesCount());
        for (ByteString entityString : p.getEntitiesList()) {
            if (entityString != null) {
                try {
                    Class eClass = getType().getMappedClass();
                    entities.add(((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(entityString));
                } catch (Exception e) {
                    throw new POSUMException("Could not read object from byte string " + entityString, e);
                }
            }
        }
        return entities;
    }

    @Override
    public void setEntities(List<GeneralDataEntity> entities) {
        maybeInitBuilder();
        for (GeneralDataEntity entity : entities) {
            builder.addEntities(((GeneralDataEntityPBImpl) entity).getProto().toByteString());
        }
    }
}
