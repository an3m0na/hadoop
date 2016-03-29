package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.HistoryProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HistoryProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class HistoryProfilePBImpl extends HistoryProfile implements GeneralDataEntityPBImpl<HistoryProfile, HistoryProfileProto> {
    private HistoryProfileProto proto = HistoryProfileProto.getDefaultInstance();
    private HistoryProfileProto.Builder builder = null;
    private boolean viaProto = false;

    public HistoryProfilePBImpl() {
        builder = HistoryProfileProto.newBuilder();
    }

    public HistoryProfilePBImpl(HistoryProfileProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    @JsonIgnore
    public HistoryProfileProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public HistoryProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = HistoryProfileProto.parseFrom(data);
        viaProto = true;
        return this;
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
            builder = HistoryProfileProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public GeneralDataEntity getOriginal() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (p.getOriginal() != null) {
            try {
                Class eClass = getType().getMappedClass();
                return ((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(p.getOriginal());
            } catch (Exception e) {
                throw new POSUMException("Could not read object from byte string " + p.getOriginal(), e);
            }
        }
        return null;
    }

    @Override
    public void setOriginal(GeneralDataEntity original) {
        maybeInitBuilder();
        if (original != null)
            builder.setOriginal(((GeneralDataEntityPBImpl) original).getProto().toByteString());
    }

    @Override
    public DataEntityType getType() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityType.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataEntityType type) {
        maybeInitBuilder();
        builder.setType(POSUMProtos.EntityTypeProto.valueOf("TYPE_" + type.name()));
    }

    @Override
    public Long getTimestamp() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getTimestamp();
    }

    @Override
    public void setTimestamp(Long timestamp) {
        maybeInitBuilder();
        builder.setTimestamp(timestamp);
    }

    @Override
    public String getId() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

    @Override
    public String getOriginalId() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOriginalId();
    }

    @Override
    public void setOriginalId(String originalId) {
        maybeInitBuilder();
        builder.setOriginalId(originalId);
    }
}
