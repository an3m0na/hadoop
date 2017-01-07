package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.HistoryProfileProto;
import org.apache.hadoop.yarn.proto.PosumProtos.HistoryProfileProtoOrBuilder;
import org.bson.types.ObjectId;

public class HistoryProfilePBImpl<T extends GeneralDataEntity> extends GeneralDataEntityPBImpl<HistoryProfile, HistoryProfileProto, HistoryProfileProto.Builder>
        implements HistoryProfile<T> {

    public HistoryProfilePBImpl() {

    }

    public HistoryProfilePBImpl(HistoryProfileProto proto) {
        super(proto);
    }

    public HistoryProfilePBImpl(DataEntityCollection type, T original) {
        super();
        setId(ObjectId.get().toHexString());
        setType(type);
        setOriginal(original);
        setOriginalId(original.getId());
    }

    @Override
    void initBuilder() {
        builder = viaProto ? HistoryProfileProto.newBuilder(proto) : HistoryProfileProto.newBuilder();
    }

    @Override
    void buildProto() {
        proto = builder.build();
    }

    @Override
    public HistoryProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = HistoryProfileProto.parseFrom(data);
        viaProto = true;
        return this;
    }

    @Override
    public String getId() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasId())
            return null;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id == null) {
            builder.clearId();
            return;
        }
        builder.setId(id);
    }

    @Override
    public Long getLastUpdated() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasLastUpdated())
            return p.getLastUpdated();
        return null;
    }

    @Override
    public void setLastUpdated(Long timestamp) {
        maybeInitBuilder();
        if (timestamp == null) {
            builder.clearLastUpdated();
            return;
        }
        builder.setLastUpdated(timestamp);
    }

    @Override
    public HistoryProfile copy() {
        return new HistoryProfilePBImpl(getProto());
    }

    @Override
    public T getOriginal() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (p.getOriginal() != null) {
            try {
                Class eClass = getType().getMappedClass();
                return (T) ((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(p.getOriginal());
            } catch (Exception e) {
                throw new PosumException("Could not read object from byte string " + p.getOriginal(), e);
            }
        }
        return null;
    }

    @Override
    public void setOriginal(T original) {
        maybeInitBuilder();
        if (original != null)
            builder.setOriginal(((GeneralDataEntityPBImpl) original).getProto().toByteString());
    }

    @Override
    public DataEntityCollection getType() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityCollection.valueOf(p.getCollection().name().substring("COLL_".length()));
    }

    @Override
    public void setType(DataEntityCollection type) {
        maybeInitBuilder();
        builder.setCollection(PosumProtos.EntityCollectionProto.valueOf("COLL_" + type.name()));
    }

    @Override
    public String getOriginalId() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOriginalId();
    }

    @Override
    public void setOriginalId(String originalId) {
        maybeInitBuilder();
        if (originalId != null)
            builder.setOriginalId(originalId);
    }
}
