package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.HistoryProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HistoryProfileProtoOrBuilder;
import org.bson.types.ObjectId;

/**
 * Created by ane on 3/21/16.
 */
public class HistoryProfilePBImpl<T extends GeneralDataEntity> extends GeneralDataEntityPBImpl<HistoryProfile, HistoryProfileProto, HistoryProfileProto.Builder>
        implements HistoryProfile<T> {

    public HistoryProfilePBImpl() {

    }

    public HistoryProfilePBImpl(DataEntityCollection type, T original) {
        super();
        long timestamp = System.currentTimeMillis();
        setId(ObjectId.get().toHexString());
        setType(type);
        setTimestamp(timestamp);
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
        return "".equals(p.getId()) ? null : p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

    @Override
    public T getOriginal() {
        HistoryProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (p.getOriginal() != null) {
            try {
                Class eClass = getType().getMappedClass();
                return (T) ((GeneralDataEntityPBImpl) eClass.newInstance()).parseToEntity(p.getOriginal());
            } catch (Exception e) {
                throw new POSUMException("Could not read object from byte string " + p.getOriginal(), e);
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
        return DataEntityCollection.valueOf(p.getType().name().substring("TYPE_".length()));
    }

    @Override
    public void setType(DataEntityCollection type) {
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
        if (timestamp != null)
            builder.setTimestamp(timestamp);
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
