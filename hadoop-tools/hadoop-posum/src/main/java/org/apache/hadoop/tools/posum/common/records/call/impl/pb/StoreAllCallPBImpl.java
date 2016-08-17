package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.MultiEntityPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.MultiEntityPayloadProto;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public class StoreAllCallPBImpl extends StoreAllCall implements PayloadPB {
    private MultiEntityPayloadPBImpl multiEntityPayloadPB;

    public StoreAllCallPBImpl() {
        multiEntityPayloadPB = new MultiEntityPayloadPBImpl();
    }

    public StoreAllCallPBImpl(MultiEntityPayloadProto proto) {
        multiEntityPayloadPB = new MultiEntityPayloadPBImpl(proto);
    }

    public MultiEntityPayloadProto getProto() {
        return multiEntityPayloadPB.getProto();
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

    @Override
    public DataEntityCollection getEntityCollection() {
        return multiEntityPayloadPB.getEntityCollection();
    }

    @Override
    public void setEntityCollection(DataEntityCollection type) {
        multiEntityPayloadPB.setEntityCollection(type);
    }

    @Override
    public List<? extends GeneralDataEntity> getEntities() {
        return multiEntityPayloadPB.getEntities();
    }

    @Override
    public void setEntities(List<? extends GeneralDataEntity> entities) {
        multiEntityPayloadPB.setEntities((List<GeneralDataEntity>)entities);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        multiEntityPayloadPB.populateFromProtoBytes(data);
    }
}
