package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.request.EntityByIdPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class EntityByIdRequestPBImpl extends  SimpleRequestPBImpl<EntityByIdPayload>  {
    public EntityByIdRequestPBImpl() {
        super();
    }

    public EntityByIdRequestPBImpl(POSUMProtos.SimpleRequestProto proto) {
        super(proto);
    }


    @Override
    public ByteString payloadToBytes(EntityByIdPayload payload) {
        return ((EntityByIdPayloadPBImpl)payload).getProto().toByteString();
    }

    @Override
    public EntityByIdPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new EntityByIdPayloadPBImpl(POSUMProtos.EntityByIdPayloadProto.parseFrom(data));
    }
}