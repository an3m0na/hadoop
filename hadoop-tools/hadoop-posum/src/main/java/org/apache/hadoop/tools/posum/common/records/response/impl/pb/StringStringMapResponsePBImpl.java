package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class StringStringMapResponsePBImpl extends SimpleResponsePBImpl<StringStringMapPayload> {

    public StringStringMapResponsePBImpl() {
        super();
    }

    public StringStringMapResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(StringStringMapPayload payload) {
        return ((StringStringMapPayloadPBImpl) payload).getProto().toByteString();
    }

    @Override
    public StringStringMapPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new StringStringMapPayloadPBImpl(POSUMProtos.StringStringMapPayloadProto.parseFrom(data));
    }
}
