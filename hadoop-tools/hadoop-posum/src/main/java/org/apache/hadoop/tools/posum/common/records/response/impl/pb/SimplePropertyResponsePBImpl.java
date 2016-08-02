package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class SimplePropertyResponsePBImpl extends SimpleResponsePBImpl<SimplePropertyPayload> {

    public SimplePropertyResponsePBImpl() {
        super();
    }

    public SimplePropertyResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(SimplePropertyPayload payload) {
        return ((SimplePropertyPayloadPBImpl) payload).getProto().toByteString();
    }

    @Override
    public SimplePropertyPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new SimplePropertyPayloadPBImpl(POSUMProtos.SimplePropertyPayloadProto.parseFrom(data));
    }
}
