package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.yarn.proto.PosumProtos;

public class SimplePropertyRequestPBImpl extends SimpleRequestPBImpl<SimplePropertyPayload> {

    public SimplePropertyRequestPBImpl(){
        super();
    }

    public SimplePropertyRequestPBImpl(PosumProtos.SimpleRequestProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(SimplePropertyPayload payload) {
        return ((SimplePropertyPayloadPBImpl)payload).getProto().toByteString();
    }

    @Override
    public SimplePropertyPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        SimplePropertyPayloadPBImpl ret = new SimplePropertyPayloadPBImpl();
        ret.populateFromProtoBytes(data);
        return ret;
    }
}
