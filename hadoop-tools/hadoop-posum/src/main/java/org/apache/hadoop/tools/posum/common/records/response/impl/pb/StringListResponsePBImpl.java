package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringListPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class StringListResponsePBImpl extends SimpleResponsePBImpl<StringListPayload> {

    public StringListResponsePBImpl() {
        super();
    }

    public StringListResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(StringListPayload payload) {
        return ((StringListPayloadPBImpl) payload).getProto().toByteString();
    }

    @Override
    public StringListPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new StringListPayloadPBImpl(POSUMProtos.StringListPayloadProto.parseFrom(data));
    }
}
