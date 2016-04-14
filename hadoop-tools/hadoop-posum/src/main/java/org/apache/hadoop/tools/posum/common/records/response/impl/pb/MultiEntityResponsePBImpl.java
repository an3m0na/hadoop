package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.response.MultiEntityPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class MultiEntityResponsePBImpl extends SimpleResponsePBImpl<MultiEntityPayload> {

    public MultiEntityResponsePBImpl() {
        super();
    }

    public MultiEntityResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(MultiEntityPayload payload) {
        return ((MultiEntityPayloadPBImpl)payload).getProto().toByteString();
    }

    @Override
    public MultiEntityPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
       return null;
    }
}
