package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.field.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.SingleEntityPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityResponsePBImpl extends SimpleResponsePBImpl<SingleEntityPayload> {

    public SingleEntityResponsePBImpl() {
        super();
    }

    public SingleEntityResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(SingleEntityPayload payload) {
        return ((SingleEntityPayloadPBImpl)payload).getProto().toByteString();
    }

    @Override
    public SingleEntityPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new SingleEntityPayloadPBImpl(POSUMProtos.SingleEntityPayloadProto.parseFrom(data));
    }
}
