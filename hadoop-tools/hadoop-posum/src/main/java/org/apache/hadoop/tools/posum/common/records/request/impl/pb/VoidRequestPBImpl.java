package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class VoidRequestPBImpl extends SimpleRequestPBImpl<VoidPayload> {

    public VoidRequestPBImpl() {
        super();
    }

    public VoidRequestPBImpl(POSUMProtos.SimpleRequestProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(VoidPayload payload) {
        return ByteString.EMPTY;
    }

    @Override
    public VoidPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return null;
    }
}
