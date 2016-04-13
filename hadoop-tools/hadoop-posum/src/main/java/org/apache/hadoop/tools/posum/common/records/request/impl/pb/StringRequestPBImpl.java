package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class StringRequestPBImpl extends SimpleRequestPBImpl<String> {

    public StringRequestPBImpl(){
        super();
    }

    public StringRequestPBImpl(POSUMProtos.SimpleRequestProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(String payload) {
        return ByteString.copyFromUtf8(payload);
    }

    @Override
    public String bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return data.toStringUtf8();
    }
}
