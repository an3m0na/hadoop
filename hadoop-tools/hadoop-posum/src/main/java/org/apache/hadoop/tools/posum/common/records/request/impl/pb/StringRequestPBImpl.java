package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Created by ane on 3/20/16.
 */
public class StringRequestPBImpl extends SimpleRequestPBImpl<String> {

    @Override
    public ByteString payloadToBytes(String payload) {
        return ByteString.copyFromUtf8(payload);
    }

    @Override
    public String bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return data.toStringUtf8();
    }
}
