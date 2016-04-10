package org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Created by ane on 3/20/16.
 */
public class VoidResponsePBImpl extends SimpleResponsePBImpl<Object> {

    @Override
    public ByteString payloadToBytes(Object payload) {
        return ByteString.EMPTY;
    }

    @Override
    public Object bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
       return null;
    }
}
