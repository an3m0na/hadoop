package org.apache.hadoop.tools.posum.common.records.message.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.message.request.impl.pb.SimpleRequestPBImpl;

/**
 * Created by ane on 3/20/16.
 */
public class VoidRequestPBImpl extends SimpleRequestPBImpl<Object> {

    @Override
    public ByteString payloadToBytes(Object payload) {
        return ByteString.EMPTY;
    }

    @Override
    public Object bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
       return null;
    }
}
