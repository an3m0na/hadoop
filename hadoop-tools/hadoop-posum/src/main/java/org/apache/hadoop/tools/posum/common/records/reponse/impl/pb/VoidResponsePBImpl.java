package org.apache.hadoop.tools.posum.common.records.reponse.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class VoidResponsePBImpl extends SimpleResponsePBImpl<Object> {

    public VoidResponsePBImpl() {
        super();
    }

    public VoidResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(Object payload) {
        return ByteString.EMPTY;
    }

    @Override
    public Object bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
       return null;
    }
}
