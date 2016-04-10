package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.yarn.proto.POSUMProtos.ConfigurationPropertiesProto;
import org.apache.hadoop.yarn.proto.YarnProtos;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
