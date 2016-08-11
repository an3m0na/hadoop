package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;

/**
 * Created by ane on 3/20/16.
 */
public class VoidPayloadPBImpl extends VoidPayload implements PayloadPB {

    public VoidPayloadPBImpl() {

    }

    @Override
    public ByteString getProtoBytes() {
        return ByteString.EMPTY;
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {

    }


}