package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.impl.pb.FindByIdCallPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class EntityByIdRequestPBImpl extends  SimpleRequestPBImpl<FindByIdCall>  {
    public EntityByIdRequestPBImpl() {
        super();
    }

    public EntityByIdRequestPBImpl(POSUMProtos.SimpleRequestProto proto) {
        super(proto);
    }


    @Override
    public ByteString payloadToBytes(FindByIdCall payload) {
        return ((FindByIdCallPBImpl)payload).getProto().toByteString();
    }

    @Override
    public FindByIdCall bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new FindByIdCallPBImpl(POSUMProtos.ByIdProto.parseFrom(data));
    }
}