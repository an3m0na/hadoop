package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.field.JobForAppPayload;
import org.apache.hadoop.tools.posum.common.records.field.SaveFlexFieldsPayload;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.JobForAppPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.SaveFlexFieldsPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class SaveFlexFieldsRequestPBImpl extends  SimpleRequestPBImpl<SaveFlexFieldsPayload>  {
    public SaveFlexFieldsRequestPBImpl() {
        super();
    }

    public SaveFlexFieldsRequestPBImpl(POSUMProtos.SimpleRequestProto proto) {
        super(proto);
    }


    @Override
    public ByteString payloadToBytes(SaveFlexFieldsPayload payload) {
        return ((SaveFlexFieldsPayloadPBImpl)payload).getProto().toByteString();
    }

    @Override
    public SaveFlexFieldsPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new SaveFlexFieldsPayloadPBImpl(POSUMProtos.SaveFlexFieldsPayloadProto.parseFrom(data));
    }
}