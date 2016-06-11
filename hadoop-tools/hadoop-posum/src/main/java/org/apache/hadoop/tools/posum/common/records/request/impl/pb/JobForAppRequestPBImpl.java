package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.field.JobForAppPayload;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.JobForAppPayloadPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class JobForAppRequestPBImpl extends  SimpleRequestPBImpl<JobForAppPayload>  {
    public JobForAppRequestPBImpl() {
        super();
    }

    public JobForAppRequestPBImpl(POSUMProtos.SimpleRequestProto proto) {
        super(proto);
    }


    @Override
    public ByteString payloadToBytes(JobForAppPayload payload) {
        return ((JobForAppPayloadPBImpl)payload).getProto().toByteString();
    }

    @Override
    public JobForAppPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        return new JobForAppPayloadPBImpl(POSUMProtos.JobForAppPayloadProto.parseFrom(data));
    }
}