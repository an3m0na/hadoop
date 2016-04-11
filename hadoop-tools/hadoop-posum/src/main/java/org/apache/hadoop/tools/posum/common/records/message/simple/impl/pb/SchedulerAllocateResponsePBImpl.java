package org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.message.simple.SchedulerAllocatePayload;
import org.apache.hadoop.yarn.proto.POSUMProtos.SchedulerAllocatePayloadProto;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SchedulerAllocateResponsePBImpl extends SimpleResponsePBImpl<Allocation> {
    @Override
    public ByteString payloadToBytes(Allocation payload) {
        SchedulerAllocatePayload payloadImpl = SchedulerAllocatePayload.newInstance(payload);
        return ((SchedulerAllocatePayloadPBImpl) payloadImpl).getProto().toByteString();
    }

    @Override
    public Allocation bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        SchedulerAllocatePayloadProto proto = SchedulerAllocatePayloadProto.parseFrom(data);
        return new SchedulerAllocatePayloadPBImpl(proto).getAllocation();
    }
}
