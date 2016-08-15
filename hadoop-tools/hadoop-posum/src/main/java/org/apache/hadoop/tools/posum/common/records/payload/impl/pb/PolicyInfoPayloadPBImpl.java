package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.PolicyInfoPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PolicyInfoPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class PolicyInfoPayloadPBImpl extends PolicyInfoPayload implements PayloadPB {
    private PolicyInfoPayloadProto proto = PolicyInfoPayloadProto.getDefaultInstance();
    private PolicyInfoPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public PolicyInfoPayloadPBImpl() {
        builder = PolicyInfoPayloadProto.newBuilder();
    }

    public PolicyInfoPayloadPBImpl(PolicyInfoPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public PolicyInfoPayloadProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(getProto());
    }

    private void mergeLocalToBuilder() {

    }

    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = PolicyInfoPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    public Integer getUsageNumber() {
        PolicyInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasUsageNumber())
            return null;
        return p.getUsageNumber();

    }

    public void setUsageNumber(Integer usageNumber) {
        maybeInitBuilder();
        if (usageNumber == null) {
            builder.clearUsageNumber();
            return;
        }
        builder.setUsageNumber(usageNumber);
    }

    public Long getUsageTime() {
        PolicyInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasUsageTime())
            return null;
        return p.getUsageTime();
    }

    public void setUsageTime(Long usageTime) {
        maybeInitBuilder();
        if (usageTime == null) {
            builder.clearUsageTime();
            return;
        }
        builder.setUsageTime(usageTime);
    }

    public Long getLastStarted() {
        PolicyInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasLastStarted())
            return null;
        return p.getLastStarted();
    }

    public void setLastStarted(Long lastStarted) {
        maybeInitBuilder();
        if (lastStarted == null) {
            builder.clearLastStarted();
            return;
        }
        builder.setLastStarted(lastStarted);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = PolicyInfoPayloadProto.parseFrom(data);
        viaProto = true;
    }

}