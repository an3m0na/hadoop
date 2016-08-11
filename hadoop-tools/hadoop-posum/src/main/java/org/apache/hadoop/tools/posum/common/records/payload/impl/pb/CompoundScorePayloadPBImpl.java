package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.POSUMProtos.CompoundScorePayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.CompoundScorePayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class CompoundScorePayloadPBImpl extends CompoundScorePayload implements PayloadPB {
    private CompoundScorePayloadProto proto = CompoundScorePayloadProto.getDefaultInstance();
    private CompoundScorePayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public CompoundScorePayloadPBImpl() {
        builder = CompoundScorePayloadProto.newBuilder();
    }

    public CompoundScorePayloadPBImpl(CompoundScorePayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public CompoundScorePayloadProto getProto() {
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
            builder = CompoundScorePayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Double getRuntime() {
        CompoundScorePayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getRuntime();
    }

    @Override
    public void setRuntime(Double runtime) {
        maybeInitBuilder();
        if (runtime != null)
            builder.setRuntime(runtime);
    }

    @Override
    public Double getPenalty() {
        CompoundScorePayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getPenalty();
    }

    @Override
    public void setPenalty(Double penalty) {
        maybeInitBuilder();
        if (penalty != null)
            builder.setPenalty(penalty);
    }

    @Override
    public Double getCost() {
        CompoundScorePayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getCost();
    }

    @Override
    public void setCost(Double cost) {
        maybeInitBuilder();
        if (cost != null)
            builder.setCost(cost);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = CompoundScorePayloadProto.parseFrom(data);
        viaProto = true;
    }
}