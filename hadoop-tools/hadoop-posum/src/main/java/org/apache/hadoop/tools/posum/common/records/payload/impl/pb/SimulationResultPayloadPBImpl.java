package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimulationResultPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimulationResultPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SimulationResultPayloadPBImpl extends SimulationResultPayload {
    private SimulationResultPayloadProto proto = SimulationResultPayloadProto.getDefaultInstance();
    private SimulationResultPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public SimulationResultPayloadPBImpl() {
        builder = SimulationResultPayloadProto.newBuilder();
    }

    public SimulationResultPayloadPBImpl(SimulationResultPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SimulationResultPayloadProto getProto() {
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
            builder = SimulationResultPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getPolicyName() {
        SimulationResultPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getPolicyName();
    }

    @Override
    public void setPolicyName(String policyName) {
        maybeInitBuilder();
        if (policyName != null)
            builder.setPolicyName(policyName);
    }

    @Override
    public CompoundScorePayload getScore() {
        SimulationResultPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasScore())
            return new CompoundScorePayloadPBImpl(p.getScore());
        return null;
    }

    @Override
    public void setScore(CompoundScorePayload score) {
        maybeInitBuilder();
        builder.setScore(((CompoundScorePayloadPBImpl) score).getProto());
    }

}