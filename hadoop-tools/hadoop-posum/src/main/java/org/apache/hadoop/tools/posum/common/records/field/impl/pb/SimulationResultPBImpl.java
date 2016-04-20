package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.field.CompoundScore;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimulationResultProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimulationResultProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SimulationResultPBImpl extends SimulationResult {
    private SimulationResultProto proto = SimulationResultProto.getDefaultInstance();
    private SimulationResultProto.Builder builder = null;
    private boolean viaProto = false;

    public SimulationResultPBImpl() {
        builder = SimulationResultProto.newBuilder();
    }

    public SimulationResultPBImpl(SimulationResultProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SimulationResultProto getProto() {
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
            builder = SimulationResultProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getPolicyName() {
        SimulationResultProtoOrBuilder p = viaProto ? proto : builder;
        return p.getPolicyName();
    }

    @Override
    public void setPolicyName(String policyName) {
        maybeInitBuilder();
        if (policyName != null)
            builder.setPolicyName(policyName);
    }

    @Override
    public CompoundScore getScore() {
        SimulationResultProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasScore())
            return new CompoundScorePBImpl(p.getScore());
        return null;
    }

    @Override
    public void setScore(CompoundScore score) {
        maybeInitBuilder();
        builder.setScore(((CompoundScorePBImpl) score).getProto());
    }

}