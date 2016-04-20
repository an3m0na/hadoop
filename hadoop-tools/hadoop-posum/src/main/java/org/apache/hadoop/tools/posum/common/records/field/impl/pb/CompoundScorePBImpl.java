package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.field.CompoundScore;
import org.apache.hadoop.yarn.proto.POSUMProtos.CompoundScoreProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.CompoundScoreProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class CompoundScorePBImpl extends CompoundScore {
    private CompoundScoreProto proto = CompoundScoreProto.getDefaultInstance();
    private CompoundScoreProto.Builder builder = null;
    private boolean viaProto = false;

    public CompoundScorePBImpl() {
        builder = CompoundScoreProto.newBuilder();
    }

    public CompoundScorePBImpl(CompoundScoreProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public CompoundScoreProto getProto() {
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
            builder = CompoundScoreProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Double getRuntime() {
        CompoundScoreProtoOrBuilder p = viaProto ? proto : builder;
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
        CompoundScoreProtoOrBuilder p = viaProto ? proto : builder;
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
        CompoundScoreProtoOrBuilder p = viaProto ? proto : builder;
        return p.getCost();
    }

    @Override
    public void setCost(Double cost) {
        maybeInitBuilder();
        if (cost != null)
            builder.setCost(cost);
    }
}