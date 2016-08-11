package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.TaskPredictionPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.TaskPredictionPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.TaskPredictionPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class TaskPredictionPayloadPBImpl extends TaskPredictionPayload implements PayloadPB {
    private TaskPredictionPayloadProto proto = TaskPredictionPayloadProto.getDefaultInstance();
    private TaskPredictionPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public TaskPredictionPayloadPBImpl() {
        builder = TaskPredictionPayloadProto.newBuilder();
    }

    public TaskPredictionPayloadPBImpl(TaskPredictionPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    @JsonIgnore
    @org.codehaus.jackson.annotate.JsonIgnore
    public TaskPredictionPayloadProto getProto() {
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
            builder = TaskPredictionPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }


    @Override
    public String getId() {
        TaskPredictionPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setPredictor(String predictor) {
        maybeInitBuilder();
        builder.setPredictor(predictor);
    }

    @Override
    public String getPredictor() {
        TaskPredictionPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getPredictor();
    }

    @Override
    public void setId(String name) {
        maybeInitBuilder();
        builder.setId(name);
    }

    @Override
    public Long getDuration() {
        TaskPredictionPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getDuration();
    }

    @Override
    public void setDuration(Long duration) {
        maybeInitBuilder();
        builder.setDuration(duration);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = TaskPredictionPayloadProto.parseFrom(data);
        viaProto = true;
    }
}