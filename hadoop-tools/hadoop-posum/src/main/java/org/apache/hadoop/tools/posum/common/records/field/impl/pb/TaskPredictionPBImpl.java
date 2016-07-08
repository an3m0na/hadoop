package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.field.EntityProperty;
import org.apache.hadoop.tools.posum.common.records.field.TaskPrediction;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.proto.POSUMProtos.TaskPredictionProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.TaskPredictionProtoOrBuilder;

import java.io.IOException;

/**
 * Created by ane on 3/20/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class TaskPredictionPBImpl extends TaskPrediction {
    private TaskPredictionProto proto = TaskPredictionProto.getDefaultInstance();
    private TaskPredictionProto.Builder builder = null;
    private boolean viaProto = false;

    public TaskPredictionPBImpl() {
        builder = TaskPredictionProto.newBuilder();
    }

    public TaskPredictionPBImpl(TaskPredictionProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    @JsonIgnore
    @org.codehaus.jackson.annotate.JsonIgnore
    public TaskPredictionProto getProto() {
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
            builder = TaskPredictionProto.newBuilder(proto);
        }
        viaProto = false;
    }


    @Override
    public String getId() {
        TaskPredictionProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setPredictor(String predictor) {
        maybeInitBuilder();
        builder.setPredictor(predictor);
    }

    @Override
    public String getPredictor() {
        TaskPredictionProtoOrBuilder p = viaProto ? proto : builder;
        return p.getPredictor();
    }

    @Override
    public void setId(String name) {
        maybeInitBuilder();
        builder.setId(name);
    }

    @Override
    public Long getDuration() {
        TaskPredictionProtoOrBuilder p = viaProto ? proto : builder;
        return p.getDuration();
    }

    @Override
    public void setDuration(Long duration) {
        maybeInitBuilder();
        builder.setDuration(duration);
    }

}