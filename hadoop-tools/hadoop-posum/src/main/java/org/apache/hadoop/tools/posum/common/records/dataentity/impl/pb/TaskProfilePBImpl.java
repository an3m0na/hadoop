package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.yarn.proto.POSUMProtos.TaskProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.TaskProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class TaskProfilePBImpl extends TaskProfile implements GeneralDataEntityPBImpl<TaskProfile, TaskProfileProto> {
    private TaskProfileProto proto = TaskProfileProto.getDefaultInstance();
    private TaskProfileProto.Builder builder = null;
    private boolean viaProto = false;

    public TaskProfilePBImpl() {
        builder = TaskProfileProto.newBuilder();
    }

    @JsonIgnore
    public TaskProfileProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public TaskProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = TaskProfileProto.parseFrom(data);
        viaProto = true;
        return this;
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
            builder = TaskProfileProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Long getStartTime() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getStartTime();
    }

    @Override
    public void setStartTime(Long startTime) {
        maybeInitBuilder();
        builder.setStartTime(startTime);
    }


    @Override
    public String getId() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

    @Override
    public Long getFinishTime() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinishTime();
    }

    @Override
    public void setFinishTime(Long finishTime) {
        maybeInitBuilder();
        builder.setFinishTime(finishTime);
    }


    @Override
    public void setInputBytes(Long inputBytes) {
        maybeInitBuilder();
        builder.setInputBytes(inputBytes);
    }

    @Override
    public Long getOutputBytes() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOutputBytes();
    }

    @Override
    public void setOutputBytes(Long outputBytes) {
        maybeInitBuilder();
        builder.setOutputBytes(outputBytes);
    }

    @Override
    public String getAppId() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAppId();
    }

    @Override
    public void setAppId(String appId) {
        maybeInitBuilder();
        builder.setAppId(appId);
    }

    @Override
    public Long getInputBytes() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getInputBytes();
    }

    @Override
    public TaskType getType() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        TaskProfileProto.TaskTypeProto enumValue = p.getType();
        if (enumValue == null || enumValue.name().equals("TYPE_NULL"))
            return null;
        return TaskType.valueOf(enumValue.name().substring("TYPE_".length()));
    }

    @Override
    public Long getExpectedInputBytes() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getExpectedInputBytes();
    }

    @Override
    public void setExpectedInputBytes(Long expectedInputBytes) {
        maybeInitBuilder();
        builder.setExpectedInputBytes(expectedInputBytes);
    }

    @Override
    public void setType(String type) {
        maybeInitBuilder();
        if (type != null)
            builder.setType(TaskProfileProto.TaskTypeProto.valueOf("TYPE_" + type));
        else
            builder.setType(TaskProfileProto.TaskTypeProto.TYPE_NULL);
    }

    @Override
    public void setReportedProgress(Float reportedProgress) {
            maybeInitBuilder();
            builder.setReportedProgress(reportedProgress);
    }

    @Override
    public Long getInputRecords() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getInputRecords();
    }

    @Override
    public void setInputRecords(Long inputRecords) {
        maybeInitBuilder();
        builder.setInputRecords(inputRecords);
    }

    @Override
    public Long getOutputRecords() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOutputRecords();
    }

    @Override
    public void setOutputRecords(Long outputRecords) {
        maybeInitBuilder();
        builder.setOutputRecords(outputRecords);
    }

    @Override
    public String getJobId() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getJobId();
    }

    @Override
    public void setJobId(String jobId) {
        maybeInitBuilder();
        builder.setJobId(jobId);
    }

    @Override
    public Float getReportedProgress() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getReportedProgress();
    }
}
