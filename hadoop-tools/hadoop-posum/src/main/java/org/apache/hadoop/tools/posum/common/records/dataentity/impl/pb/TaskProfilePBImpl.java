package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.yarn.proto.POSUMProtos.TaskProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.TaskProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class TaskProfilePBImpl extends GeneralDataEntityPBImpl<TaskProfile, TaskProfileProto, TaskProfileProto.Builder>
        implements TaskProfile {

    @Override
    void initBuilder() {
        builder = viaProto ? TaskProfileProto.newBuilder(proto) : TaskProfileProto.newBuilder();
    }

    @Override
    void buildProto() {
        proto = builder.build();
    }

    @Override
    public TaskProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = TaskProfileProto.parseFrom(data);
        viaProto = true;
        return this;
    }

    @Override
    public String getId() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return "".equals(p.getId()) ? null : p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id != null)
            builder.setId(id);
    }

    @Override
    public Long getStartTime() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getStartTime();
    }

    @Override
    public void setStartTime(Long startTime) {
        maybeInitBuilder();
        if (startTime != null)
            builder.setStartTime(startTime);
    }

    @Override
    public Long getFinishTime() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinishTime();
    }

    @Override
    public void setFinishTime(Long finishTime) {
        maybeInitBuilder();
        if (finishTime != null)
            builder.setFinishTime(finishTime);
    }


    @Override
    public void setInputBytes(Long inputBytes) {
        maybeInitBuilder();
        if (inputBytes != null)
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
        if (outputBytes != null)
            builder.setOutputBytes(outputBytes);
    }

    @Override
    public Integer getDuration() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        if(p.hasStartTime() && p.hasFinishTime())
            return Long.valueOf(p.getFinishTime() - p.getStartTime()).intValue();
        else
            return -1;
    }

    @Override
    public String getAppId() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAppId();
    }

    @Override
    public void setAppId(String appId) {
        maybeInitBuilder();
        if (appId != null)
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
        if (reportedProgress != null)
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
        if (inputRecords != null)
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
        if (outputRecords != null)
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
        if (jobId != null)
            builder.setJobId(jobId);
    }

    @Override
    public Float getReportedProgress() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getReportedProgress();
    }

    @Override
    public void setSuccessfulAttempt(String successfulAttempt) {
        maybeInitBuilder();
        if (successfulAttempt != null)
            builder.setSuccessfulAttempt(successfulAttempt);
    }

    @Override
    public String getSuccessfulAttempt() {
        TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getSuccessfulAttempt();
    }
}
