package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobProfileProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobProfileProtoOrBuilder;

/**
 * Created by ane on 3/21/16.
 */
public class JobProfilePBImpl extends GeneralDataEntityPBImpl<JobProfile, JobProfileProto, JobProfileProto.Builder>
        implements JobProfile{

    @Override
    void initBuilder() {
        builder = viaProto? JobProfileProto.newBuilder(proto) : JobProfileProto.newBuilder();
    }

    @Override
    void buildProto() {
        proto = builder.build();
    }

    @Override
    public JobProfile parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = JobProfileProto.parseFrom(data);
        viaProto = true;
        return this;
    }
    @Override
    public String getId() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return "".equals(p.getId())? null : p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        builder.setId(id);
    }

    @Override
    public Long getStartTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getStartTime();
    }

    @Override
    public void setStartTime(Long startTime) {
        maybeInitBuilder();
        builder.setStartTime(startTime);
    }

    @Override
    public String getUser() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getUser();
    }

    @Override
    public void setUser(String user) {
        maybeInitBuilder();
        builder.setUser(user);
    }

    @Override
    public String getName() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getName();
    }

    @Override
    public void setName(String name) {
        maybeInitBuilder();
        builder.setName(name);
    }

    @Override
    public Long getFinishTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getFinishTime();
    }

    @Override
    public void setFinishTime(Long finishTime) {
        maybeInitBuilder();
        builder.setFinishTime(finishTime);
    }

    @Override
    public Integer getDuration() {
        return new Long(Math.min(0, getFinishTime()-getStartTime())).intValue();
    }

    @Override
    public void setTotalMapTasks(Integer totalMapTasks) {
        maybeInitBuilder();
        builder.setTotalMapTasks(totalMapTasks);
    }

    @Override
    public void setTotalReduceTasks(Integer totalReduceTasks) {
        maybeInitBuilder();
        builder.setTotalReduceTasks(totalReduceTasks);
    }

    @Override
    public void setInputBytes(Long inputBytes) {
        maybeInitBuilder();
        builder.setInputBytes(inputBytes);
    }

    @Override
    public Long getOutputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOutputBytes();
    }

    @Override
    public void setOutputBytes(Long outputBytes) {
        maybeInitBuilder();
        builder.setOutputBytes(outputBytes);
    }

    @Override
    public Long getSubmitTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getSubmitTime();
    }

    @Override
    public void setSubmitTime(Long submitTime) {
        maybeInitBuilder();
        builder.setSubmitTime(submitTime);
    }

    @Override
    public Integer getTotalMapTasks() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getTotalMapTasks();
    }

    @Override
    public Integer getTotalReduceTasks() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getTotalReduceTasks();
    }

    @Override
    public String getAppId() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAppId();
    }

    @Override
    public void setAppId(String appId) {
        maybeInitBuilder();
        builder.setAppId(appId);
    }

    @Override
    public JobState getState() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        JobProfileProto.JobStateProto enumValue = p.getState();
        if (enumValue == null || enumValue.name().equals("STATE_NULL"))
            return null;
        return JobState.valueOf(enumValue.name().substring("STATE_".length()));
    }

    @Override
    public void setState(JobState state) {
        maybeInitBuilder();
        if (state != null)
            builder.setState(JobProfileProto.JobStateProto.valueOf("STATE_" + state.name()));
        else
            builder.setState(JobProfileProto.JobStateProto.STATE_NULL);
    }

    @Override
    public Float getMapProgress() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getMapProgress();
    }

    @Override
    public void setMapProgress(Float mapProgress) {
        maybeInitBuilder();
        builder.setMapProgress(mapProgress);
    }

    @Override
    public Float getReduceProgress() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getReduceProgress();
    }

    @Override
    public void setReduceProgress(Float reduceProgress) {
        maybeInitBuilder();
        builder.setReduceProgress(reduceProgress);
    }

    @Override
    public Integer getCompletedMaps() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getCompletedMaps();
    }

    @Override
    public void setCompletedMaps(Integer completedMaps) {
        maybeInitBuilder();
        builder.setCompletedMaps(completedMaps);
    }

    @Override
    public Integer getCompletedReduces() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getCompletedReduces();
    }

    @Override
    public void setCompletedReduces(Integer completedReduces) {
        maybeInitBuilder();
        builder.setCompletedReduces(completedReduces);
    }

    @Override
    public Boolean isUberized() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getUberized();
    }

    @Override
    public void setUberized(Boolean uberized) {
        maybeInitBuilder();
        builder.setUberized(uberized);
    }

    @Override
    public Integer getInputSplits() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getInputSplits();
    }

    @Override
    public void setInputSplits(Integer inputSplits) {
        maybeInitBuilder();
        builder.setInputSplits(inputSplits);
    }

    @Override
    public Long getInputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getInputBytes();
    }

    @Override
    public Long getAvgSplitSize() {
        return null;
    }

    @Override
    public Integer getAvgMapDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgMapDuration();
    }

    @Override
    public void setAvgMapDuration(Integer avgMapDuration) {
        maybeInitBuilder();
        builder.setAvgMapDuration(avgMapDuration);
    }

    @Override
    public Integer getAvgReduceDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgReduceDuration();
    }

    @Override
    public void setAvgReduceDuration(Integer avgReduceDuration) {
        maybeInitBuilder();
        builder.setAvgReduceDuration(avgReduceDuration);
    }

    @Override
    public Integer getAvgTaskDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgTaskDuration();
    }

    @Override
    public void setAvgTaskDuration(Integer avgTaskDuration) {
        maybeInitBuilder();
        builder.setAvgTaskDuration(avgTaskDuration);
    }

    @Override
    public void setAvgShuffleDuration(Integer avgShuffleDuration) {
        maybeInitBuilder();
        builder.setAvgShuffleDuration(avgShuffleDuration);
    }

    @Override
    public Integer getAvgShuffleDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgShuffleDuration();
    }

    @Override
    public void setAvgMergeDuration(Integer avgMergeDuration) {
        maybeInitBuilder();
        builder.setAvgMergeDuration(avgMergeDuration);
    }

    @Override
    public Integer getAvgMergeDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgMergeDuration();
    }
}
