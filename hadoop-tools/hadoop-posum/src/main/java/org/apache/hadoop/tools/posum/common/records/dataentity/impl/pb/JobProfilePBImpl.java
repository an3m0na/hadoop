package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.yarn.proto.PosumProtos.JobProfileProto;
import org.apache.hadoop.yarn.proto.PosumProtos.JobProfileProtoOrBuilder;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 3/21/16.
 */
public class JobProfilePBImpl extends GeneralDataEntityPBImpl<JobProfile, JobProfileProto, JobProfileProto.Builder>
        implements JobProfile {

    public JobProfilePBImpl() {
    }

    public JobProfilePBImpl(JobProfileProto proto) {
        super(proto);
    }

    @Override
    void initBuilder() {
        builder = viaProto ? JobProfileProto.newBuilder(proto) : JobProfileProto.newBuilder();
    }

    private Map<String, String> flexMap;

    @Override
    void buildProto() {
        maybeInitBuilder();
        builder.clearFlexFields();
        if (flexMap != null) {
            StringStringMapPayloadPBImpl flexFields = new StringStringMapPayloadPBImpl();
            flexFields.setEntries(flexMap);
            builder.setFlexFields(flexFields.getProto());
        }
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
        if (!p.hasId())
            return null;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id == null) {
            builder.clearId();
            return;
        }
        builder.setId(id);
    }

    @Override
    public JobProfile copy() {
        return new JobProfilePBImpl(getProto());
    }

    @Override
    public Long getStartTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getStartTime();
    }

    @Override
    public void setStartTime(Long startTime) {
        maybeInitBuilder();
        if (startTime == null) {
            builder.clearStartTime();
            return;
        }
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
        if (user == null) {
            builder.clearUser();
            return;
        }
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
        if (name == null) {
            builder.clearName();
            return;
        }
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
        if (finishTime == null) {
            builder.clearFinishTime();
            return;
        }
        builder.setFinishTime(finishTime);
    }

    @Override
    public Long getDuration() {
        return Math.max(0, getFinishTime() - getStartTime());
    }

    @Override
    public void setTotalMapTasks(Integer totalMapTasks) {
        maybeInitBuilder();
        if (totalMapTasks == null) {
            builder.clearTotalMapTasks();
            return;
        }
        builder.setTotalMapTasks(totalMapTasks);
    }

    @Override
    public void setTotalReduceTasks(Integer totalReduceTasks) {
        maybeInitBuilder();
        if (totalReduceTasks == null) {
            builder.clearTotalReduceTasks();
            return;
        }
        builder.setTotalReduceTasks(totalReduceTasks);
    }

    @Override
    public Long getTotalInputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasTotalInputBytes())
            return p.getTotalInputBytes();
        else return null;
    }

    @Override
    public void setTotalInputBytes(Long inputBytes) {
        maybeInitBuilder();
        if (inputBytes == null) {
            builder.clearInputBytes();
            return;
        }
        builder.setTotalInputBytes(inputBytes);
    }

    @Override
    public Long getInputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getInputBytes();
    }

    @Override
    public void setInputBytes(Long inputBytes) {
        maybeInitBuilder();
        if (inputBytes == null) {
            builder.clearInputBytes();
            return;
        }
        builder.setInputBytes(inputBytes);
    }

    @Override
    public Long getMapOutputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getMapOutputBytes();
    }

    @Override
    public void setMapOutputBytes(Long bytes) {
        maybeInitBuilder();
        if (bytes == null) {
            builder.clearMapOutputBytes();
            return;
        }
        builder.setMapOutputBytes(bytes);
    }

    @Override
    public Long getReduceInputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getReduceInputBytes();
    }

    @Override
    public void setReduceInputBytes(Long bytes) {
        maybeInitBuilder();
        if (bytes == null) {
            builder.clearReduceInputBytes();
            return;
        }
        builder.setReduceInputBytes(bytes);
    }

    @Override
    public Long getOutputBytes() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getOutputBytes();
    }

    @Override
    public void setOutputBytes(Long outputBytes) {
        maybeInitBuilder();
        if (outputBytes == null) {
            builder.clearOutputBytes();
            return;
        }
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
        if (submitTime == null) {
            builder.clearSubmitTime();
            return;
        }
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
        if (appId == null) {
            builder.clearAppId();
            return;
        }
        builder.setAppId(appId);
    }

    @Override
    public JobState getState() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasState())
            return null;
        return JobState.valueOf(p.getState().name().substring("STATE_".length()));
    }

    @Override
    public void setState(JobState state) {
        maybeInitBuilder();
        if (state == null) {
            builder.clearState();
            return;
        }
        builder.setState(JobProfileProto.JobStateProto.valueOf("STATE_" + state.name()));
    }

    @Override
    public Float getMapProgress() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getMapProgress();
    }

    @Override
    public void setMapProgress(Float mapProgress) {
        maybeInitBuilder();
        if (mapProgress == null) {
            builder.clearMapProgress();
            return;
        }
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
        if (reduceProgress == null) {
            builder.clearReduceProgress();
            return;
        }
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
        if (completedMaps == null) {
            builder.clearCompletedMaps();
            return;
        }
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
        if (completedReduces == null) {
            builder.clearCompletedReduces();
            return;
        }
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
        if (uberized == null) {
            builder.clearUberized();
            return;
        }
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
        if (inputSplits == null) {
            builder.clearInputSplits();
            return;
        }
        builder.setInputSplits(inputSplits);
    }

    @Override
    public Long getAvgMapDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgMapDuration();
    }

    @Override
    public void setAvgMapDuration(Long avgMapDuration) {
        maybeInitBuilder();
        if (avgMapDuration == null) {
            builder.clearAvgMapDuration();
            return;
        }
        builder.setAvgMapDuration(avgMapDuration);
    }

    @Override
    public Long getAvgReduceDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgReduceDuration();
    }

    @Override
    public void setAvgReduceDuration(Long duration) {
        maybeInitBuilder();
        if (duration == null) {
            builder.clearAvgReduceDuration();
            return;
        }
        builder.setAvgReduceDuration(duration);
    }


    @Override
    public Long getAvgTaskDuration() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgTaskDuration();
    }

    @Override
    public void setAvgTaskDuration(Long avgTaskDuration) {
        maybeInitBuilder();
        if (avgTaskDuration == null) {
            builder.clearAvgTaskDuration();
            return;
        }
        builder.setAvgTaskDuration(avgTaskDuration);
    }

    @Override
    public void setAvgShuffleTime(Long avgShuffleDuration) {
        maybeInitBuilder();
        if (avgShuffleDuration == null) {
            builder.clearAvgShuffleTime();
            return;
        }
        builder.setAvgShuffleTime(avgShuffleDuration);
    }

    @Override
    public Long getAvgShuffleTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgShuffleTime();
    }

    @Override
    public void setAvgMergeTime(Long time) {
        maybeInitBuilder();
        if (time == null) {
            builder.clearAvgMergeTime();
            return;
        }
        builder.setAvgMergeTime(time);
    }

    @Override
    public Long getAvgMergeTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgMergeTime();
    }

    @Override
    public void setAvgReduceTime(Long time) {
        maybeInitBuilder();
        if (time == null) {
            builder.clearAvgReduceTime();
            return;
        }
        builder.setAvgReduceTime(time);
    }

    @Override
    public Long getAvgReduceTime() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAvgReduceTime();
    }

    @Override
    public void setQueue(String queue) {
        maybeInitBuilder();
        if (queue == null) {
            builder.clearQueue();
            return;
        }
        builder.setQueue(queue);

    }

    @Override
    public String getQueue() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getQueue();
    }

    @Override
    public void addAll(Map<String, String> other) {
        maybeInitBuilder();
        getFlexFields().putAll(other);
    }

    @Override
    public String getFlexField(String name) {
        return getFlexFields().get(name);
    }

    @Override
    public Map<String, String> getFlexFields() {
        if (flexMap == null) {
            JobProfileProtoOrBuilder p = viaProto ? proto : builder;
            flexMap = new StringStringMapPayloadPBImpl(p.getFlexFields()).getEntries();
        }
        return flexMap;
    }

    @Override
    public String getReducerClass() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasReducerClass())
            return null;
        return p.getReducerClass();
    }

    @Override
    public void setMapperClass(String name) {
        maybeInitBuilder();
        if (name == null) {
            builder.clearName();
            return;
        }
        builder.setMapperClass(name);
    }

    @Override
    public String getMapperClass() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasMapperClass())
            return null;
        return p.getMapperClass();
    }

    @Override
    public void setReducerClass(String name) {
        maybeInitBuilder();
        if (name == null) {
            builder.clearReducerClass();
            return;
        }
        builder.setReducerClass(name);

    }

    @Override
    public List<String> getSplitLocations() {
        JobProfileProtoOrBuilder p = viaProto ? proto : builder;
        return p.getSplitLocationsList();
    }

    @Override
    public void setSplitLocations(List<String> locations) {
        maybeInitBuilder();
        builder.clearSplitLocations();
        if (locations == null) {
            return;
        }
        builder.addAllSplitLocations(locations);
    }
}
