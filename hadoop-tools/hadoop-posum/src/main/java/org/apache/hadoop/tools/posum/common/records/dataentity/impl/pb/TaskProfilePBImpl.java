package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.yarn.proto.PosumProtos.TaskProfileProto;
import org.apache.hadoop.yarn.proto.PosumProtos.TaskProfileProtoOrBuilder;

import java.util.List;

public class TaskProfilePBImpl extends GeneralDataEntityPBImpl<TaskProfile, TaskProfileProto, TaskProfileProto.Builder>
  implements TaskProfile {

  public TaskProfilePBImpl() {
  }

  public TaskProfilePBImpl(TaskProfileProto proto) {
    super(proto);
  }

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
  public Long getLastUpdated() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLastUpdated())
      return null;
    return p.getLastUpdated();
  }

  @Override
  public void setLastUpdated(Long timestamp) {
    maybeInitBuilder();
    if (timestamp == null) {
      builder.clearLastUpdated();
      return;
    }
    builder.setLastUpdated(timestamp);
  }

  @Override
  public TaskProfile copy() {
    return new TaskProfilePBImpl(getProto());
  }

  @Override
  public Long getStartTime() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasStartTime())
      return null;
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
  public Long getFinishTime() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasFinishTime())
      return null;
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
  public void setInputBytes(Long inputBytes) {
    maybeInitBuilder();
    if (inputBytes == null) {
      builder.clearInputBytes();
      return;
    }
    builder.setInputBytes(inputBytes);
  }

  @Override
  public Long getOutputBytes() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasOutputBytes())
      return null;
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
  public String getAppId() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppId())
      return null;
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
  public Long getInputBytes() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasInputBytes())
      return null;
    return p.getInputBytes();
  }

  @Override
  public TaskType getType() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasType())
      return null;
    return TaskType.valueOf(p.getType().name().substring("TYPE_".length()));
  }

  @Override
  public void setType(TaskType type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearType();
      return;
    }
    builder.setType(TaskProfileProto.TaskTypeProto.valueOf("TYPE_" + type.name()));
  }

  @Override
  public void setReportedProgress(Float reportedProgress) {
    maybeInitBuilder();
    if (reportedProgress == null) {
      builder.clearReportedProgress();
      return;
    }
    builder.setReportedProgress(reportedProgress);
  }

  @Override
  public Long getInputRecords() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasInputRecords())
      return null;
    return p.getInputRecords();
  }

  @Override
  public void setInputRecords(Long inputRecords) {
    maybeInitBuilder();
    if (inputRecords == null) {
      builder.clearInputRecords();
      return;
    }
    builder.setInputRecords(inputRecords);
  }

  @Override
  public Long getOutputRecords() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasOutputRecords())
      return null;
    return p.getOutputRecords();
  }

  @Override
  public void setOutputRecords(Long outputRecords) {
    maybeInitBuilder();
    if (outputRecords == null) {
      builder.clearOutputRecords();
      return;
    }
    builder.setOutputRecords(outputRecords);
  }

  @Override
  public String getJobId() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasJobId())
      return null;
    return p.getJobId();
  }

  @Override
  public void setJobId(String jobId) {
    maybeInitBuilder();
    if (jobId == null) {
      builder.clearJobId();
      return;
    }
    builder.setJobId(jobId);
  }

  @Override
  public Float getReportedProgress() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasReportedProgress())
      return null;
    return p.getReportedProgress();
  }

  @Override
  public void setSuccessfulAttempt(String successfulAttempt) {
    maybeInitBuilder();
    if (successfulAttempt == null) {
      builder.clearSuccessfulAttempt();
      return;
    }
    builder.setSuccessfulAttempt(successfulAttempt);
  }

  @Override
  public String getSuccessfulAttempt() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSuccessfulAttempt())
      return null;
    return p.getSuccessfulAttempt();
  }

  @Override
  public Long getShuffleTime() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasShuffleTime())
      return null;
    return p.getShuffleTime();
  }

  @Override
  public void setShuffleTime(Long time) {
    maybeInitBuilder();
    if (time == null) {
      builder.clearShuffleTime();
      return;
    }
    builder.setShuffleTime(time);
  }

  @Override
  public Long getMergeTime() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasMergeTime())
      return null;
    return p.getMergeTime();
  }

  @Override
  public void setMergeTime(Long time) {
    maybeInitBuilder();
    if (time == null) {
      builder.clearMergeTime();
      return;
    }
    builder.setMergeTime(time);
  }

  @Override
  public Long getReduceTime() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasReduceTime())
      return null;
    return p.getReduceTime();
  }

  @Override
  public void setReduceTime(Long time) {
    maybeInitBuilder();
    if (time == null) {
      builder.clearReduceTime();
      return;
    }
    builder.setReduceTime(time);
  }

  @Override
  public Boolean isLocal() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasLocal())
      return null;
    return p.getLocal();
  }

  @Override
  public void setLocal(Boolean local) {
    maybeInitBuilder();
    if (local == null) {
      builder.clearLocal();
      return;
    }
    builder.setLocal(local);
  }

  @Override
  public void setHttpAddress(String address) {
    maybeInitBuilder();
    if (address == null) {
      builder.clearHttpAddress();
      return;
    }
    builder.setHttpAddress(address);
  }

  @Override
  public List<String> getSplitLocations() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    return p.getSplitLocationsList();
  }

  @Override
  public void setSplitLocations(List<String> splitLocations) {
    maybeInitBuilder();
    builder.clearSplitLocations();
    if (splitLocations == null) {
      return;
    }
    builder.addAllSplitLocations(splitLocations);
  }

  @Override
  public Long getSplitSize() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasSplitSize())
      return null;
    return p.getSplitSize();
  }

  @Override
  public void setSplitSize(Long splitSize) {
    maybeInitBuilder();
    builder.clearSplitSize();
    if (splitSize == null) {
      return;
    }
    builder.setSplitSize(splitSize);
  }

  @Override
  public String getHttpAddress() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHttpAddress())
      return null;
    return p.getHttpAddress();
  }

  @Override
  public TaskState getState() {
    TaskProfileProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState())
      return null;
    return TaskState.valueOf(p.getState().name().substring("STATE_".length()));
  }

  @Override
  public void setState(TaskState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(TaskProfileProto.TaskStateProto.valueOf("STATE_" + state.name()));
  }
}
