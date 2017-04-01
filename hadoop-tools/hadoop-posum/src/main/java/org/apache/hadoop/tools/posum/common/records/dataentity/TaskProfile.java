package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

import java.util.List;

public interface TaskProfile extends GeneralDataEntity<TaskProfile> {

  String getAppId();

  void setAppId(String appId);

  String getJobId();

  void setJobId(String jobId);

  TaskType getType();

  void setType(TaskType type);

  Long getStartTime();

  void setStartTime(Long startTime);

  Long getFinishTime();

  void setFinishTime(Long finishTime);

  TaskState getState();

  void setState(TaskState state);

  Float getReportedProgress();

  void setReportedProgress(Float reportedProgress);

  String getSuccessfulAttempt();

  void setSuccessfulAttempt(String successfulAttempt);

  Long getInputBytes();

  void setInputBytes(Long inputBytes);

  Long getOutputBytes();

  void setOutputBytes(Long outputBytes);

  Long getInputRecords();

  void setInputRecords(Long inputRecords);

  Long getOutputRecords();

  void setOutputRecords(Long outputRecords);

  Long getShuffleTime();

  void setShuffleTime(Long time);

  Long getMergeTime();

  void setMergeTime(Long time);

  Long getReduceTime();

  void setReduceTime(Long time);

  Boolean isLocal();

  void setLocal(Boolean local);

  String getHttpAddress();

  void setHttpAddress(String address);

  List<String> getSplitLocations();

  void setSplitLocations(List<String> splitLocations);

  Long getSplitSize();

  void setSplitSize(Long splitSize);
}
