package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;

import java.util.List;
import java.util.Map;

public interface JobProfile extends GeneralDataEntity<JobProfile> {

  String getName();

  void setName(String name);

  Long getStartTime();

  void setStartTime(Long startTime);

  Long getFinishTime();

  void setFinishTime(Long finishTime);

  void setTotalMapTasks(Integer totalMapTasks);

  void setTotalReduceTasks(Integer totalReduceTasks);

  Long getTotalSplitSize();

  void setTotalSplitSize(Long inputBytes);

  Long getInputBytes();

  void setInputBytes(Long inputBytes);

  Long getMapOutputBytes();

  void setMapOutputBytes(Long bytes);

  Long getReduceInputBytes();

  void setReduceInputBytes(Long bytes);

  Long getOutputBytes();

  void setOutputBytes(Long outputBytes);

  String getUser();

  void setUser(String user);

  Long getSubmitTime();

  void setSubmitTime(Long submitTime);

  Integer getTotalMapTasks();

  Integer getTotalReduceTasks();

  String getAppId();

  void setAppId(String appId);

  JobState getState();

  void setState(JobState state);

  Float getMapProgress();

  void setMapProgress(Float mapProgress);

  Float getReduceProgress();

  void setReduceProgress(Float reduceProgress);

  Integer getCompletedMaps();

  void setCompletedMaps(Integer completedMaps);

  Integer getCompletedReduces();

  void setCompletedReduces(Integer completedReduces);

  Boolean isUberized();

  void setUberized(Boolean uberized);

  Long getAvgMapDuration();

  void setAvgMapDuration(Long avgMapDuration);

  Long getAvgReduceDuration();

  void setAvgReduceDuration(Long avgReduceDuration);

  void setAvgShuffleTime(Long time);

  Long getAvgShuffleTime();

  void setAvgMergeTime(Long time);

  Long getAvgMergeTime();

  void setAvgReduceTime(Long time);

  Long getAvgReduceTime();

  void setQueue(String queue);

  String getQueue();

  void addAllFlexFields(Map<String, String> other);

  String getFlexField(String name);

  Map<String, String> getFlexFields();

  String getMapperClass();

  void setMapperClass(String name);

  String getReducerClass();

  void setReducerClass(String name);

  List<List<String>> getSplitLocations();

  void setSplitLocations(List<List<String>> splitLocations);

  List<Long> getSplitSizes();

  void setSplitSizes(List<Long> splitSizes);

  Long getDeadline();

  void setDeadline(Long deadline);

  String getHostName();

  void setHostName(String hostName);

  boolean isFinished();
}
