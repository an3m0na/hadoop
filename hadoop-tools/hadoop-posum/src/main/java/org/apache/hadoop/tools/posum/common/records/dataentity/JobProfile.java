package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;

/**
 * Created by ane on 2/8/16.
 */
 public interface JobProfile extends GeneralDataEntity {

    //NOT INCLUDED:
//    <failedReduceAttempts>0</failedReduceAttempts>
//    <killedReduceAttempts>0</killedReduceAttempts>
//    <successfulReduceAttempts>1</successfulReduceAttempts>
//    <failedMapAttempts>0</failedMapAttempts>
//    <killedMapAttempts>0</killedMapAttempts>
//    <successfulMapAttempts>30</successfulMapAttempts>

     String getName();

     void setName(String name);

     Long getStartTime();

     void setStartTime(Long startTime);

     Long getFinishTime();

     void setFinishTime(Long finishTime);

     Integer getDuration();

     void setTotalMapTasks(Integer totalMapTasks);

     void setTotalReduceTasks(Integer totalReduceTasks);

     void setInputBytes(Long inputBytes);

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

     void setState(String state);

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

     Integer getInputSplits();

     void setInputSplits(Integer inputSplits);

     Long getInputBytes();

     Long getAvgSplitSize();

     Integer getAvgMapDuration();

     void setAvgMapDuration(Integer avgMapDuration);

     Integer getAvgReduceDuration();

     void setAvgReduceDuration(Integer avgReduceDuration);

     Integer getAvgTaskDuration();

     void setAvgTaskDuration(Integer avgTaskDuration);

     void setAvgShuffleDuration(Integer avgShuffleDuration);

     Integer getAvgShuffleDuration();

     void setAvgMergeDuration(Integer avgMergeDuration);

     Integer getAvgMergeDuration();

}
