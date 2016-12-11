package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

public interface TaskProfile extends GeneralDataEntity<TaskProfile> {

    Long getInputBytes();

    void setInputBytes(Long inputBytes);

    Long getOutputBytes();

    void setOutputBytes(Long outputBytes);

    Long getDuration();

    TaskType getType();

    Long getStartTime();

    void setStartTime(Long startTime);

    Long getFinishTime();

    void setFinishTime(Long finishTime);

    void setType(TaskType type);

    void setReportedProgress(Float reportedProgress);

    Long getInputRecords();

    void setInputRecords(Long inputRecords);

    Long getOutputRecords();

    void setOutputRecords(Long outputRecords);

    String getAppId();

    void setAppId(String appId);

    String getJobId();

    void setJobId(String jobId);

    Float getReportedProgress();

    void setSuccessfulAttempt(String successfulAttempt);

    String getSuccessfulAttempt();

    Long getShuffleTime();

    void setShuffleTime(Long time);

    Long getMergeTime();

    void setMergeTime(Long time);

    Long getReduceTime();

    void setReduceTime(Long time);

    Boolean isLocal();

    void setLocal(Boolean local);

    void setHttpAddress(String address);

    String getHttpAddress();

    TaskState getState();

    void setState(TaskState state);
}
