package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * Created by ane on 2/8/16.
 */
public interface TaskProfile extends GeneralDataEntity {

    Long getInputBytes();

    void setInputBytes(Long inputBytes);

    Long getOutputBytes();

    void setOutputBytes(Long outputBytes);

    Integer getDuration();

    TaskType getType();

    Long getStartTime();

    void setStartTime(Long startTime);

    Long getFinishTime();

    void setFinishTime(Long finishTime);

    void setType(String type);

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
}
