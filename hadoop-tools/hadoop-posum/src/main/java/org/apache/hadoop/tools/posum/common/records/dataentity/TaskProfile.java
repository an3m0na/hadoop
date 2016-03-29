package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.Counters;
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

//     Counters getCounters() {
//        return counters;
//    }
//
//     synchronized void setCounters(Counters counters) {
//        this.counters = counters;
//    }

     void setFinishTime(Long finishTime);

     Long getExpectedInputBytes();

     void setExpectedInputBytes(Long expectedInputBytes);

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
}
