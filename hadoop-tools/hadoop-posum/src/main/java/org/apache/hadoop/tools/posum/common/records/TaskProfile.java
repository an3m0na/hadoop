package org.apache.hadoop.tools.posum.common.records;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.codehaus.jackson.annotate.JsonProperty;
import org.mongojack.Id;

/**
 * Created by ane on 2/8/16.
 */
public class TaskProfile extends GeneralProfile {
    private Long inputBytes;
    private Long inputRecords;
    private Long outputBytes;
    private Long outputRecords;
    private Long expectedInputBytes;
    private Long expectedOutputBytes;
    private Long startTime = 0L;
    private Long finishTime = 0L;
    private Integer expectedDuration;
    private TaskType type;
    private Counters counters;
    private Float reportedProgress;
    private String appId;
    private String jobId;

    public TaskProfile(String id, TaskType type) {
        this.id = id;
        this.type = type;
    }

    public Long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(Long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public Long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(Long outputBytes) {
        this.outputBytes = outputBytes;
    }

    public Integer getDuration() {
        if(finishTime == 0) return -1;
        return new Long(finishTime - startTime).intValue();
    }

    public TaskType getType() {
        return type;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getFinishTime() {
        return finishTime;
    }

    public Counters getCounters() {
        return counters;
    }

    public synchronized void setCounters(Counters counters) {
        this.counters = counters;
    }

    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    public Long getExpectedInputBytes() {
        return expectedInputBytes;
    }

    public void setExpectedInputBytes(Long expectedInputBytes) {
        this.expectedInputBytes = expectedInputBytes;
    }

    public Long getExpectedOutputBytes() {
        return expectedOutputBytes;
    }

    public void setExpectedOutputBytes(Long expectedOutputBytes) {
        this.expectedOutputBytes = expectedOutputBytes;
    }

    public Integer getExpectedDuration() {
        return expectedDuration;
    }

    public void setExpectedDuration(Integer expectedDuration) {
        this.expectedDuration = expectedDuration;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public void setReportedProgress(Float reportedProgress) {
        this.reportedProgress = reportedProgress;
    }

    public Long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(Long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public Long getOutputRecords() {
        return outputRecords;
    }

    public void setOutputRecords(Long outputRecords) {
        this.outputRecords = outputRecords;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Float getReportedProgress() {
        return reportedProgress;
    }

    @Override
    public String toString() {
        return "TaskProfile{" +
                "id=" + id +
                ", inputBytes=" + inputBytes +
                ", inputRecords=" + inputRecords +
                ", outputBytes=" + outputBytes +
                ", outputRecords=" + outputRecords +
                ", expectedInputBytes=" + expectedInputBytes +
                ", expectedOutputBytes=" + expectedOutputBytes +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", expectedDuration=" + expectedDuration +
                ", type=" + type +
                ", counters=" + counters +
                ", reportedProgress=" + reportedProgress +
                '}';
    }

}
