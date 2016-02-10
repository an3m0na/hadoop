package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Created by ane on 2/8/16.
 */
public class TaskProfile {
    private String taskId;
    private String jobId;
    private Long inputBytes;
    private Long outputBytes;
    private Long expectedInputBytes;
    private Long expectedOutputBytes;
    private Long startTime;
    private Long finishTime;
    private Integer expectedDuration;
    private TaskType type;
    private Counters counters;
    private Float reportedProgress;

    public TaskProfile(String taskId, TaskType type, Long startTime){
        this.taskId = taskId;
        this.type = type;
        this.startTime = startTime;
    }

    public String getTaskId() {
        return taskId;
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

    public float getReportedProgress() {
        return reportedProgress;
    }

    public void setReportedProgress(float reportedProgress) {
        this.reportedProgress = reportedProgress;
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

    public String getJobId() {
        return jobId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public void setReportedProgress(Float reportedProgress) {
        this.reportedProgress = reportedProgress;
    }
}
