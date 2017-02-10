package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

public class TaskPredictionInput {
  private String taskId;
  private TaskType taskType;
  private String nodeAddress;
  private String jobId;
  private JobProfile job;

  public TaskPredictionInput(String taskId) {
    this(taskId, (String) null);
  }

  public TaskPredictionInput(String taskId, String nodeAddress) {
    this.taskId = taskId;
    this.nodeAddress = nodeAddress;
  }

  public TaskPredictionInput(String jobId, TaskType taskType) {
    this(jobId, taskType, null);
  }

  public TaskPredictionInput(String jobId, TaskType taskType, String nodeAddress) {
    this.jobId = jobId;
    this.taskType = taskType;
    this.nodeAddress = nodeAddress;
  }

  private TaskPredictionInput(JobProfile job, TaskType taskType, String nodeAddress) {
    this(job.getId(), taskType, nodeAddress);
    this.job = job;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }

  public String getNodeAddress() {
    return nodeAddress;
  }

  public void setNodeAddress(String nodeAddress) {
    this.nodeAddress = nodeAddress;
  }

  public JobProfile getJob() {
    return job;
  }

  public void setJob(JobProfile job) {
    this.job = job;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public String toString() {
    return "TaskPredictionInput{" +
      "taskId='" + taskId + '\'' +
      ", taskType=" + taskType +
      ", nodeAddress='" + nodeAddress + '\'' +
      ", jobId='" + jobId + '\'' +
      ", job=" + job +
      '}';
  }
}
