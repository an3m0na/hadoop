package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;

public class TaskPredictionInput {
  private String taskId;
  private TaskType taskType;
  private String nodeAddress;
  private String jobId;
  private JobProfile job;
  private TaskProfile task;

  public TaskPredictionInput(String taskId) {
    this.taskId = taskId;
  }

  public TaskPredictionInput(String taskId, String nodeAddress) {
    this(taskId);
    this.nodeAddress = nodeAddress;
  }

  public TaskPredictionInput(TaskProfile task) {
    this(task.getId());
    this.task = task;
    this.jobId = task.getJobId();
    this.taskType = task.getType();
  }

  public TaskPredictionInput(TaskProfile task, String nodeAddress) {
    this(task);
    this.nodeAddress = nodeAddress;
  }

  public TaskPredictionInput(String jobId, TaskType taskType) {
    this.jobId = jobId;
    this.taskType = taskType;
  }

  public TaskPredictionInput(String jobId, TaskType taskType, String nodeAddress) {
    this(jobId, taskType);
    this.nodeAddress = nodeAddress;
  }

  public TaskPredictionInput(JobProfile job, TaskType taskType) {
    this(job.getId(), taskType);
    this.job = job;
  }

  public TaskPredictionInput(JobProfile job, TaskType taskType, String nodeAddress) {
    this(job, taskType);
    this.nodeAddress = nodeAddress;
  }

  public String getTaskId() {
    return taskId;
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

  public void setTask(TaskProfile task) {
    this.task = task;
  }

  public TaskProfile getTask() {
    return task;
  }
}
