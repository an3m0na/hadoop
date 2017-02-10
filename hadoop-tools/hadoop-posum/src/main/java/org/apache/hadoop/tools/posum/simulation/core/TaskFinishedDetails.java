package org.apache.hadoop.tools.posum.simulation.core;

class TaskFinishedDetails implements EventDetails {
  private String taskId;

  public TaskFinishedDetails(String taskId) {
    this.taskId = taskId;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }
}
