package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;

import java.util.List;

class TaskInfo {
  private List<TaskProfile> tasks;
  private List<CountersProxy> counters;

  TaskInfo() {

  }

  TaskInfo(List<TaskProfile> tasks, List<CountersProxy> counters) {
    this.tasks = tasks;
    this.counters = counters;
  }

  List<TaskProfile> getTasks() {
    return tasks;
  }

  void setTasks(List<TaskProfile> tasks) {
    this.tasks = tasks;
  }

  List<CountersProxy> getCounters() {
    return counters;
  }

  void setCounters(List<CountersProxy> counters) {
    this.counters = counters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskInfo taskInfo = (TaskInfo) o;

    if (tasks != null ? !tasks.equals(taskInfo.tasks) : taskInfo.tasks != null) return false;
    return counters != null ? counters.equals(taskInfo.counters) : taskInfo.counters == null;

  }

  @Override
  public int hashCode() {
    int result = tasks != null ? tasks.hashCode() : 0;
    result = 31 * result + (counters != null ? counters.hashCode() : 0);
    return result;
  }
}
