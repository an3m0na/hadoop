package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.Utils;

import java.util.ArrayList;
import java.util.List;

class TaskInfoCollector {

  private HadoopAPIClient api;

  TaskInfoCollector() {
    this.api = new HadoopAPIClient();
  }

  TaskInfo getFinishedTaskInfo(JobProfile job) {
    List<TaskProfile> tasks = api.getFinishedTasksInfo(job.getId());
    List<CountersProxy> countersList = new ArrayList<>(tasks.size());

    for (TaskProfile task : tasks) {
      api.addFinishedAttemptInfo(task);
      task.setAppId(job.getAppId());
      if (task.getSplitLocations() != null && task.getHttpAddress() != null) {
        if (task.getSplitLocations().contains(task.getHttpAddress()))
          task.setLocal(true);
      }
      CountersProxy counters = api.getFinishedTaskCounters(task.getJobId(), task.getId());
      if (counters != null) {
        Utils.updateTaskStatisticsFromCounters(task, counters);
        countersList.add(counters);
      }
    }
    return new TaskInfo(tasks, countersList);
  }

  TaskInfo getRunningTaskInfo(JobProfile job) {
    List<TaskProfile> tasks = api.getRunningTasksInfo(job);
    if (tasks == null || tasks.size() == 0) {
      return null;
    }
    List<CountersProxy> countersList = new ArrayList<>(tasks.size());

    for (TaskProfile task : tasks) {
      task.setAppId(job.getAppId());
      if (!api.addRunningAttemptInfo(task)) {
        return null;
      }
      CountersProxy counters = api.getRunningTaskCounters(task.getAppId(), task.getJobId(), task.getId());
      if (counters == null) {
        return null;
      }
      Utils.updateTaskStatisticsFromCounters(task, counters);
      countersList.add(counters);
    }
    return new TaskInfo(tasks, countersList);
  }
}
