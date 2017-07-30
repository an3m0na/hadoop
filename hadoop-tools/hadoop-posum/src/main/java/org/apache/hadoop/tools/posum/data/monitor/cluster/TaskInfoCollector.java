package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;

class TaskInfoCollector {

  private HadoopAPIClient api;
  private Database db;

  TaskInfoCollector() {

  }

  TaskInfoCollector(Database db) {
    this.db = db;
    this.api = new HadoopAPIClient();
  }

  TaskInfo getFinishedTaskInfo(JobProfile job) {
    List<TaskProfile> previousTasks = getCurrentTaskProfiles(job);
    if (previousTasks == null || previousTasks.size() < 1) {
      previousTasks = createTaskStubs(job);
    }

    List<TaskProfile> tasks = api.getFinishedTasksInfo(job.getId(), previousTasks);
    List<CountersProxy> countersList = new ArrayList<>(tasks.size());

    for (TaskProfile task : tasks) {
      api.addFinishedAttemptInfo(task);
      task.setAppId(job.getAppId());
      if (task.isLocal() == null && task.getSplitLocations() != null && task.getHttpAddress() != null) {
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

  TaskInfo getRunningTaskInfo(AppProfile app, JobProfile job) {
    List<TaskProfile> tasks = getCurrentTaskProfiles(job);
    if (tasks == null || tasks.size() < 1) {
      tasks = createTaskStubs(job);
    }
    if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
      tasks = api.getRunningTasksInfo(job, tasks);
      if (tasks == null || tasks.size() == 0) {
        // job might have finished
        return null;
      }
      List<CountersProxy> countersList = new ArrayList<>(tasks.size());

      for (TaskProfile task : tasks) {
        if (!api.addRunningAttemptInfo(task)) {
          return null;
        }
        if (task.isLocal() == null && task.getSplitLocations() != null && task.getHttpAddress() != null) {
          if (task.getSplitLocations().contains(task.getHttpAddress()))
            task.setLocal(true);
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
    return new TaskInfo(tasks);
  }

  private List<TaskProfile> createTaskStubs(JobProfile job) {
    List<TaskProfile> taskStubs = new ArrayList<>(job.getTotalMapTasks());
    JobId jobId = Utils.parseJobId(job.getId());
    for (int i = 0; i < job.getSplitLocations().size(); i++) {
      TaskProfile task = Records.newRecord(TaskProfile.class);
      task.setId(MRBuilderUtils.newTaskId(jobId, i, TaskType.MAP).toString());
      task.setAppId(job.getAppId());
      task.setJobId(job.getId());
      task.setType(TaskType.MAP);
      task.setState(TaskState.NEW);
      task.setSplitLocations(job.getSplitLocations().get(i));
      task.setSplitSize(job.getSplitSizes().get(i));
      taskStubs.add(task);
    }
    for (int i = 0; i < job.getTotalReduceTasks(); i++) {
      TaskProfile task = Records.newRecord(TaskProfile.class);
      task.setId(MRBuilderUtils.newTaskId(jobId, i, TaskType.REDUCE).toString());
      task.setAppId(job.getAppId());
      task.setJobId(job.getId());
      task.setType(TaskType.REDUCE);
      task.setState(TaskState.NEW);
      taskStubs.add(task);
    }
    return taskStubs;
  }

  private List<TaskProfile> getCurrentTaskProfiles(JobProfile job) {
    MultiEntityPayload ret =
      db.execute(FindByQueryCall.newInstance(TASK, QueryUtils.is("jobId", job.getId()), ID_FIELD, false));
    if (ret == null)
      return null;
    return ret.getEntities();
  }
}
