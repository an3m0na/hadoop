package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils;
import org.apache.hadoop.tools.posum.common.util.communication.RestClient;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.tools.posum.client.data.DatabaseUtils.ID_FIELD;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.util.ShutdownThreadsHelper.shutdownExecutorService;

class TaskInfoCollector {

  private static final Log logger = LogFactory.getLog(TaskInfoCollector.class);

  private HadoopAPIClient api;
  private Database db;
  private ExecutorService executor;

  TaskInfoCollector() {
    executor = Executors.newFixedThreadPool(PosumConfiguration.COLLECTOR_THREAD_COUNT_DEFAULT);
  }

  TaskInfoCollector(Configuration conf, Database db) {
    int poolSize = conf.getInt(PosumConfiguration.COLLECTOR_THREAD_COUNT, PosumConfiguration.COLLECTOR_THREAD_COUNT_DEFAULT);
    executor = Executors.newFixedThreadPool(poolSize);
    this.db = db;
    this.api = new HadoopAPIClient();
  }

  private class TaskDetailFetcher implements Callable<CountersProxy> {

    private TaskProfile task;

    public TaskDetailFetcher(TaskProfile task) {
      this.task = task;
    }

    @Override
    public CountersProxy call() throws Exception {
      try {
        if (!api.addRunningAttemptInfo(task)) {
          return null;
        }
        if (task.isLocal() == null && task.getSplitLocations() != null && task.getHostName() != null) {
          task.setLocal(task.getSplitLocations().contains(task.getHostName()));
        }
        CountersProxy counters = api.getRunningTaskCounters(task.getAppId(), task.getJobId(), task.getId());
        if (counters == null) {
          return null;
        }
        ClusterUtils.updateTaskStatisticsFromCounters(task, counters);
        return counters;
      } catch (Exception e) {
        throw new PosumException("Exception occured while getting details for " + task.getId());
      }
    }
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
      if (task.isLocal() == null && task.getSplitLocations() != null && task.getHostName() != null) {
        task.setLocal(task.getSplitLocations().contains(task.getHostName()));
      }
      CountersProxy counters = api.getFinishedTaskCounters(task.getJobId(), task.getId());
      if (counters != null) {
        ClusterUtils.updateTaskStatisticsFromCounters(task, counters);
        countersList.add(counters);
      }
    }
    return new TaskInfo(tasks, countersList);
  }

  TaskInfo getRunningTaskInfo(AppProfile app, JobProfile job) {
    List<TaskProfile> sortedCurrentTasks = getCurrentTaskProfiles(job);
    if (sortedCurrentTasks == null || sortedCurrentTasks.size() < 1) {
      sortedCurrentTasks = createTaskStubs(job);
    }
    if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
      List<TaskProfile> updatedTasks = api.getRunningTasksInfo(job, sortedCurrentTasks);
      if (updatedTasks == null || updatedTasks.size() == 0) {
        // job might have finished
        return null;
      }
      Collections.sort(updatedTasks, new Comparator<TaskProfile>() {
        @Override
        public int compare(TaskProfile o1, TaskProfile o2) {
          return o1.getId().compareTo(o2.getId());
        }
      });
      List<Future<CountersProxy>> fetchers = new ArrayList<>(sortedCurrentTasks.size());
      for (int i = 0; i < sortedCurrentTasks.size(); i++) {
        if (!sortedCurrentTasks.get(i).isFinished())
          fetchers.add(executor.submit(new TaskDetailFetcher(updatedTasks.get(i))));
      }
      List<CountersProxy> countersList = resolveFetchers(fetchers);
      if (countersList == null) return null;
      return new TaskInfo(updatedTasks, countersList);
    }
    return new TaskInfo(sortedCurrentTasks);
  }

  private List<CountersProxy> resolveFetchers(List<Future<CountersProxy>> fetchers) {
    List<CountersProxy> countersList = new LinkedList<>();
    for (Future<CountersProxy> fetcher : fetchers) {
      try {
        CountersProxy counters = fetcher.get();
        if (counters == null) {
          cancelRemainingFetchers(fetchers);
          return null;
        }
        countersList.add(counters);
      } catch (InterruptedException | ExecutionException e) {
        throw new PosumException(e);
      }
    }
    return countersList;
  }

  private void cancelRemainingFetchers(List<Future<CountersProxy>> fetchers) {
    for (Future<CountersProxy> fetcher : fetchers) {
      try {
        if (!fetcher.isDone())
          fetcher.cancel(true);
      } catch (Exception e) {
        logger.debug("Problem cancelling task detail fetcher", e);
      }
    }
  }

  private List<TaskProfile> createTaskStubs(JobProfile job) {
    List<TaskProfile> taskStubs = new ArrayList<>(job.getTotalMapTasks());
    JobId jobId = ClusterUtils.parseJobId(job.getId());
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

  void shutDown() throws InterruptedException {
    shutdownExecutorService(executor);
  }
}
