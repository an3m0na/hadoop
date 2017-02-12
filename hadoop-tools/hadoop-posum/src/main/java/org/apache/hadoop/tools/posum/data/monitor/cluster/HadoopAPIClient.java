package org.apache.hadoop.tools.posum.data.monitor.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.CountersProxyPBImpl;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopAPIClient {

  private static Log logger = LogFactory.getLog(HadoopAPIClient.class);

  private RestClient restClient;
  private final ObjectMapper mapper;

  static class JobCountersWrapper {
    public CountersProxyPBImpl jobCounters;
  }

  static class TaskCountersWrapper {
    public CountersProxyPBImpl jobTaskCounters;
  }


  HadoopAPIClient() {
    restClient = new RestClient();
    mapper = new ObjectMapper();
  }

  List<AppProfile> getAppsInfo() {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.RM, "cluster/apps", new String[]{});
    JsonNode rawApps = getRawNode(rawString, "apps", "app");
    if (rawApps == null)
      return Collections.emptyList();
    List<AppProfile> apps = new ArrayList<>(rawApps.size());
    for (int i = 0; i < rawApps.size(); i++) {
      ObjectNode rawApp = (ObjectNode) rawApps.get(i);
      AppProfile app = Records.newRecord(AppProfile.class);
      app.setId(rawApp.get("id").asText());
      app.setStartTime(rawApp.get("startedTime").asLong());
      app.setFinishTime(rawApp.get("finishedTime").asLong());
      app.setName(rawApp.get("name").asText());
      app.setUser(rawApp.get("user").asText());
      app.setQueue(rawApp.get("queue").asText());
      app.setState(YarnApplicationState.valueOf(rawApp.get("state").asText()));
      app.setStatus(FinalApplicationStatus.valueOf(rawApp.get("finalStatus").asText()));
      app.setTrackingUI(RestClient.TrackingUI.fromLabel(rawApp.get("trackingUI").asText()));
      apps.add(app);
    }
    return apps;
  }

  private JsonNode getRawNode(String rawString, String... pathSections) {
    if (rawString == null)
      return null;
    try {
      JsonNode wrapper = mapper.readTree(rawString);
      for (String pathSection : pathSections) {
        if (!wrapper.has(pathSection))
          return null;
        wrapper = wrapper.get(pathSection);
        if (wrapper == null)
          return null;
      }
      return wrapper;
    } catch (IOException e) {
      logger.debug("Exception parsing JSON string", e);
      return null;
    }
  }

  boolean checkAppFinished(AppProfile app) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.RM, "cluster/apps/%s", new String[]{app.getId()});
    JsonNode rawApp = getRawNode(rawString, "app");
    if (rawApp == null)
      return false;
    if (RestClient.TrackingUI.HISTORY.equals(
      RestClient.TrackingUI.fromLabel(rawApp.get("trackingUI").asText()))) {
      app.setTrackingUI(RestClient.TrackingUI.HISTORY);
      app.setFinishTime(rawApp.get("finishedTime").asLong());
      app.setState(YarnApplicationState.valueOf(rawApp.get("state").asText()));
      app.setStatus(FinalApplicationStatus.valueOf(rawApp.get("finalStatus").asText()));
      return true;
    }
    return false;
  }

  JobProfile getFinishedJobInfo(String appId) {
    ApplicationId realAppId = Utils.parseApplicationId(appId);
    JobId expectedRealJobId = Records.newRecord(JobId.class);
    expectedRealJobId.setAppId(realAppId);
    expectedRealJobId.setId(realAppId.getId());
    String expectedJobId = expectedRealJobId.toString();

    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.HISTORY, "jobs", new String[]{});
    JsonNode rawJobs = getRawNode(rawString, "jobs", "job");
    if (rawJobs == null)
      return null;
    String lastRelatedJobId = null;
    for (int i = 0; i < rawJobs.size(); i++) {
      //FIXME not so sure this is the way to make the connection between apps and historical jobs
      // it chooses the job with its id the same as the appId, and, if none have it,
      // the one with an identical timestamp with the appId
      String jobId = rawJobs.get(i).get("id").asText();
      if (expectedJobId.equals(jobId))
        return getFinishedJobInfo(appId, jobId, null);
      String[] parts = jobId.split("_");
      if (realAppId.getClusterTimestamp() == Long.parseLong(parts[1])) {
        lastRelatedJobId = jobId;
      }
    }
    return getFinishedJobInfo(appId, lastRelatedJobId, null);

  }

  JobProfile getFinishedJobInfo(String appId, String jobId, JobProfile previousJob) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.HISTORY, "jobs/%s", new String[]{jobId});
    JsonNode rawJob = getRawNode(rawString, "job");
    if (rawJob == null)
      return null;
    JobProfile job = previousJob != null ? previousJob : Records.newRecord(JobProfile.class);
    job.setId(rawJob.get("id").asText());
    job.setAppId(appId);
    job.setSubmitTime(rawJob.get("submitTime").asLong());
    job.setStartTime(rawJob.get("startTime").asLong());
    job.setFinishTime(rawJob.get("finishTime").asLong());
    job.setName(rawJob.get("name").asText());
    job.setUser(rawJob.get("user").asText());
    job.setQueue(rawJob.get("queue").asText());
    job.setState(JobState.valueOf(rawJob.get("state").asText()));
    job.setTotalMapTasks(rawJob.get("mapsTotal").asInt());
    job.setTotalReduceTasks(rawJob.get("reducesTotal").asInt());
    job.setCompletedMaps(rawJob.get("mapsCompleted").asInt());
    job.setCompletedReduces(rawJob.get("reducesCompleted").asInt());
    job.setMapProgress(100 * (job.getTotalMapTasks() <= 0 ? 1f : job.getCompletedMaps() / job.getTotalMapTasks()));
    job.setReduceProgress(100 * (job.getTotalReduceTasks() <= 0 ? 1f : job.getCompletedReduces() / job.getTotalReduceTasks()));
    job.setUberized(rawJob.get("uberized").asBoolean());
    job.setAvgMapDuration(rawJob.get("avgMapTime").asLong());
    job.setAvgReduceTime(rawJob.get("avgReduceTime").asLong());
    job.setAvgShuffleTime(rawJob.get("avgShuffleTime").asLong());
    job.setAvgMergeTime(rawJob.get("avgMergeTime").asLong());
    job.setAvgReduceDuration(job.getAvgShuffleTime() + job.getAvgMergeTime() + job.getAvgReduceTime());
    return job;
  }

  JobProfile getRunningJobInfo(String appId, String queue, JobProfile previousJob) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.AM, "jobs", new String[]{appId});
    JsonNode rawJobs = getRawNode(rawString, "jobs", "job");
    if (rawJobs == null)
      return null;
    if (rawJobs.size() != 1)
      throw new PosumException("Unexpected number of jobs for mapreduce app " + appId);
    JsonNode rawJob = rawJobs.get(0);
    JobProfile job = previousJob != null ? previousJob : Records.newRecord(JobProfile.class);
    job.setId(rawJob.get("id").asText());
    job.setAppId(appId);
    job.setName(rawJob.get("name").asText());
    job.setQueue(queue);
    job.setUser(rawJob.get("user").asText());
    job.setStartTime(rawJob.get("startTime").asLong());
    job.setFinishTime(rawJob.get("finishTime").asLong());
    job.setState(JobState.valueOf(rawJob.get("state").asText()));
    job.setMapProgress(new Double(rawJob.get("mapProgress").asDouble()).floatValue());
    job.setReduceProgress(new Double(rawJob.get("reduceProgress").asDouble()).floatValue());
    job.setCompletedMaps(rawJob.get("mapsCompleted").asInt());
    job.setCompletedReduces(rawJob.get("reducesCompleted").asInt());
    job.setTotalMapTasks(rawJob.get("mapsTotal").asInt());
    job.setTotalReduceTasks(rawJob.get("reducesTotal").asInt());
    job.setUberized(rawJob.get("uberized").asBoolean());
    return job;
  }

  List<TaskProfile> getFinishedTasksInfo(String jobId) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.HISTORY, "jobs/%s/tasks", new String[]{jobId});
    JsonNode rawTasks = getRawNode(rawString, "tasks", "task");
    if (rawTasks == null)
      return Collections.emptyList();
    List<TaskProfile> tasks = new ArrayList<>(rawTasks.size());
    for (int i = 0; i < rawTasks.size(); i++) {
      JsonNode rawTask = rawTasks.get(i);
      TaskProfile task = Records.newRecord(TaskProfile.class);
      task.setId(rawTask.get("id").asText());
      task.setJobId(jobId);
      task.setType(TaskType.valueOf(rawTask.get("type").asText()));
      task.setStartTime(rawTask.get("startTime").asLong());
      task.setFinishTime(rawTask.get("finishTime").asLong());
      task.setReportedProgress(new Double(rawTask.get("progress").asDouble()).floatValue());
      task.setState(TaskState.valueOf(rawTask.get("state").asText()));
      task.setSuccessfulAttempt(rawTask.get("successfulAttempt").asText());
      tasks.add(task);
    }
    return tasks;
  }

  List<TaskProfile> getRunningTasksInfo(JobProfile job) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.AM, "jobs/%s/tasks", new String[]{job.getAppId(), job.getId()});
    JsonNode rawTasks = getRawNode(rawString, "tasks", "task");
    if (rawTasks == null)
      return Collections.emptyList();
    List<TaskProfile> tasks = new ArrayList<>(rawTasks.size());
    for (int i = 0; i < rawTasks.size(); i++) {
      JsonNode rawTask = rawTasks.get(i);
      TaskProfile task = Records.newRecord(TaskProfile.class);
      task.setId(rawTask.get("id").asText());
      task.setAppId(job.getAppId());
      task.setJobId(job.getId());
      task.setType(TaskType.valueOf(rawTask.get("type").asText()));
      task.setStartTime(rawTask.get("startTime").asLong());
      task.setFinishTime(rawTask.get("finishTime").asLong());
      task.setReportedProgress(new Double(rawTask.get("progress").asDouble()).floatValue());
      task.setState(TaskState.valueOf(rawTask.get("state").asText()));
      tasks.add(task);
    }
    return tasks;
  }

  boolean addRunningAttemptInfo(TaskProfile task) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.AM, "jobs/%s/tasks/%s/attempts", new String[]{task.getAppId(), task.getJobId(), task.getId()});
    JsonNode rawAttempts = getRawNode(rawString, "taskAttempts", "taskAttempt");
    if (rawAttempts == null)
      return false;
    for (int i = 0; i < rawAttempts.size(); i++) {
      JsonNode rawAttempt = rawAttempts.get(i);
      String state = rawAttempt.get("state").asText();
      if (!TaskState.FAILED.name().equals(state) && !TaskState.KILLED.name().equals(state)) {
        if (rawAttempt.has("elapsedShuffleTime"))
          task.setShuffleTime(rawAttempt.get("elapsedShuffleTime").asLong());
        if (rawAttempt.has("elapsedMergeTime"))
          task.setMergeTime(rawAttempt.get("elapsedMergeTime").asLong());
        if (rawAttempt.has("elapsedReduceTime"))
          task.setReduceTime(rawAttempt.get("elapsedReduceTime").asLong());
        if (rawAttempt.has("nodeHttpAddress")) {
          String[] addressParts = rawAttempt.get("nodeHttpAddress").asText().split(":");
          String host = addressParts.length > 2 ? addressParts[1] : addressParts[0];
          task.setHttpAddress(host.trim());
        }
        return true;
      }
    }
    return true;
  }

  void addFinishedAttemptInfo(TaskProfile task) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.HISTORY, "jobs/%s/tasks/%s/attempts", new String[]{task.getJobId(), task.getId()});
    JsonNode rawAttempts = getRawNode(rawString, "taskAttempts", "taskAttempt");
    if (rawAttempts == null)
      return;
    for (int i = 0; i < rawAttempts.size(); i++) {
      JsonNode rawAttempt = rawAttempts.get(i);
      if (TaskState.SUCCEEDED.name().equals(rawAttempt.get("state").asText())) {
        if (rawAttempt.has("elapsedShuffleTime"))
          task.setShuffleTime(rawAttempt.get("elapsedShuffleTime").asLong());
        if (rawAttempt.has("elapsedMergeTime"))
          task.setMergeTime(rawAttempt.get("elapsedMergeTime").asLong());
        if (rawAttempt.has("elapsedReduceTime"))
          task.setReduceTime(rawAttempt.get("elapsedReduceTime").asLong());
        if (rawAttempt.has("nodeHttpAddress")) {
          String[] addressParts = rawAttempt.get("nodeHttpAddress").asText().split(":");
          String host = addressParts.length > 2 ? addressParts[1] : addressParts[0];
          task.setHttpAddress(host.trim());
        }
        return;
      }
    }
  }

  JobConfProxy getFinishedJobConf(String jobId) {
    String rawString = restClient.getInfo(String.class,
      RestClient.TrackingUI.HISTORY, "jobs/%s/conf", new String[]{jobId});
    JsonNode rawConf = getRawNode(rawString, "conf");
    if (rawConf == null)
      return null;
    JobConfProxy conf = Records.newRecord(JobConfProxy.class);
    conf.setConfPath(rawConf.get("path").asText());
    conf.setId(jobId);
    JsonNode properties = rawConf.get("property");
    Map<String, String> map = new HashMap<>(properties.size());
    for (int i = 0; i < properties.size(); i++) {
      JsonNode property = properties.get(i);
      map.put(property.get("name").asText(), property.get("value").asText());
    }
    conf.setPropertyMap(map);
    return conf;
  }

  CountersProxy getRunningJobCounters(String appId, String jobId) {
    try {
      JobCountersWrapper wrapper = restClient.getInfo(JobCountersWrapper.class,
        RestClient.TrackingUI.AM, "jobs/%s/counters", new String[]{appId, jobId});
      if (wrapper == null)
        return null;
      return wrapper.jobCounters;
    } catch (Exception e) {
      logger.debug("Exception parsing counters from AM", e);
    }
    return null;
  }

  CountersProxy getRunningTaskCounters(String appId, String jobId, String taskId) {
    try {
      TaskCountersWrapper wrapper = restClient.getInfo(TaskCountersWrapper.class,
        RestClient.TrackingUI.AM, "jobs/%s/tasks/%s/counters", new String[]{appId, jobId, taskId});
      if (wrapper == null)
        return null;
      return wrapper.jobTaskCounters;
    } catch (Exception e) {
      logger.debug("Exception parsing counters from AM", e);
    }
    return null;
  }


  CountersProxy getFinishedJobCounters(String jobId) {
    try {
      JobCountersWrapper wrapper = restClient.getInfo(JobCountersWrapper.class,
        RestClient.TrackingUI.HISTORY, "jobs/%s/counters", new String[]{jobId});
      if (wrapper == null)
        return null;
      return wrapper.jobCounters;
    } catch (Exception e) {
      logger.debug("Exception parsing counters from HISTORY", e);
    }
    return null;
  }

  CountersProxy getFinishedTaskCounters(String jobId, String taskId) {
    try {
      TaskCountersWrapper wrapper = restClient.getInfo(TaskCountersWrapper.class,
        RestClient.TrackingUI.HISTORY, "jobs/%s/tasks/%s/counters", new String[]{jobId, taskId});
      if (wrapper == null)
        return null;
      return wrapper.jobTaskCounters;
    } catch (Exception e) {
      logger.debug("Exception parsing counters from HISTORY", e);
    }
    return null;
  }
}
