package org.apache.hadoop.tools.posum.database.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.CountersProxyPBImpl;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.util.*;

/**
 * Created by ane on 3/7/16.
 */
public class HadoopAPIClient {

    private static Log logger = LogFactory.getLog(HadoopAPIClient.class);

    private final RestClient restClient;
    private final Configuration conf;
    private final ObjectMapper mapper;

    private static class JobCountersWrapper {
        public CountersProxyPBImpl jobCounters;
    }

    private static class TaskCountersWrapper {
        public CountersProxyPBImpl jobTaskCounters;
    }


    public HadoopAPIClient(Configuration conf) {
        restClient = new RestClient();
        this.conf = conf;
        mapper = new ObjectMapper();
    }

    public List<AppProfile> getAppsInfo() {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.RM, "cluster/apps", new String[]{});
            if (rawString == null)
                return Collections.emptyList();
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("apps"))
                return Collections.emptyList();
            wrapper = wrapper.get("apps");
            if (wrapper.isNull())
                return Collections.emptyList();
            JsonNode rawApps = wrapper.get("app");
            List<AppProfile> apps;
            apps = new ArrayList<>(rawApps.size());
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
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return Collections.emptyList();
        }
    }

    public JobProfile getFinishedJobInfo(String appId) {
        ApplicationId realAppId = Utils.parseApplicationId(appId);
        JobId expectedRealJobId = Records.newRecord(JobId.class);
        expectedRealJobId.setAppId(realAppId);
        expectedRealJobId.setId(realAppId.getId());
        String expectedJobId = expectedRealJobId.toString();
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.HISTORY, "jobs", new String[]{});
            if (rawString == null)
                return null;
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("jobs"))
                return null;
            wrapper = wrapper.get("jobs");
            if (wrapper.isNull())
                return null;
            JsonNode rawJobs = wrapper.get("job");
            String lastRelatedJobId = null;
            for (int i = 0; i < rawJobs.size(); i++) {
                //FIXME not so sure this is the way to make the connection between apps and historical jobs
                // it chooses the job with its id the same as the appId, and, if none have it,
                // the one with an identical timestamp with the appId
                String jobId = rawJobs.get(i).get("id").asText();
                if (expectedJobId.equals(jobId))
                    return getFinishedJobInfo(appId, jobId);
                String[] parts = jobId.split("_");
                if (realAppId.getClusterTimestamp() == Long.parseLong(parts[1])) {
                    lastRelatedJobId = jobId;
                }
            }
            return getFinishedJobInfo(appId, lastRelatedJobId);
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return null;
        }
    }

    public JobProfile getFinishedJobInfo(String appId, String jobId) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.HISTORY, "jobs/%s", new String[]{jobId});
            if (rawString == null)
                return null;
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("job"))
                return null;
            JsonNode rawJob = wrapper.get("job");
            if (rawJob.isNull())
                return null;
            JobProfile job = Records.newRecord(JobProfile.class);
            job.setId(rawJob.get("id").asText());
            job.setAppId(appId);
            job.setSubmitTime(rawJob.get("submitTime").asLong());
            job.setStartTime(rawJob.get("startTime").asLong());
            job.setFinishTime(rawJob.get("finishTime").asLong());
            job.setName(rawJob.get("name").asText());
            job.setUser(rawJob.get("user").asText());
            job.setQueue(rawJob.get("queue").asText());
            job.setState(JobState.valueOf(rawJob.get("state").asText()));
            job.setCompletedMaps(rawJob.get("mapsCompleted").asInt());
            job.setCompletedReduces(rawJob.get("reducesCompleted").asInt());
            job.setTotalMapTasks(rawJob.get("mapsTotal").asInt());
            job.setTotalReduceTasks(rawJob.get("reducesTotal").asInt());
            job.setUberized(rawJob.get("uberized").asBoolean());
            job.setAvgMapDuration(rawJob.get("avgMapTime").asLong());
            job.setAvgReduceTime(rawJob.get("avgReduceTime").asLong());
            job.setAvgShuffleTime(rawJob.get("avgShuffleTime").asLong());
            job.setAvgMergeTime(rawJob.get("avgMergeTime").asLong());
            return job;
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return null;
        }
    }

    public JobProfile getRunningJobInfo(String appId, String queue, JobProfile previousJob) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.AM, "jobs", new String[]{appId});
            if (rawString == null)
                return null;
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("jobs"))
                return null;
            wrapper = wrapper.get("jobs");
            if (wrapper.isNull())
                return null;
            JsonNode rawJobs = wrapper.get("job");
            if (rawJobs.size() != 1)
                throw new POSUMException("Unexpected number of jobs for mapreduce app " + appId);
            JsonNode rawJob = rawJobs.get(0);
            JobProfile job = previousJob != null ? previousJob : Records.newRecord(JobProfile.class);
            job.setAppId(appId);
            job.setQueue(queue);
            job.setSubmitTime(rawJob.get("startTime").asLong());
            job.setStartTime(job.getSubmitTime());
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
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return null;
        }
    }

    public List<TaskProfile> getFinishedTasksInfo(String jobId) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.HISTORY, "jobs/%s/tasks", new String[]{jobId});
            if (rawString == null)
                return Collections.emptyList();
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("tasks"))
                return Collections.emptyList();
            wrapper = wrapper.get("tasks");
            if (wrapper.isNull())
                return Collections.emptyList();
            JsonNode rawTasks = wrapper.get("task");
            List<TaskProfile> tasks = new ArrayList<>(rawTasks.size());
            for (int i = 0; i < rawTasks.size(); i++) {
                JsonNode rawTask = rawTasks.get(i);
                TaskProfile task = Records.newRecord(TaskProfile.class);
                task.setId(rawTask.get("id").asText());
                task.setJobId(jobId);
                task.setType(rawTask.get("type").asText());
                task.setStartTime(rawTask.get("startTime").asLong());
                task.setFinishTime(rawTask.get("finishTime").asLong());
                task.setReportedProgress(new Double(rawTask.get("progress").asDouble()).floatValue());
                task.setSuccessfulAttempt(rawTask.get("successfulAttempt").asText());
                tasks.add(task);
            }
            return tasks;
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return Collections.emptyList();
        }
    }

    public List<TaskProfile> getRunningTasksInfo(JobProfile job) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.AM, "jobs/%s/tasks", new String[]{job.getAppId(), job.getId()});
            if (rawString == null)
                return Collections.emptyList();
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("tasks"))
                return Collections.emptyList();
            wrapper = wrapper.get("tasks");
            if (wrapper.isNull())
                return Collections.emptyList();
            JsonNode rawTasks = wrapper.get("task");
            List<TaskProfile> tasks = new ArrayList<>(rawTasks.size());
            for (int i = 0; i < rawTasks.size(); i++) {
                JsonNode rawTask = rawTasks.get(i);
                TaskProfile task = Records.newRecord(TaskProfile.class);
                task.setId(rawTask.get("id").asText());
                task.setAppId(job.getAppId());
                task.setJobId(job.getId());
                task.setType(rawTask.get("type").asText());
                task.setStartTime(rawTask.get("startTime").asLong());
                task.setFinishTime(rawTask.get("finishTime").asLong());
                task.setReportedProgress(new Double(rawTask.get("progress").asDouble()).floatValue());
                tasks.add(task);
            }
            return tasks;
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return Collections.emptyList();
        }
    }

    public void addRunningAttemptInfo(TaskProfile task) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.AM, "jobs/%s/tasks/%s/attempts", new String[]{task.getAppId(), task.getJobId(), task.getId()});
            if (rawString == null)
                return;
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("taskAttempts"))
                return;
            wrapper = wrapper.get("taskAttempts");
            if (wrapper.isNull())
                return;
            JsonNode rawAttempts = wrapper.get("taskAttempt");
            for (int i = 0; i < rawAttempts.size(); i++) {
                JsonNode rawAttempt = rawAttempts.get(i);
                String state = rawAttempt.get("state").asText();
                if (TaskState.RUNNING.name().equals(state) || TaskState.SUCCEEDED.name().equals(state)) {
                    if (rawAttempt.has("elapsedShuffleTime"))
                        task.setShuffleTime(rawAttempt.get("elapsedShuffleTime").asLong());
                    if (rawAttempt.has("elapsedMergeTime"))
                        task.setMergeTime(rawAttempt.get("elapsedMergeTime").asLong());
                    if (rawAttempt.has("elapsedReduceTime"))
                        task.setReduceTime(rawAttempt.get("elapsedReduceTime").asLong());
                    if (rawAttempt.has("nodeHttpAddress"))
                        task.setHttpAddress(rawAttempt.get("nodeHttpAddress").asText());
                    return;
                }
            }
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
        }
    }

    public void addFinishedAttemptInfo(TaskProfile task) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.HISTORY, "jobs/%s/tasks/%s/attempts", new String[]{task.getJobId(), task.getId()});
            if (rawString == null)
                return;
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("taskAttempts"))
                return;
            wrapper = wrapper.get("taskAttempts");
            if (wrapper.isNull())
                return;
            JsonNode rawAttempts = wrapper.get("taskAttempt");
            for (int i = 0; i < rawAttempts.size(); i++) {
                JsonNode rawAttempt = rawAttempts.get(i);
                if (TaskState.SUCCEEDED.name().equals(rawAttempt.get("state").asText())) {
                    if (rawAttempt.has("elapsedShuffleTime"))
                        task.setShuffleTime(rawAttempt.get("elapsedShuffleTime").asLong());
                    if (rawAttempt.has("elapsedMergeTime"))
                        task.setMergeTime(rawAttempt.get("elapsedMergeTime").asLong());
                    if (rawAttempt.has("elapsedReduceTime"))
                        task.setReduceTime(rawAttempt.get("elapsedReduceTime").asLong());
                    if (rawAttempt.has("nodeHttpAddress"))
                        task.setHttpAddress(rawAttempt.get("nodeHttpAddress").asText());
                    return;
                }
            }
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
        }
    }

    public CountersProxy getRunningJobCounters(String appId, String jobId) {
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

    public CountersProxy getRunningTaskCounters(String appId, String jobId, String taskId) {
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

    public Map<String, String> getJobConfProperties(String appId, String jobId, Map<String, String> requested) {

        try {
            Map<String, String> ret = new HashMap<>(requested.size());
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.AM, "jobs/%s/conf", new String[]{appId, jobId});
            if (rawString == null)
                return null;
            JsonNode wrapper = mapper.readTree(rawString);
            JsonNode properties = wrapper.get("conf").get("property");
            for (int i = 0; i < properties.size(); i++) {
                JsonNode property = properties.get(i);
                String requestedLabel = requested.get(property.get("name").asText());
                if (requestedLabel != null)
                    ret.put(requestedLabel, property.get("value").asText());
            }
            return ret;
        } catch (WebApplicationException e) {
            logger.error("Could not get job conf for " + jobId, e);
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
        }
        return null;
    }

    public JobConfProxy getFinishedJobConf(String jobId) {
        try {
            String rawString = restClient.getInfo(String.class,
                    RestClient.TrackingUI.HISTORY, "jobs/%s/conf", new String[]{jobId});
            if (rawString == null)
                return null;
            JsonNode wrapper = mapper.readTree(rawString);
            if (!wrapper.has("conf"))
                return null;
            wrapper = wrapper.get("conf");
            if (wrapper.isNull())
                return null;
            JobConfProxy conf = Records.newRecord(JobConfProxy.class);
            conf.setConfPath(wrapper.get("path").asText());
            conf.setId(jobId);
            JsonNode properties = wrapper.get("property");
            Map<String, String> map = new HashMap<>(properties.size());
            for (int i = 0; i < properties.size(); i++) {
                JsonNode property = properties.get(i);
                map.put(property.get("name").asText(), property.get("value").asText());
            }
            conf.setPropertyMap(map);
            return conf;
        } catch (IOException e) {
            logger.debug("Exception parsing JSON string", e);
            return null;
        }
    }

    public CountersProxy getFinishedJobCounters(String jobId) {
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

    public CountersProxy getFinishedTaskCounters(String jobId, String taskId) {
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
