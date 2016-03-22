package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.task.reduce.ExceptionReporter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.Utils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.util.*;

/**
 * Created by ane on 3/7/16.
 */
public class SystemInfoCollector implements Configurable {

    private static Log logger = LogFactory.getLog(SystemInfoCollector.class);

    private RestClient restClient;
    private Configuration conf;

    public SystemInfoCollector(Configuration conf) {
        restClient = new RestClient();
        this.conf = conf;
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public List<AppProfile> getAppsInfo() {
        List<AppProfile> apps = Collections.emptyList();
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.RM, "cluster/apps", new String[]{});
            if (wrapper.isNull("apps"))
                return Collections.emptyList();
            JSONArray rawApps = wrapper.getJSONObject("apps").getJSONArray("app");
            apps = new ArrayList<>(rawApps.length());
            for (int i = 0; i < rawApps.length(); i++) {
                JSONObject rawApp = rawApps.getJSONObject(i);
                AppProfile app = new AppProfile(rawApp.getString("id"));
                app.setStartTime(rawApp.getLong("startedTime"));
                app.setFinishTime(rawApp.getLong("finishedTime"));
                app.setName(rawApp.getString("name"));
                app.setUser(rawApp.getString("user"));
                app.setState(rawApp.getString("state"));
                app.setStatus(rawApp.getString("finalStatus"));
                app.setTrackingUI(rawApp.getString("trackingUI"));
                apps.add(app);
            }
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing apps", e);
        }
        return apps;
    }

    private JobProfile readJobConf(String appId, JobId jobId, FileSystem fs, JobConf conf, Path jobSubmitDir) throws IOException {
        JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
                TypeConverter.fromYarn(jobId), fs,
                conf,
                jobSubmitDir);

        long inputLength = 0;
        for (JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo : taskSplitMetaInfo) {
            inputLength += aTaskSplitMetaInfo.getInputDataLength();
        }

        logger.debug("[" + getClass().getSimpleName() + "] Input splits: " + taskSplitMetaInfo.length);
        logger.debug("[" + getClass().getSimpleName() + "] Total input size: " + inputLength);

        JobProfile profile = new JobProfile(jobId.toString());
        profile.setAppId(appId);
        profile.setName(conf.getJobName());
        profile.setUser(conf.getUser());
        profile.setInputBytes(inputLength);
        profile.setInputSplits(taskSplitMetaInfo.length);
        //TODO continue populating JobProfile
        return profile;
    }

    public JobProfile getSubmittedJobInfo(String appId) throws IOException {
        final ApplicationId actualAppId = Utils.parseApplicationId(appId);
        FileSystem fs = FileSystem.get(conf);
        Path confPath = MRApps.getStagingAreaDir(conf, UserGroupInformation.getCurrentUser().getUserName());
        confPath = fs.makeQualified(confPath);

        logger.debug("[" + getClass().getSimpleName() + "] Looking in staging path: " + confPath);
        FileStatus[] statuses = fs.listStatus(confPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.toString().contains("job_" + actualAppId.getClusterTimestamp());
            }
        });

        if (statuses.length != 1)
            throw new POSUMException("No job profile directory for: " + appId);

        Path jobConfDir = statuses[0].getPath();
        logger.debug("[" + getClass().getSimpleName() + "] Checking file path: " + jobConfDir);
        String jobId = jobConfDir.getName();
        JobConf jobConf = new JobConf(new Path(jobConfDir, "job.xml"));
        //DANGER We assume there can only be one job / application
        return readJobConf(appId, Utils.parseJobId(appId, jobId), fs, jobConf, jobConfDir);

    }

    public JobProfile getFinishedJobInfo(String appId) {
        ApplicationId realAppId = Utils.parseApplicationId(appId);
        JobId expectedRealJobId = Records.newRecord(JobId.class);
        expectedRealJobId.setAppId(realAppId);
        expectedRealJobId.setId(realAppId.getId());
        String expectedJobId = expectedRealJobId.toString();
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.HISTORY, "jobs", new String[]{});
            if (wrapper.isNull("jobs"))
                return null;
            JSONArray rawJobs = wrapper.getJSONObject("jobs").getJSONArray("job");
            String lastRelatedJobId = null;
            for (int i = 0; i < rawJobs.length(); i++) {
                //FIXME not so sure this is the way to make the connection between apps and historical jobs
                // it chooses the job with its id the same as the appId, and, if none have it,
                // the one with an identical timestamp with the appId
                String jobId = rawJobs.getJSONObject(i).getString("id");
                if (expectedJobId.equals(jobId))
                    return getFinishedJobInfo(appId, jobId);
                String[] parts = jobId.split("_");
                if (realAppId.getClusterTimestamp() == Long.parseLong(parts[1])) {
                    lastRelatedJobId = jobId;
                }
            }
            return getFinishedJobInfo(appId, lastRelatedJobId);
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing jobs from HISTORY", e);
        }
        return null;
    }

    public JobProfile getFinishedJobInfo(String appId, String jobId) {
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.HISTORY, "jobs/%s", new String[]{jobId});
            if (wrapper.isNull("job"))
                return null;
            JSONObject rawJob = wrapper.getJSONObject("job");
            JobProfile job = new JobProfile(rawJob.getString("id"));
            job.setAppId(appId);
            job.setSubmitTime(rawJob.getLong("submitTime"));
            job.setStartTime(rawJob.getLong("startTime"));
            job.setFinishTime(rawJob.getLong("finishTime"));
            job.setName(rawJob.getString("name"));
            job.setUser(rawJob.getString("user"));
            job.setState(rawJob.getString("state"));
            job.setCompletedMaps(rawJob.getInt("mapsCompleted"));
            job.setCompletedReduces(rawJob.getInt("reducesCompleted"));
            job.setTotalMapTasks(rawJob.getInt("mapsTotal"));
            job.setTotalReduceTasks(rawJob.getInt("reducesTotal"));
            job.setUberized(rawJob.getBoolean("uberized"));
            job.setAvgMapDuration(rawJob.getInt("avgMapTime"));
            job.setAvgReduceDuration(rawJob.getInt("avgReduceTime"));
            job.setAvgShuffleDuration(rawJob.getInt("avgShuffleTime"));
            job.setAvgMergeDuration(rawJob.getInt("avgMergeTime"));
            return job;
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing jobs from HISTORY", e);
        }
        return null;
    }

    public JobProfile getRunningJobInfo(String appId, JobProfile previousJob) {
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.AM, "jobs", new String[]{appId});
            if (wrapper.isNull("jobs"))
                return null;
            JSONArray rawJobs = wrapper.getJSONObject("jobs").getJSONArray("job");
            if (rawJobs.length() != 1)
                throw new POSUMException("Unexpected number of jobs for mapreduce app " + appId);
            JSONObject rawJob = rawJobs.getJSONObject(0);
            JobProfile job = new JobProfile(rawJob.getString("id"));
            job.setAppId(appId);
            job.setStartTime(rawJob.getLong("startTime"));
            job.setFinishTime(rawJob.getLong("finishTime"));
            job.setName(rawJob.getString("name"));
            job.setUser(rawJob.getString("user"));
            job.setState(rawJob.getString("state"));
            job.setMapProgress(new Double(rawJob.getDouble("mapProgress")).floatValue());
            job.setReduceProgress(new Double(rawJob.getDouble("reduceProgress")).floatValue());
            job.setCompletedMaps(rawJob.getInt("mapsCompleted"));
            job.setCompletedReduces(rawJob.getInt("reducesCompleted"));
            job.setTotalMapTasks(rawJob.getInt("mapsTotal"));
            job.setTotalReduceTasks(rawJob.getInt("reducesTotal"));
            job.setUberized(rawJob.getBoolean("uberized"));
            if (previousJob != null) {
                job.setInputBytes(previousJob.getInputBytes());
                job.setInputSplits(previousJob.getInputSplits());
            }
            return job;
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing jobs from AM", e);
        }
        return null;
    }

    public List<TaskProfile> getFinishedTasksInfo(String appId, String jobId) {
        List<TaskProfile> tasks = Collections.emptyList();
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.HISTORY, "jobs/%s/tasks", new String[]{jobId});
            if (wrapper.isNull("tasks"))
                return Collections.emptyList();
            JSONArray rawTasks = wrapper.getJSONObject("tasks").getJSONArray("task");
            tasks = new ArrayList<>(rawTasks.length());
            for (int i = 0; i < rawTasks.length(); i++) {
                JSONObject rawTask = rawTasks.getJSONObject(i);
                TaskProfile task = new TaskProfile(rawTask.getString("id"));
                task.setJobId(jobId);
                task.setType(rawTask.getString("type"));
                task.setStartTime(rawTask.getLong("startTime"));
                task.setFinishTime(rawTask.getLong("finishTime"));
                task.setReportedProgress(new Double(rawTask.getDouble("progress")).floatValue());
                task.setSuccessfulAttempt(rawTask.getString("successfulAttempt"));
                tasks.add(task);
            }
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing tasks from HISTORY", e);
        }
        return tasks;
    }

    public List<TaskProfile> getRunningTasksInfo(JobProfile job) {
        List<TaskProfile> tasks = Collections.emptyList();
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.AM, "jobs/%s/tasks", new String[]{job.getAppId(), job.getId()});
            if (wrapper.isNull("tasks"))
                return Collections.emptyList();
            JSONArray rawTasks = wrapper.getJSONObject("tasks").getJSONArray("task");
            tasks = new ArrayList<>(rawTasks.length());
            for (int i = 0; i < rawTasks.length(); i++) {
                JSONObject rawTask = rawTasks.getJSONObject(i);
                TaskProfile task = new TaskProfile(rawTask.getString("id"));
                task.setAppId(job.getAppId());
                task.setType(rawTask.getString("type"));
                task.setStartTime(rawTask.getLong("startTime"));
                task.setFinishTime(rawTask.getLong("finishTime"));
                task.setReportedProgress(new Double(rawTask.getDouble("progress")).floatValue());
                task.setExpectedInputBytes(job.getAvgSplitSize());
                tasks.add(task);
            }
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing tasks from AM", e);
        }
        return tasks;
    }

    public Map<String, String> getJobConfProperties(String appId, String jobId, Map<String, String> requested) {
        Map<String, String> ret = new HashMap<>(requested.size());
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.AM, "jobs/%s/conf", new String[]{
                    appId,
                    jobId
            });
            try {
                JSONArray properties = wrapper.getJSONObject("conf").getJSONArray("property");
                for (int i = 0; i < properties.length(); i++) {
                    JSONObject property = properties.getJSONObject(i);
                    String requestedLabel = requested.get(property.getString("name"));
                    if (requestedLabel != null)
                        ret.put(requestedLabel, property.getString("value"));
                }
            } catch (JSONException e) {
                logger.debug("[" + getClass().getSimpleName() + "] Exception parsing job conf", e);
            }
        } catch (WebApplicationException e) {
            logger.error("[" + getClass().getSimpleName() + "] Could not get job conf for " + jobId, e);
        }
        return ret;

    }
}
