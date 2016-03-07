package org.apache.hadoop.tools.posum.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.Utils;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
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
        List<AppProfile> apps = null;
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

    private JobProfile readJobConf(String appId, JobId jobId, FileSystem fs, JobConf conf, Path jobSubmitDir) {
        try {
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
            profile.setJobName(conf.getJobName());
            profile.setUser(conf.getUser());
            profile.setInputBytes(inputLength);
            profile.setInputSplits(taskSplitMetaInfo.length);
            //TODO continue populating JobProfile

        } catch (IOException e) {
            throw new YarnRuntimeException(e);
        }
        return null;
    }

    public List<JobProfile> getSubmittedJobsInfo(String appId) {
        ApplicationId actualAppId = Utils.parseApplicationId(appId);
        if (actualAppId == null)
            throw new YarnRuntimeException("[" + getClass().getSimpleName() + "] Wrong ApplicationId supplied for job info retrieval");
        List<JobProfile> profiles = new ArrayList<>(1);
        try {
            FileSystem fs = FileSystem.get(conf);
            Path confPath = MRApps.getStagingAreaDir(conf, UserGroupInformation.getCurrentUser().getUserName());
            confPath = fs.makeQualified(confPath);

            for (RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(confPath, false); iterator.hasNext(); ) {
                LocatedFileStatus status = iterator.next();
                Path filePath = status.getPath();
                if (filePath.toString().contains("" + actualAppId.getClusterTimestamp())) {
                    String jobId = filePath.getName();
                    JobConf jobConf = new JobConf(new Path(status.getPath(), "job.xml"));
                    if (fs.exists(confPath)) {
                        logger.debug("[" + getClass().getSimpleName() + "] Found staging path: " + confPath);
                        profiles.add(readJobConf(appId, Utils.parseJobId(appId, jobId), fs, jobConf, confPath));
                    } else {
                        logger.debug("[" + getClass().getSimpleName() + "] Path does not exist: " + confPath);
                    }
                }
            }

            if (profiles.size() < 1) {
                logger.debug("[" + getClass().getSimpleName() + "] No job configurations found for " + appId);
            }
        } catch (Exception e) {
            logger.debug("[" + getClass().getSimpleName() + "] Could not read job profiles for: " + appId, e);
        }
        return profiles;
    }

    public List<JobProfile> getFinishedJobInfo(String appId) {
        //TODO get from history
        return null;
    }

    public List<JobProfile> getRunningJobsInfo(String appId) {
        List<JobProfile> jobs = null;
        try {
            JSONObject wrapper = restClient.getInfo(RestClient.TrackingUI.AM, "jobs", new String[]{appId});
            if (wrapper.isNull("jobs"))
                return Collections.emptyList();
            JSONArray rawJobs = wrapper.getJSONObject("jobs").getJSONArray("job");
            jobs = new ArrayList<>(rawJobs.length());
            for (int i = 0; i < rawJobs.length(); i++) {
                JSONObject rawJob = rawJobs.getJSONObject(i);
                JobProfile job = new JobProfile(rawJob.getString("id"));
                job.setAppId(appId);
                job.setStartTime(rawJob.getLong("startTime"));
                job.setFinishTime(rawJob.getLong("finishTime"));
                job.setJobName(rawJob.getString("name"));
                job.setUser(rawJob.getString("user"));
                job.setState(rawJob.getString("state"));
                job.setMapProgress(new Double(rawJob.getDouble("mapProgress")).floatValue());
                job.setReduceProgress(new Double(rawJob.getDouble("reduceProgress")).floatValue());
                job.setCompletedMaps(rawJob.getInt("mapsCompleted"));
                job.setCompletedReduces(rawJob.getInt("reducesCompleted"));
                job.setTotalMapTasks(rawJob.getInt("mapsTotal"));
                job.setTotalReduceTasks(rawJob.getInt("reducesTotal"));
                job.setUberized(rawJob.getBoolean("uberized"));
                jobs.add(job);
            }
        } catch (JSONException e) {
            logger.debug("[" + getClass().getSimpleName() + "] Exception parsing jobs from AM", e);
        }
        return jobs;
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
