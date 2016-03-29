package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.Utils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.AppProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.JobProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.TaskProfilePBImpl;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.store.DataTransaction;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * Created by ane on 2/4/16.
 */
class ClusterInfoCollector {

    private static Log logger = LogFactory.getLog(ClusterInfoCollector.class);

    private Set<String> running = new HashSet<>();
    private Set<String> finished = new HashSet<>();
    private DataStore dataStore;
    private HadoopAPIClient collector;
    private Configuration conf;
    private boolean historyEnabled;

    ClusterInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.collector = new HadoopAPIClient(conf);
        this.conf = conf;
        this.historyEnabled = conf.getBoolean(POSUMConfiguration.MONITOR_KEEP_HISTORY,
                POSUMConfiguration.MONITOR_KEEP_HISTORY_DEFAULT);
    }

    void collect() {
        List<AppProfile> apps = collector.getAppsInfo();
        logger.debug("[" + getClass().getSimpleName() + "] Found " + apps.size() + " apps");
        for (AppProfile app : apps) {
            if (!finished.contains(app.getId())) {
                logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " not finished");
                if (RestClient.TrackingUI.HISTORY.equals(app.getTrackingUI())) {
                    logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " finished just now");
                    moveAppToHistory(app);
                } else {
                    logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " is running");
                    running.add(app.getId());
                    updateAppInfo(app);
                }
            }
        }
    }

    private void moveAppToHistory(final AppProfile app) {
        final String appId = app.getId();
        logger.debug("[" + getClass().getSimpleName() + "] Moving " + appId + " to history");
        running.remove(appId);
        finished.add(appId);

        // gather app info
        List<JobProfile> jobs = dataStore.find(DataEntityType.JOB, "appId", appId);
        JobProfile job;
        String jobId;
        if (jobs.size() > 1)
            throw new POSUMException("Unexpected number of jobs for mapreduce app " + appId);
        else if (jobs.size() < 1) {
            job = collector.getFinishedJobInfo(appId);
            jobId = job.getId();
        } else {
            jobId = jobs.get(0).getId();
            job = collector.getFinishedJobInfo(appId, jobId);
        }
        final JobProfile finalJob = job;
        final List<TaskProfile> tasks = collector.getFinishedTasksInfo(appId, jobId);

        // move info in database
        dataStore.runTransaction(new DataTransaction() {
            @Override
            public void run() throws Exception {
                dataStore.delete(DataEntityType.APP, appId);
                dataStore.delete(DataEntityType.JOB, "appId", appId);
                dataStore.delete(DataEntityType.TASK, "appId", appId);
                dataStore.updateOrStore(DataEntityType.APP_HISTORY, app);
                dataStore.updateOrStore(DataEntityType.JOB_HISTORY, finalJob);
                for (TaskProfile task : tasks) {
                    dataStore.updateOrStore(DataEntityType.TASK_HISTORY, task);
                }
            }
        });
    }

    private void updateAppInfo(final AppProfile app) {
        logger.debug("[" + getClass().getSimpleName() + "] Updating " + app.getId() + " info");

        dataStore.updateOrStore(DataEntityType.APP, app);
        if (historyEnabled) {
            dataStore.store(DataEntityType.HISTORY,
                    new HistoryProfilePBImpl<>(DataEntityType.APP, (AppProfilePBImpl) app));
        }

        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            JobProfile lastJobInfo = dataStore.getJobProfileForApp(app.getId());
            final JobProfile job = collector.getRunningJobInfo(app.getId(), lastJobInfo);
            if (job == null)
                logger.debug("[" + getClass().getSimpleName() + "] Could not find job for " + app.getId());
            else {
                final List<TaskProfile> tasks = collector.getRunningTasksInfo(job);
                Integer mapDuration = 0, reduceDuration = 0, avgDuration = 0, mapNo = 0, reduceNo = 0, avgNo = 0;
                for (TaskProfile task : tasks) {
                    Integer duration = task.getDuration();
                    if (duration > 0) {
                        if (TaskType.MAP.equals(task.getType())) {
                            mapDuration += task.getDuration();
                            mapNo++;
                        }
                        if (TaskType.REDUCE.equals(task.getType())) {
                            reduceDuration += task.getDuration();
                            reduceNo++;
                        }
                        avgDuration += duration;
                        avgNo++;
                    }
                    if (avgNo > 0) {
                        job.setAvgTaskDuration(avgDuration / avgNo);
                        if (mapNo > 0)
                            job.setAvgMapDuration(mapDuration / mapNo);
                        if (reduceNo > 0)
                            job.setAvgReduceDuration(reduceDuration / reduceNo);
                    }
                }

                dataStore.runTransaction(new DataTransaction() {
                    @Override
                    public void run() throws Exception {
                        dataStore.updateOrStore(DataEntityType.JOB, job);
                        for (TaskProfile task : tasks) {
                            dataStore.updateOrStore(DataEntityType.TASK, task);
                        }
                    }
                });

                if (historyEnabled) {
                    dataStore.store(DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.APP, (AppProfilePBImpl) app));
                    dataStore.store(DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.JOB, (JobProfilePBImpl) job));
                    for (TaskProfile task : tasks) {
                        dataStore.store(DataEntityType.HISTORY,
                                new HistoryProfilePBImpl<>(DataEntityType.TASK, (TaskProfilePBImpl) task));
                    }
                }
            }
        } else {
            //app is not yet tracked
            logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " is not tracked");
            try {
                final JobProfile job = getSubmittedJobInfo(app.getId());
                dataStore.updateOrStore(DataEntityType.JOB, job);
                if (historyEnabled) {
                    dataStore.store(DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.JOB, (JobProfilePBImpl) job));
                }
            } catch (Exception e) {
                logger.error("Could not get job info from staging dir!", e);
            }
        }
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

        JobProfile profile = Records.newRecord(JobProfile.class);
        profile.setId(jobId.toString());
        profile.setAppId(appId);
        profile.setName(conf.getJobName());
        profile.setUser(conf.getUser());
        profile.setInputBytes(inputLength);
        profile.setInputSplits(taskSplitMetaInfo.length);
        return profile;
    }

    private JobProfile getSubmittedJobInfo(String appId) throws IOException {
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
}
