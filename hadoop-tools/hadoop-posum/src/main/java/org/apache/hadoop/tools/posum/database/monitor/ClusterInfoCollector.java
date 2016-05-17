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
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.store.DataTransaction;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * Created by ane on 2/4/16.
 */
public class ClusterInfoCollector {

    private static Log logger = LogFactory.getLog(ClusterInfoCollector.class);

    private Set<String> running = new HashSet<>();
    private Set<String> finished = new HashSet<>();
    private final DataStore dataStore;
    private final DataEntityDB db = DataEntityDB.getMain();
    private final HadoopAPIClient api;
    private final Configuration conf;
    private final boolean historyEnabled;

    ClusterInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.api = new HadoopAPIClient(conf);
        this.conf = conf;
        this.historyEnabled = conf.getBoolean(POSUMConfiguration.MONITOR_KEEP_HISTORY,
                POSUMConfiguration.MONITOR_KEEP_HISTORY_DEFAULT);
    }

    void collect() {
        List<AppProfile> apps = api.getAppsInfo();
        logger.trace("Found " + apps.size() + " apps");
        for (AppProfile app : apps) {
            if (!finished.contains(app.getId())) {
                logger.trace("App " + app.getId() + " not finished");
                if (RestClient.TrackingUI.HISTORY.equals(app.getTrackingUI())) {
                    logger.trace("App " + app.getId() + " finished just now");
                    moveAppToHistory(app);
                } else {
                    logger.trace("App " + app.getId() + " is running");
                    running.add(app.getId());
                    updateAppInfo(app);
                }
            }
        }
    }

    private void moveAppToHistory(final AppProfile app) {
        final String appId = app.getId();
        logger.trace("Moving " + appId + " to history");
        running.remove(appId);
        finished.add(appId);

        // gather app info
        List<JobProfile> jobs = dataStore.find(db, DataEntityType.JOB, "appId", appId);
        JobProfile job;
        String jobId;
        if (jobs.size() > 1)
            throw new POSUMException("Unexpected number of jobs for mapreduce app " + appId);
        else if (jobs.size() < 1) {
            job = api.getFinishedJobInfo(appId);
            jobId = job.getId();
        } else {
            jobId = jobs.get(0).getId();
            job = api.getFinishedJobInfo(appId, jobId);
        }
        final JobProfile finalJob = job;
        final List<TaskProfile> tasks = api.getFinishedTasksInfo(appId, jobId);

        // move info in database
        dataStore.runTransaction(new DataTransaction() {
            @Override
            public void run() throws Exception {
                dataStore.delete(db, DataEntityType.APP, appId);
                dataStore.delete(db, DataEntityType.JOB, "appId", appId);
                dataStore.delete(db, DataEntityType.TASK, "appId", appId);
                dataStore.updateOrStore(db, DataEntityType.APP_HISTORY, app);
                dataStore.updateOrStore(db, DataEntityType.JOB_HISTORY, finalJob);
                for (TaskProfile task : tasks) {
                    dataStore.updateOrStore(db, DataEntityType.TASK_HISTORY, task);
                }
            }
        });
    }

    private void updateAppInfo(final AppProfile app) {
        logger.trace("Updating " + app.getId() + " info");

        dataStore.updateOrStore(db, DataEntityType.APP, app);
        if (historyEnabled) {
            dataStore.store(db, DataEntityType.HISTORY,
                    new HistoryProfilePBImpl<>(DataEntityType.APP, app));
        }

        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            JobProfile lastJobInfo = dataStore.getJobProfileForApp(db, app.getId());
            final JobProfile job = api.getRunningJobInfo(app.getId(), lastJobInfo);
            if (job == null)
                logger.warn("Could not find job for " + app.getId());
            else {
                final List<TaskProfile> tasks = api.getRunningTasksInfo(job);
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
                        dataStore.updateOrStore(db, DataEntityType.JOB, job);
                        for (TaskProfile task : tasks) {
                            dataStore.updateOrStore(db, DataEntityType.TASK, task);
                        }
                    }
                });

                if (historyEnabled) {
                    dataStore.store(db, DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.APP, app));
                    dataStore.store(db, DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.JOB, job));
                    for (TaskProfile task : tasks) {
                        dataStore.store(db, DataEntityType.HISTORY,
                                new HistoryProfilePBImpl<>(DataEntityType.TASK, task));
                    }
                }
            }
        } else {
            //app is not yet tracked
            logger.trace(" pp " + app.getId() + " is not tracked");
            try {
                final JobProfile job = getSubmittedJobInfo(conf, app.getId());
                dataStore.updateOrStore(db, DataEntityType.JOB, job);
                if (historyEnabled) {
                    dataStore.store(db, DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.JOB,  job));
                }
            } catch (Exception e) {
                logger.error("Could not get job info from staging dir!", e);
            }
        }
    }

    private static JobProfile readJobConf(String appId, JobId jobId, FileSystem fs, JobConf conf, Path jobSubmitDir) throws IOException {
        JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
                TypeConverter.fromYarn(jobId), fs,
                conf,
                jobSubmitDir);

        long inputLength = 0;
        for (JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo : taskSplitMetaInfo) {
            inputLength += aTaskSplitMetaInfo.getInputDataLength();
        }

        logger.trace("Input splits: " + taskSplitMetaInfo.length);
        logger.trace("Total input size: " + inputLength);

        JobProfile profile = Records.newRecord(JobProfile.class);
        profile.setId(jobId.toString());
        profile.setAppId(appId);
        profile.setName(conf.getJobName());
        profile.setUser(conf.getUser());
        profile.setInputBytes(inputLength);
        profile.setInputSplits(taskSplitMetaInfo.length);
        return profile;
    }

    public static JobProfile getSubmittedJobInfo(Configuration conf, String appId) throws IOException {
        final ApplicationId actualAppId = Utils.parseApplicationId(appId);
        FileSystem fs = FileSystem.get(conf);
        Path confPath = MRApps.getStagingAreaDir(conf, UserGroupInformation.getCurrentUser().getUserName());
        confPath = fs.makeQualified(confPath);

        logger.trace("Looking in staging path: " + confPath);
        FileStatus[] statuses = fs.listStatus(confPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.toString().contains("job_" + actualAppId.getClusterTimestamp());
            }
        });

        if (statuses.length != 1)
            throw new POSUMException("Wrong number of job profile directories for: " + appId);

        Path jobConfDir = statuses[0].getPath();
        logger.trace("Checking file path: " + jobConfDir);
        String jobId = jobConfDir.getName();
        JobConf jobConf = new JobConf(new Path(jobConfDir, "job.xml"));
        //DANGER We assume there can only be one job / application
        return readJobConf(appId, Utils.parseJobId(appId, jobId), fs, jobConf, jobConfDir);

    }
}
