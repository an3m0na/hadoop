package org.apache.hadoop.tools.posum.database.monitor;

import com.mongodb.DuplicateKeyException;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.field.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.field.CounterInfoPayload;
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
import java.net.URI;
import java.net.URISyntaxException;
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
                    updateAppInfo(app);
                    running.add(app.getId());
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
            // there is no running record of the job
            job = api.getFinishedJobInfo(appId);
            jobId = job.getId();
        } else {
            // update the running info with the history info
            jobId = jobs.get(0).getId();
            job = api.getFinishedJobInfo(appId, jobId);
        }
        final JobProfile finalJob = job;
        final JobConfProxy jobConf = api.getFinishedJobConf(jobId);
        final CountersProxy jobCounters = api.getFinishedJobCounters(jobId);
        final List<TaskProfile> tasks = api.getFinishedTasksInfo(jobId);
        final List<CountersProxy> taskCounters = new ArrayList<>(tasks.size());
        for (TaskProfile task : tasks) {
            if (task.getType().equals(TaskType.REDUCE))
                api.addRunningAttemptInfo(task);
            taskCounters.add(api.getFinishedTaskCounters(jobId, task.getId()));
        }

        // move info in database
        dataStore.runTransaction(db, new DataTransaction() {
            @Override
            public void run() throws Exception {
                try {
                    dataStore.delete(db, DataEntityType.APP, appId);
                    dataStore.delete(db, DataEntityType.JOB, "appId", appId);
                    dataStore.delete(db, DataEntityType.TASK, "appId", appId);
                    dataStore.delete(db, DataEntityType.JOB_CONF, finalJob.getId());
                    dataStore.delete(db, DataEntityType.COUNTER, finalJob.getId());
                    dataStore.store(db, DataEntityType.APP_HISTORY, app);
                    dataStore.store(db, DataEntityType.JOB_HISTORY, finalJob);
                    dataStore.store(db, DataEntityType.JOB_CONF_HISTORY, jobConf);
                    dataStore.store(db, DataEntityType.COUNTER_HISTORY, jobCounters);
                    for (TaskProfile task : tasks) {
                        dataStore.store(db, DataEntityType.TASK_HISTORY, task);
                    }
                    for (CountersProxy counters : taskCounters) {
                        if (counters == null)
                            continue;
                        dataStore.store(db, DataEntityType.COUNTER_HISTORY, counters);
                    }
                } catch (Exception e) {
                    logger.error("Could not move app data to history", e);
                }
            }
        });
    }

    private void updateAppInfo(final AppProfile app) {
        dataStore.updateOrStore(db, DataEntityType.APP, app);
        if (historyEnabled) {
            dataStore.store(db, DataEntityType.HISTORY,
                    new HistoryProfilePBImpl<>(DataEntityType.APP, app));
        }

        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            JobProfile lastJobInfo = dataStore.getJobProfileForApp(db, app.getId(), app.getUser());
            final JobProfile job = api.getRunningJobInfo(app.getId(), app.getQueue(), lastJobInfo);
            if (job == null)
                logger.warn("Could not find job for " + app.getId());
            else {
                final CountersProxy jobCounters = api.getRunningJobCounters(app.getId(), job.getId());
                final List<TaskProfile> tasks = api.getRunningTasksInfo(job);
                final List<CountersProxy> taskCounters = new ArrayList<>(tasks.size());

                long mapDuration = 0, reduceDuration = 0, shuffleDuration = 0, mergeDuration = 0, avgDuration = 0;
                int mapNo = 0, reduceNo = 0, avgNo = 0;
                long mapInputSize = 0, mapOutputSize = 0, reduceInputSize = 0, reduceOutputSize = 0;

                for (TaskProfile task : tasks) {
                    CountersProxy counters = api.getRunningTaskCounters(task.getAppId(), task.getJobId(), task.getId());
                    if (counters != null) {
                        taskCounters.add(counters);
                        for (CounterGroupInfoPayload group : counters.getCounterGroup()) {
                            if (TaskCounter.class.getName().equals(group.getCounterGroupName()))
                                for (CounterInfoPayload counter : group.getCounter()) {
                                    switch (counter.getName()) {
                                        case "MAP_OUTPUT_BYTES":
                                            if (task.getType().equals(TaskType.MAP))
                                                task.setOutputBytes(counter.getTotalCounterValue());
                                            break;
                                        case "REDUCE_SHUFFLE_BYTES":
                                            if (task.getType().equals(TaskType.REDUCE))
                                                task.setInputBytes(counter.getTotalCounterValue());
                                            break;
                                    }
                                }
                            if (FileSystemCounter.class.getName().equals(group.getCounterGroupName()))
                                for (CounterInfoPayload counter : group.getCounter()) {
                                    switch (counter.getName()) {
                                        case "FILE_BYTES_READ":
                                            if (task.getType().equals(TaskType.MAP))
                                                task.setOutputBytes(task.getOutputBytes() +
                                                        counter.getTotalCounterValue());
                                            break;
                                        case "HDFS_BYTES_READ":
                                            if (task.getType().equals(TaskType.MAP))
                                                task.setInputBytes(task.getInputBytes() +
                                                        counter.getTotalCounterValue());
                                            break;
                                        case "HDFS_BYTES_WRITTEN":
                                            if (task.getType().equals(TaskType.REDUCE))
                                                task.setOutputBytes(task.getOutputBytes() +
                                                        counter.getTotalCounterValue());
                                            break;
                                    }
                                }
                        }
                    }

                    if (task.getType().equals(TaskType.REDUCE))
                        api.addRunningAttemptInfo(task);

                    Integer duration = task.getDuration();

                    if (duration > 0) {
                        // task has finished
                        if (TaskType.MAP.equals(task.getType())) {
                            mapDuration += task.getDuration();
                            mapNo++;
                            mapInputSize += task.getInputBytes();
                            mapOutputSize += task.getOutputBytes();
                        }
                        if (TaskType.REDUCE.equals(task.getType())) {
                            reduceDuration += task.getDuration();
                            shuffleDuration += task.getShuffleTime();
                            mergeDuration += task.getMergeTime();
                            reduceNo++;
                            reduceInputSize += task.getInputBytes();
                            reduceOutputSize += task.getOutputBytes();
                        }
                        avgDuration += duration;
                        avgNo++;
                    }
                }

                if (avgNo > 0) {
                    job.setAvgTaskDuration(avgDuration / avgNo);
                    if (mapNo > 0) {
                        job.setAvgMapDuration(mapDuration / mapNo);
                    }
                    if (reduceNo > 0) {
                        job.setAvgReduceDuration(reduceDuration / reduceNo);
                        job.setAvgShuffleDuration(shuffleDuration / reduceNo);
                        job.setAvgMergeDuration(mergeDuration / reduceNo);
                    }
                }

                job.setInputBytes(mapInputSize);
                job.setMapOutputBytes(mapOutputSize);
                job.setReduceInputBytes(reduceInputSize);
                job.setOutputBytes(reduceOutputSize);

                dataStore.runTransaction(db, new DataTransaction() {
                    @Override
                    public void run() throws Exception {
                        dataStore.updateOrStore(db, DataEntityType.JOB, job);
                        dataStore.updateOrStore(db, DataEntityType.COUNTER, jobCounters);
                        for (CountersProxy counters : taskCounters)
                            dataStore.updateOrStore(db, DataEntityType.COUNTER, counters);
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
                    dataStore.store(db, DataEntityType.HISTORY,
                            new HistoryProfilePBImpl<>(DataEntityType.COUNTER, jobCounters));
                    for (TaskProfile task : tasks) {
                        dataStore.store(db, DataEntityType.HISTORY,
                                new HistoryProfilePBImpl<>(DataEntityType.TASK, task));
                    }
                }
            }
        } else {
            //app is not yet tracked
            logger.trace("App " + app.getId() + " is not tracked");
            if (!running.contains(app.getId())) {
                // get job info directly from the conf in the staging dir
                try {
                    JobProfile job = getAndStoreSubmittedJobInfo(conf, app.getId(), app.getUser(), dataStore, db);
                    if (historyEnabled) {
                        dataStore.store(db, DataEntityType.HISTORY,
                                new HistoryProfilePBImpl<>(DataEntityType.JOB, job));
                    }
                } catch (Exception e) {
                    logger.error("Could not get job info from staging dir!", e);
                }
            }
        }
    }

    public static JobProfile getAndStoreSubmittedJobInfo(Configuration conf,
                                                         String appId,
                                                         String user,
                                                         final DataStore store,
                                                         final DataEntityDB db) throws IOException {
        final JobConfProxy confProxy = getSubmittedJobConf(conf, appId, user);
        final JobProfile job = getSubmittedJobInfo(confProxy, appId);
        job.setTotalMapTasks(job.getInputSplits());
        int reduces = 0;
        String reducesString = confProxy.getEntry(MRJobConfig.NUM_REDUCES);
        if (reducesString != null && reducesString.length() > 0)
            reduces = Integer.valueOf(reducesString);
        job.setTotalReduceTasks(reduces);
        store.runTransaction(db, new DataTransaction() {
            @Override
            public void run() throws Exception {
                try {
                    store.store(db, DataEntityType.JOB, job);
                    store.store(db, DataEntityType.JOB_CONF, confProxy);
                } catch (DuplicateKeyException e) {
                    // this is possible; do nothing
                } catch (Exception e) {
                    logger.warn("Exception occurred when storing new job info", e);
                }
            }
        });
        return job;
    }

    private static JobProfile getJobProfileFromConf(String appId, JobId jobId, FileSystem fs, JobConf conf, Path jobSubmitDir) throws IOException {
        JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
                TypeConverter.fromYarn(jobId), fs,
                conf,
                jobSubmitDir);

        long inputLength = 0;
        for (JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo : taskSplitMetaInfo) {
            inputLength += aTaskSplitMetaInfo.getInputDataLength();
        }

        JobProfile profile = Records.newRecord(JobProfile.class);
        profile.setId(jobId.toString());
        profile.setAppId(appId);
        profile.setName(conf.getJobName());
        profile.setUser(conf.getUser());
        profile.setTotalInputBytes(inputLength);
        profile.setInputSplits(taskSplitMetaInfo.length);
        return profile;
    }

    private static JobProfile getSubmittedJobInfo(JobConfProxy confProxy, String appId) throws IOException {
        FileSystem fs = FileSystem.get(confProxy.getConf());
        String jobConfDirPath = confProxy.getConfPath();
        Path jobConfDir = null;
        try {
            jobConfDir = new Path(new URI(jobConfDirPath));
            logger.trace("Checking file path: " + jobConfDir);
            String jobId = jobConfDir.getName();
            //DANGER We assume there can only be one job / application
            return getJobProfileFromConf(appId, Utils.parseJobId(appId, jobId), fs, confProxy.getConf(), jobConfDir);
        } catch (URISyntaxException e) {
            throw new POSUMException("Invalid jobConfDir path " + jobConfDirPath, e);
        }
    }

    private static JobConfProxy getSubmittedJobConf(Configuration conf, String appId, String user) throws IOException {
        final ApplicationId actualAppId = Utils.parseApplicationId(appId);
        FileSystem fs = FileSystem.get(conf);
        Path confPath = MRApps.getStagingAreaDir(conf,
                user != null ? user : UserGroupInformation.getCurrentUser().getUserName());
        confPath = fs.makeQualified(confPath);

        logger.trace("Looking in staging path: " + confPath);
        FileStatus[] statuses = fs.listStatus(confPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.toString().contains("job_" + actualAppId.getClusterTimestamp());
            }
        });

        //DANGER We assume there can only be one job / application
        if (statuses.length != 1)
            throw new POSUMException("Wrong number of job profile directories for: " + appId);

        Path jobConfDir = statuses[0].getPath();
        logger.trace("Checking file path: " + jobConfDir);
        JobConf jobConf = new JobConf(new Path(jobConfDir, "job.xml"));
        JobConfProxy proxy = Records.newRecord(JobConfProxy.class);
        proxy.setId(jobConfDir.getName());
        proxy.setConfPath(jobConfDir.toUri().toString());
        proxy.setConf(jobConf);
        return proxy;
    }
}
