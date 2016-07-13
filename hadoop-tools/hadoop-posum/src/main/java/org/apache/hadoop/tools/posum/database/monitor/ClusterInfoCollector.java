package org.apache.hadoop.tools.posum.database.monitor;

import com.mongodb.DuplicateKeyException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
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
import org.apache.hadoop.util.StringUtils;
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
    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    public ClusterInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.api = new HadoopAPIClient(conf);
        this.conf = conf;
        this.historyEnabled = conf.getBoolean(POSUMConfiguration.MONITOR_KEEP_HISTORY,
                POSUMConfiguration.MONITOR_KEEP_HISTORY_DEFAULT);
    }

    void refresh() {
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

        List<JobProfile> jobs =
                dataStore.find(db, DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId), 0, 0);
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
            JobProfile previousJob = jobs.get(0);
            jobId = previousJob.getId();
            job = api.getFinishedJobInfo(appId, jobId, previousJob);
        }
        final JobProfile finalJob = job;

        final JobConfProxy jobConf = api.getFinishedJobConf(jobId);
        setClassNamesFromConf(job, jobConf);
        final CountersProxy jobCounters = api.getFinishedJobCounters(jobId);

        if (jobCounters != null) {
            for (CounterGroupInfoPayload group : jobCounters.getCounterGroup()) {
                if (TaskCounter.class.getName().equals(group.getCounterGroupName()))
                    for (CounterInfoPayload counter : group.getCounter()) {
                        switch (counter.getName()) {
                            // make sure to record map materialized (compressed) bytes if compression is enabled
                            // this is because reduce_shuffle_bytes are also in compressed form
                            case "MAP_OUTPUT_BYTES":
                                Long previous = job.getMapOutputBytes();
                                if (previous == null || previous == 0)
                                    job.setMapOutputBytes(counter.getTotalCounterValue());
                                break;
                            case "MAP_OUTPUT_MATERIALIZED_BYTES":
                                Long value = counter.getTotalCounterValue();
                                if (value > 0)
                                    job.setMapOutputBytes(value);
                                break;
                            case "REDUCE_SHUFFLE_BYTES":
                                job.setReduceInputBytes(counter.getTotalCounterValue());
                                break;
                        }
                    }
                if (FileSystemCounter.class.getName().equals(group.getCounterGroupName()))
                    for (CounterInfoPayload counter : group.getCounter()) {
                        switch (counter.getName()) {
                            case "FILE_BYTES_READ":
                                job.setInputBytes(job.getInputBytes() + counter.getMapCounterValue());
                                break;
                            case "HDFS_BYTES_READ":
                                job.setInputBytes(job.getInputBytes() + counter.getMapCounterValue());
                                break;
                            case "HDFS_BYTES_WRITTEN":
                                job.setOutputBytes(job.getOutputBytes() + counter.getReduceCounterValue());
                                break;
                        }
                    }
            }
        }

        final List<TaskProfile> tasks = api.getFinishedTasksInfo(jobId);
        final List<CountersProxy> taskCounters = new ArrayList<>(tasks.size());
        for (TaskProfile task : tasks) {
            api.addFinishedAttemptInfo(task);
            if (job.getSplitLocations() != null && task.getHttpAddress() != null) {
                int splitIndex = Utils.parseTaskId(task.getAppId(), task.getId()).getId();
                if (job.getSplitLocations().get(splitIndex).equals(task.getHttpAddress()))
                    task.setLocal(true);
            }
            CountersProxy counters = api.getFinishedTaskCounters(jobId, task.getId());
            if (counters != null) {
                taskCounters.add(counters);
                parseTaskCounters(task, counters);
            }
        }

        // move info in database
        dataStore.runTransaction(db, new DataTransaction() {
            @Override
            public void run() throws Exception {
                try {
                    dataStore.delete(db, DataEntityType.APP, appId);
                    dataStore.delete(db, DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId));
                    dataStore.delete(db, DataEntityType.TASK, Collections.singletonMap("appId", (Object) appId));
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
                        dataStore.delete(db, DataEntityType.COUNTER, counters.getId());
                        dataStore.store(db, DataEntityType.COUNTER_HISTORY, counters);
                    }
                } catch (Exception e) {
                    logger.error("Could not move app data to history", e);
                }
            }
        });
    }

    private void parseTaskCounters(TaskProfile task, CountersProxy counters) {

        for (CounterGroupInfoPayload group : counters.getCounterGroup()) {
            if (TaskCounter.class.getName().equals(group.getCounterGroupName()))
                for (CounterInfoPayload counter : group.getCounter()) {
                    switch (counter.getName()) {
                        // make sure to record map materialized (compressed) bytes if compression is enabled
                        // this is because reduce_shuffle_bytes are also in compressed form
                        case "MAP_OUTPUT_BYTES":
                            if (task.getType().equals(TaskType.MAP)) {
                                Long previous = task.getOutputBytes();
                                if (previous == null || previous == 0)
                                    task.setOutputBytes(counter.getTotalCounterValue());
                            }
                            break;
                        case "MAP_OUTPUT_MATERIALIZED_BYTES":
                            if (task.getType().equals(TaskType.MAP)) {
                                Long value = counter.getTotalCounterValue();
                                if (value > 0)
                                    task.setOutputBytes(value);
                            }
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
                            if (task.getType().equals(TaskType.MAP)) {
                                task.setInputBytes(task.getInputBytes() +
                                        counter.getTotalCounterValue());
                            }
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

    private void updateAppInfo(final AppProfile app) {
        dataStore.updateOrStore(db, DataEntityType.APP, app);
        if (historyEnabled) {
            dataStore.store(db, DataEntityType.HISTORY,
                    new HistoryProfilePBImpl<>(DataEntityType.APP, app));
        }

        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            JobProfile lastJobInfo = getCurrentProfileForApp(app.getId(), app.getUser());
            final JobProfile job = api.getRunningJobInfo(app.getId(), app.getQueue(), lastJobInfo);
            if (job == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }
            final CountersProxy jobCounters = api.getRunningJobCounters(app.getId(), job.getId());
            if (jobCounters == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }
            final List<TaskProfile> tasks = api.getRunningTasksInfo(job);
            if (tasks == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }
            final List<CountersProxy> taskCounters = new ArrayList<>(tasks.size());

            long mapDuration = 0, reduceDuration = 0, reduceTime = 0, shuffleTime = 0, mergeTime = 0, avgDuration = 0;
            int mapNo = 0, reduceNo = 0, avgNo = 0;
            long mapInputSize = 0, mapOutputSize = 0, reduceInputSize = 0, reduceOutputSize = 0;

            for (TaskProfile task : tasks) {
                Long duration = task.getDuration();
                if (duration > 0) {
                    // task has finished; get statistics

                    if (!api.addRunningAttemptInfo(task)) {
                        if (api.checkAppFinished(app))
                            moveAppToHistory(app);
                        return;
                    }

                    CountersProxy counters = api.getRunningTaskCounters(task.getAppId(), task.getJobId(), task.getId());

                    if (counters == null) {
                        if (api.checkAppFinished(app))
                            moveAppToHistory(app);
                        return;
                    }

                    taskCounters.add(counters);
                    parseTaskCounters(task, counters);

                    if (TaskType.MAP.equals(task.getType())) {
                        mapDuration += task.getDuration();
                        mapNo++;
                        mapInputSize += task.getInputBytes();
                        mapOutputSize += task.getOutputBytes();
                        if (job.getSplitLocations() != null && task.getHttpAddress() != null) {
                            int splitIndex = Utils.parseTaskId(task.getAppId(), task.getId()).getId();
                            if (job.getSplitLocations().get(splitIndex).equals(task.getHttpAddress()))
                                task.setLocal(true);
                        }
                    }
                    if (TaskType.REDUCE.equals(task.getType())) {
                        reduceDuration += task.getDuration();
                        reduceTime += task.getReduceTime();
                        shuffleTime += task.getShuffleTime();
                        mergeTime += task.getMergeTime();
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
                    job.setAvgShuffleTime(shuffleTime / reduceNo);
                    job.setAvgMergeTime(mergeTime / reduceNo);
                    job.setAvgReduceTime(reduceTime / reduceNo);
                }
            }

            job.setInputBytes(mapInputSize);
            job.setMapOutputBytes(mapOutputSize);
            job.setReduceInputBytes(reduceInputSize);
            job.setOutputBytes(reduceOutputSize);
            // update in case of discrepancy
            job.setCompletedMaps(mapNo);
            job.setCompletedReduces(reduceNo);

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
        } else {
            //app is not yet tracked
            logger.trace("App " + app.getId() + " is not tracked");
            if (!running.contains(app.getId())) {
                // get job info directly from the conf in the staging dir
                try {
                    JobProfile job = getAndStoreSubmittedJobInfo(conf, app.getId(), app.getUser(), db);
                    if (historyEnabled && job != null) {
                        dataStore.store(db, DataEntityType.HISTORY,
                                new HistoryProfilePBImpl<>(DataEntityType.JOB, job));
                    }
                } catch (Exception e) {
                    logger.error("Could not get job info from staging dir!", e);
                }
            }
        }
    }

    public JobProfile getCurrentProfileForApp(String appId, String user) {
        JobProfile profile = dataStore.getJobProfileForApp(db, appId, user);

        if (profile != null)
            return profile;

        // if not found, force the reading of the configuration
        try {
            logger.debug("Forcing fetch of job info from conf for " + appId);
            return getAndStoreSubmittedJobInfo(conf, appId, user, db);
        } catch (Exception e) {
            logger.debug("Could not retrieve job info for app " + appId, e);
        }
        return null;
    }

    public JobProfile getAndStoreSubmittedJobInfo(Configuration conf,
                                                  String appId,
                                                  String user,
                                                  final DataEntityDB db) throws IOException {
        final JobConfProxy confProxy = getSubmittedJobConf(conf, appId, user);
        final JobProfile job = getSubmittedJobInfo(confProxy, appId);
        job.setTotalMapTasks(job.getInputSplits());
        int reduces = 0;
        String reducesString = confProxy.getEntry(MRJobConfig.NUM_REDUCES);
        if (reducesString != null && reducesString.length() > 0)
            reduces = Integer.valueOf(reducesString);
        job.setTotalReduceTasks(reduces);
        dataStore.runTransaction(db, new DataTransaction() {
            @Override
            public void run() throws Exception {
                try {
                    dataStore.store(db, DataEntityType.JOB, job);
                    dataStore.store(db, DataEntityType.JOB_CONF, confProxy);
                } catch (DuplicateKeyException e) {
                    // this is possible; do nothing
                } catch (Exception e) {
                    logger.warn("Exception occurred when storing new job info", e);
                }
            }
        });
        return job;
    }

    private static JobProfile getJobProfileFromConf(String appId,
                                                    JobId jobId,
                                                    FileSystem fs,
                                                    JobConfProxy jobConfProxy,
                                                    Path jobSubmitDir) throws IOException {
        JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
                TypeConverter.fromYarn(jobId), fs,
                jobConfProxy.getConf(),
                jobSubmitDir);

        long inputLength = 0;
        List<String> splitLocations = new ArrayList<>(taskSplitMetaInfo.length);
        for (JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo : taskSplitMetaInfo) {
            inputLength += aTaskSplitMetaInfo.getInputDataLength();
            splitLocations.add(StringUtils.join(" ", aTaskSplitMetaInfo.getLocations()));
        }

        JobProfile profile = Records.newRecord(JobProfile.class);
        profile.setId(jobId.toString());
        profile.setAppId(appId);
        profile.setName(jobConfProxy.getEntry(MRJobConfig.JOB_NAME));
        profile.setUser(jobConfProxy.getEntry(MRJobConfig.USER_NAME));
        profile.setTotalInputBytes(inputLength);
        profile.setInputSplits(taskSplitMetaInfo.length);
        setClassNamesFromConf(profile, jobConfProxy);
        profile.setSplitLocations(splitLocations);
        return profile;
    }

    private static void setClassNamesFromConf(JobProfile profile, JobConfProxy conf) {
        String classString = conf.getEntry(MRJobConfig.MAP_CLASS_ATTR);
        if (classString == null)
            classString = conf.getEntry(OLD_MAP_CLASS_ATTR);
        if (classString == null)
            profile.setReducerClass(IdentityMapper.class.getName());
        profile.setMapperClass(classString);
        classString = conf.getEntry(MRJobConfig.REDUCE_CLASS_ATTR);
        if (classString == null)
            classString = conf.getEntry(OLD_REDUCE_CLASS_ATTR);
        if (classString == null)
            profile.setReducerClass(IdentityReducer.class.getName());
        profile.setReducerClass(classString);
    }

    private static JobProfile getSubmittedJobInfo(JobConfProxy confProxy, String appId) throws IOException {
        FileSystem fs = FileSystem.get(confProxy.getConf());
        String jobConfDirPath = confProxy.getConfPath();
        Path jobConfDir = null;
        try {
            jobConfDir = new Path(new URI(jobConfDirPath));
            String jobId = jobConfDir.getName();
            //DANGER We assume there can only be one job / application
            return getJobProfileFromConf(appId, Utils.parseJobId(appId, jobId), fs, confProxy, jobConfDir);
        } catch (URISyntaxException e) {
            throw new POSUMException("Invalid jobConfDir path " + jobConfDirPath, e);
        }
    }

    private static JobConfProxy getSubmittedJobConf(Configuration conf, final String appId, String user) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path confPath = MRApps.getStagingAreaDir(conf,
                user != null ? user : UserGroupInformation.getCurrentUser().getUserName());
        confPath = fs.makeQualified(confPath);

        logger.debug("Looking in staging path: " + confPath);
        FileStatus[] statuses = fs.listStatus(confPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.toString().contains(appId.replace("application", "job"));
            }
        });

        //DANGER We assume there can only be one job / application
        if (statuses.length != 1) {
            if (statuses.length > 1)
                throw new POSUMException("Too many conf directories found for " + appId);
            logger.warn("Job conf dir not found for: " + appId);
            return null;
        }

        Path jobConfDir = statuses[0].getPath();
        logger.debug("Checking file path for conf: " + jobConfDir);
        Path jobConfPath = new Path(jobConfDir, "job.xml");
        Configuration jobConf = new JobConf(false);
        jobConf.addResource(fs.open(jobConfPath), jobConfPath.toString());
        JobConfProxy proxy = Records.newRecord(JobConfProxy.class);
        proxy.setId(jobConfDir.getName());
        proxy.setConfPath(jobConfDir.toUri().toString());
        proxy.setConf(jobConf);
        return proxy;
    }
}
