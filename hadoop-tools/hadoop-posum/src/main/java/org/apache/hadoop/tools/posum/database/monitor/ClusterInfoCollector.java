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
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.records.call.*;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.*;

/**
 * Created by ane on 2/4/16.
 */
public class ClusterInfoCollector {

    private static Log logger = LogFactory.getLog(ClusterInfoCollector.class);

    private Set<String> running = new HashSet<>();
    private Set<String> finished = new HashSet<>();
    private final Database db;
    private final HadoopAPIClient api;
    private final Configuration conf;
    private final boolean auditEnabled;
    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    public ClusterInfoCollector(Configuration conf, Database db) {
        this.db = db;
        this.api = new HadoopAPIClient(conf);
        this.conf = conf;
        this.auditEnabled = conf.getBoolean(PosumConfiguration.MONITOR_KEEP_HISTORY,
                PosumConfiguration.MONITOR_KEEP_HISTORY_DEFAULT);
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
        JobForAppCall getJobForApp = JobForAppCall.newInstance(appId, app.getUser());
        JobProfile job = db.executeDatabaseCall(getJobForApp).getEntity();
        String jobId;
        if (job == null) {
            // there is no running record of the job
            job = api.getFinishedJobInfo(appId);
            jobId = job.getId();
        } else {
            // update the running info with the history info
            jobId = job.getId();
            job = api.getFinishedJobInfo(appId, job.getId(), job);
        }

        TransactionCall updateCalls = TransactionCall.newInstance()
                .addCall(DeleteByIdCall.newInstance(APP, appId))
                .addCall(StoreCall.newInstance(DataEntityCollection.APP_HISTORY, app))
                .addCall(DeleteByQueryCall.newInstance(JOB, QueryUtils.is("appId", appId)))
                .addCall(DeleteByQueryCall.newInstance(TASK, QueryUtils.is("appId", appId)))
                .addCall(DeleteByIdCall.newInstance(DataEntityCollection.JOB_CONF, job.getId()))
                .addCall(DeleteByIdCall.newInstance(COUNTER, job.getId()));

        JobConfProxy jobConf = api.getFinishedJobConf(jobId);
        setClassNamesFromConf(job, jobConf);
        updateCalls.addCall(StoreCall.newInstance(DataEntityCollection.JOB_CONF_HISTORY, jobConf));

        CountersProxy jobCounters = api.getFinishedJobCounters(jobId);
        updateCalls.addCall(StoreCall.newInstance(COUNTER_HISTORY, jobCounters));

        Utils.updateJobStatisticsFromCounters(job, jobCounters);
        updateCalls.addCall(StoreCall.newInstance(JOB_HISTORY, job));

        final List<TaskProfile> tasks = api.getFinishedTasksInfo(jobId);
        for (TaskProfile task : tasks) {
            api.addFinishedAttemptInfo(task);
            task.setAppId(appId);
            if (job.getSplitLocations() != null && task.getHttpAddress() != null) {
                int splitIndex = Utils.parseTaskId(task.getId()).getId();
                if (job.getSplitLocations().get(splitIndex).equals(task.getHttpAddress()))
                    task.setLocal(true);
            }

            CountersProxy counters = api.getFinishedTaskCounters(jobId, task.getId());
            if (counters != null) {
                updateCalls.addCall(DeleteByIdCall.newInstance(COUNTER, counters.getId()));
                updateCalls.addCall(StoreCall.newInstance(COUNTER_HISTORY, counters));

                Utils.updateTaskStatisticsFromCounters(task, counters);
                updateCalls.addCall(StoreCall.newInstance(TASK_HISTORY, task));
            }
        }

        try {
            db.executeDatabaseCall(updateCalls);
        } catch (Exception e) {
            logger.error("Could not move app data to history", e);
        }
    }

    private void updateAppInfo(final AppProfile app) {
        db.executeDatabaseCall(UpdateOrStoreCall.newInstance(APP, app));
        if (auditEnabled) {
            db.executeDatabaseCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(APP, app)));
        }

        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            TransactionCall updateCalls = TransactionCall.newInstance();
            TransactionCall auditCalls = TransactionCall.newInstance();

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

            updateCalls.addCall(UpdateOrStoreCall.newInstance(COUNTER, jobCounters));
            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(COUNTER, jobCounters)));

            final List<TaskProfile> tasks = api.getRunningTasksInfo(job);
            if (tasks == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }

            for (TaskProfile task : tasks) {
                task.setAppId(app.getId());
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

                updateCalls.addCall(UpdateOrStoreCall.newInstance(COUNTER, counters));
                auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(COUNTER, jobCounters)));

                Utils.updateTaskStatisticsFromCounters(task, counters);
                updateCalls.addCall(UpdateOrStoreCall.newInstance(TASK, task));
                auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(TASK, task)));
            }

            Utils.updateJobStatisticsFromTasks(job, tasks);
            updateCalls.addCall(UpdateOrStoreCall.newInstance(JOB, job));
            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB, job)));

            db.executeDatabaseCall(updateCalls);

            if (auditEnabled) {
                db.executeDatabaseCall(auditCalls);
            }

        } else {
            //app is not yet tracked
            logger.trace("App " + app.getId() + " is not tracked");
            if (!running.contains(app.getId())) {
                // get job info directly from the conf in the staging dir
                try {
                    JobProfile job = getAndStoreSubmittedJobInfo(conf, app.getId(), app.getUser(), db);
                    if (auditEnabled && job != null) {
                        db.executeDatabaseCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB, job)));
                    }
                } catch (Exception e) {
                    logger.error("Could not get job info from staging dir!", e);
                }
            }
        }
    }

    public JobProfile getCurrentProfileForApp(String appId, String user) {
        JobProfile profile = db.executeDatabaseCall(JobForAppCall.newInstance(appId, user)).getEntity();
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
                                                  Database db) throws IOException {
        ApplicationId realAppId = Utils.parseApplicationId(appId);
        final JobConfProxy confProxy = getSubmittedJobConf(conf, realAppId, user);
        final JobProfile job = getSubmittedJobInfo(confProxy, realAppId);
        job.setTotalMapTasks(job.getInputSplits());
        int reduces = 0;
        String reducesString = confProxy.getEntry(MRJobConfig.NUM_REDUCES);
        if (reducesString != null && reducesString.length() > 0)
            reduces = Integer.valueOf(reducesString);
        job.setTotalReduceTasks(reduces);
        TransactionCall transaction = TransactionCall.newInstance();
        transaction.addCall(StoreCall.newInstance(JOB, job))
                .addCall(StoreCall.newInstance(DataEntityCollection.JOB_CONF, confProxy));
        try {
            db.executeDatabaseCall(transaction);
        } catch (DuplicateKeyException e) {
            // this is possible; do nothing
        } catch (Exception e) {
            logger.warn("Exception occurred when storing new job info", e);
        }
        return job;
    }

    private static JobProfile getJobProfileFromConf(ApplicationId appId,
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
        profile.setAppId(appId.toString());
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

    private static JobProfile getSubmittedJobInfo(JobConfProxy confProxy, ApplicationId appId) throws IOException {
        FileSystem fs = FileSystem.get(confProxy.getConf());
        String jobConfDirPath = confProxy.getConfPath();
        Path jobConfDir = null;
        try {
            jobConfDir = new Path(new URI(jobConfDirPath));
            //DANGER We assume there can only be one job / application
            return getJobProfileFromConf(
                    appId,
                    Utils.composeJobId(appId.getClusterTimestamp(), appId.getId()),
                    fs,
                    confProxy,
                    jobConfDir
            );
        } catch (URISyntaxException e) {
            throw new PosumException("Invalid jobConfDir path " + jobConfDirPath, e);
        }
    }

    private static JobConfProxy getSubmittedJobConf(Configuration conf, final ApplicationId appId, String user) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path confPath = MRApps.getStagingAreaDir(conf,
                user != null ? user : UserGroupInformation.getCurrentUser().getUserName());
        confPath = fs.makeQualified(confPath);

        //DANGER We assume there can only be one job / application
        Path jobConfDir = new Path(confPath,
                Utils.composeJobId(appId.getClusterTimestamp(), appId.getId()).toString());
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
