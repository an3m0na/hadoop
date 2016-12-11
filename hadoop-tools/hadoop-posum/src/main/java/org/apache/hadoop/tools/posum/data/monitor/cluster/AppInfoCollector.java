package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;

public class AppInfoCollector {

    private static Log logger = LogFactory.getLog(AppInfoCollector.class);

    private Set<String> running = new HashSet<>();
    private Set<String> finished = new HashSet<>();
    private final Database db;
    private final HadoopAPIClient api;
    private final boolean auditEnabled;
    private final JobInfoCollector jobInfoCollector;
    private final TaskInfoCollector taskInfoCollector;

    public AppInfoCollector(Configuration conf, Database db) {
        this.db = db;
        this.api = new HadoopAPIClient();
        this.jobInfoCollector = new JobInfoCollector(conf, api, db);
        this.taskInfoCollector = new TaskInfoCollector(api);
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

        JobInfo jobInfo = jobInfoCollector.getFinishedJobInfo(app);
        JobProfile job = jobInfo.getProfile();

        TransactionCall updateCalls = TransactionCall.newInstance()
                .addCall(DeleteByIdCall.newInstance(APP, appId))
                .addCall(StoreCall.newInstance(DataEntityCollection.APP_HISTORY, app))
                .addCall(DeleteByQueryCall.newInstance(JOB, QueryUtils.is("appId", appId)))
                .addCall(DeleteByQueryCall.newInstance(TASK, QueryUtils.is("appId", appId)))
                .addCall(DeleteByIdCall.newInstance(JOB_CONF, job.getId()))
                .addCall(DeleteByIdCall.newInstance(COUNTER, job.getId()));

        updateCalls.addCall(StoreCall.newInstance(DataEntityCollection.JOB_CONF_HISTORY, jobInfo.getConf()));

        Utils.updateJobStatisticsFromCounters(job, jobInfo.getJobCounters());
        updateCalls.addCall(StoreCall.newInstance(JOB_HISTORY, job));
        updateCalls.addCall(StoreCall.newInstance(COUNTER_HISTORY, jobInfo.getJobCounters()));

        final List<TaskProfile> tasks = taskInfoCollector.getFinishedTaskInfo(job);
        final List<CountersProxy> taskCounters = taskInfoCollector.updateFinishedTasksFromCounters(tasks);

        for (TaskProfile task : tasks) {
            updateCalls.addCall(StoreCall.newInstance(TASK_HISTORY, task));
        }
        for (CountersProxy counters : taskCounters) {
            updateCalls.addCall(DeleteByIdCall.newInstance(COUNTER, counters.getId()));
            updateCalls.addCall(StoreCall.newInstance(COUNTER_HISTORY, counters));
        }

        try {
            db.executeDatabaseCall(updateCalls);
        } catch (Exception e) {
            logger.error("Could not move app data to history", e);
        }
    }

    private void updateAppInfo(final AppProfile app) {
        TransactionCall updateCalls = TransactionCall.newInstance();
        TransactionCall auditCalls = TransactionCall.newInstance();

        updateCalls.addCall(UpdateOrStoreCall.newInstance(APP, app));
        if (auditEnabled)
            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(APP, app)));

        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            JobInfo jobInfo = jobInfoCollector.getRunningJobInfo(app);
            if (jobInfo == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }
            if (jobInfo.getConf() != null) {
                updateCalls.addCall(UpdateOrStoreCall.newInstance(JOB_CONF, jobInfo.getConf()));
                auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB_CONF, jobInfo.getConf())));
            }
            JobProfile job = jobInfo.getProfile();
            updateCalls.addCall(UpdateOrStoreCall.newInstance(COUNTER, jobInfo.getJobCounters()));
            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(COUNTER, jobInfo.getJobCounters())));

            List<TaskProfile> tasks = taskInfoCollector.getRunningTaskInfo(job);
            if (tasks == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }

            List<CountersProxy> taskCounters = taskInfoCollector.updateRunningTasksFromCounters(tasks);
            if (taskCounters == null) {
                if (api.checkAppFinished(app))
                    moveAppToHistory(app);
                return;
            }

            for (TaskProfile task : tasks) {
                updateCalls.addCall(UpdateOrStoreCall.newInstance(TASK, task));
                auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(TASK, task)));
            }

            for (CountersProxy counters : taskCounters) {
                updateCalls.addCall(UpdateOrStoreCall.newInstance(COUNTER, counters));
                auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(COUNTER, counters)));
            }

            Utils.updateJobStatisticsFromTasks(job, tasks);
            updateCalls.addCall(UpdateOrStoreCall.newInstance(JOB, job));
            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB, job)));

        } else {
            //app is not yet tracked
            logger.trace("App " + app.getId() + " is not tracked");
            if (!running.contains(app.getId())) {
                // get profile info directly from the conf in the staging dir
                try {
                    JobInfo jobInfo = jobInfoCollector.getSubmittedJobInfo(app.getId(), app.getUser());
                    if (jobInfo != null) {
                        updateCalls.addCall(StoreCall.newInstance(JOB, jobInfo.getProfile()));
                        updateCalls.addCall(StoreCall.newInstance(JOB_CONF, jobInfo.getConf()));
                        if (auditEnabled) {
                            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB, jobInfo.getProfile())));
                            auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB_CONF, jobInfo.getConf())));
                        }
                    }
                } catch (Exception e) {
                    logger.error("Could not get profile info from staging dir!", e);
                }
            }
        }

        db.executeDatabaseCall(updateCalls);

        if (auditEnabled) {
            db.executeDatabaseCall(auditCalls);
        }
    }


}
