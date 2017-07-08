package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
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
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;

public class AppInfoCollector {

  private static Log logger = LogFactory.getLog(AppInfoCollector.class);

  private Set<String> running = new HashSet<>();
  private Set<String> finished = new HashSet<>();
  private Database db;
  private HadoopAPIClient api;
  private boolean auditEnabled = true;
  private JobInfoCollector jobInfoCollector;
  private TaskInfoCollector taskInfoCollector;

  AppInfoCollector() {

  }

  public AppInfoCollector(Configuration conf, Database db) {
    this.db = db;
    this.api = new HadoopAPIClient();
    this.jobInfoCollector = new JobInfoCollector(conf, db);
    this.taskInfoCollector = new TaskInfoCollector();
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
    db.notifyUpdate();
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
      .addCall(StoreCall.newInstance(APP_HISTORY, app))
      .addCall(DeleteByQueryCall.newInstance(JOB, QueryUtils.is("appId", appId)))
      .addCall(DeleteByQueryCall.newInstance(TASK, QueryUtils.is("appId", appId)))
      .addCall(DeleteByIdCall.newInstance(JOB_CONF, job.getId()))
      .addCall(DeleteByIdCall.newInstance(COUNTER, job.getId()));

    updateCalls.addCall(StoreCall.newInstance(JOB_CONF_HISTORY, jobInfo.getConf()));

    updateCalls.addCall(StoreCall.newInstance(JOB_HISTORY, job));
    updateCalls.addCall(StoreCall.newInstance(COUNTER_HISTORY, jobInfo.getJobCounters()));

    TaskInfo taskInfo = taskInfoCollector.getFinishedTaskInfo(job);

    for (TaskProfile task : taskInfo.getTasks()) {
      updateCalls.addCall(StoreCall.newInstance(TASK_HISTORY, task));
    }
    for (CountersProxy counters : taskInfo.getCounters()) {
      updateCalls.addCall(DeleteByIdCall.newInstance(COUNTER, counters.getId()));
      updateCalls.addCall(StoreCall.newInstance(COUNTER_HISTORY, counters));
    }

    try {
      db.execute(updateCalls);
    } catch (Exception e) {
      logger.error("Could not move app data to history", e);
    }
  }

  private void updateAppInfo(final AppProfile app) {
    TransactionCall updateCalls = TransactionCall.newInstance();
    TransactionCall auditCalls = TransactionCall.newInstance();

    updateCalls.addCall(UpdateOrStoreCall.newInstance(APP, app));
    auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(APP, app)));

    JobInfo jobInfo = jobInfoCollector.getRunningJobInfo(app);
    if (jobInfo == null) {
      if (api.checkAppFinished(app))
        moveAppToHistory(app);
      return;
    }
    JobConfProxy jobConf = jobInfo.getConf();
    if (jobConf != null) {
      updateCalls.addCall(UpdateOrStoreCall.newInstance(JOB_CONF, jobConf));
      auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB_CONF, jobConf)));
    }
    CountersProxy jobCounters = jobInfo.getJobCounters();
    if (jobCounters != null) {
      updateCalls.addCall(UpdateOrStoreCall.newInstance(COUNTER, jobCounters));
      auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(COUNTER, jobCounters)));
    }

    JobProfile job = jobInfo.getProfile();
    TaskInfo taskInfo = taskInfoCollector.getRunningTaskInfo(job);
    if (taskInfo == null) {
      if (api.checkAppFinished(app)) {
        moveAppToHistory(app);
        return;
      }
      // add stubs for each task and exit
      List<TaskProfile> taskStubs = jobInfo.getTaskStubs();
      if (taskStubs != null) {
        // it means the profile is not new, but task stubs should be already there
        updateCalls.addCall(StoreAllCall.newInstance(TASK, taskStubs));
        for (TaskProfile task : taskStubs) {
          auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(TASK, task)));
        }
      }

    } else {
      for (TaskProfile task : taskInfo.getTasks()) {
        updateCalls.addCall(UpdateOrStoreCall.newInstance(TASK, task));
        auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(TASK, task)));
      }

      for (CountersProxy counters : taskInfo.getCounters()) {
        updateCalls.addCall(UpdateOrStoreCall.newInstance(COUNTER, counters));
        auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(COUNTER, counters)));
      }

      Utils.updateJobStatisticsFromTasks(job, taskInfo.getTasks());
    }

    updateCalls.addCall(UpdateOrStoreCall.newInstance(JOB, job));
    auditCalls.addCall(StoreCall.newInstance(HISTORY, new HistoryProfilePBImpl<>(JOB, job)));

    db.execute(updateCalls);

    if (auditEnabled)
      db.execute(auditCalls);
  }

}
