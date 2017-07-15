package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;

import java.text.MessageFormat;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.apache.hadoop.tools.posum.common.util.Utils.parseApplicationId;

public class ApplicationMonitor implements EventHandler<ApplicationEvent> {
  private static final Log LOG = LogFactory.getLog(ApplicationMonitor.class);
  private SimulationContext simulationContext;
  private Database db;
  private Database sourceDb;

  public ApplicationMonitor(SimulationContext simulationContext, Database db, Database sourceDb) {
    this.simulationContext = simulationContext;
    this.db = db;
    this.sourceDb = sourceDb;
  }

  @Override
  public void handle(ApplicationEvent event) {
    switch (event.getType()) {
      case APPLICATION_SUBMITTED:
        applicationSubmitted(event.getOldAppId(), event.getNewAppId());
        break;
      case APPLICATION_FINISHED:
        applicationFinished(event.getNewAppId());
        break;
    }
  }

  private void applicationFinished(ApplicationId appId) {
    String appIdString = appId.toString();

    TransactionCall transaction = TransactionCall.newInstance();
    AppProfile app = db.execute(FindByIdCall.newInstance(APP, appIdString)).getEntity();
    if (app != null) {
      transaction.addCall(DeleteByIdCall.newInstance(APP, appIdString));
      transaction.addCall(StoreCall.newInstance(APP_HISTORY, app));
    }
    JobProfile job = db.execute(JobForAppCall.newInstance(appIdString)).getEntity();
    job.setFinishTime(simulationContext.getCurrentTime());
    transaction.addCall(DeleteByIdCall.newInstance(JOB, job.getId()));
    transaction.addCall(StoreCall.newInstance(JOB_HISTORY, job));

    JobConfProxy jobConf = db.execute(FindByIdCall.newInstance(JOB_CONF, job.getId())).getEntity();
    if (jobConf != null) {
      transaction.addCall(DeleteByIdCall.newInstance(JOB_CONF, job.getId()));
      transaction.addCall(StoreCall.newInstance(JOB_CONF_HISTORY, jobConf));
    }

    CountersProxy jobCounters = db.execute(FindByIdCall.newInstance(COUNTER, job.getId())).getEntity();
    if (jobCounters != null) {
      transaction.addCall(DeleteByIdCall.newInstance(COUNTER, job.getId()));
      transaction.addCall(StoreCall.newInstance(COUNTER_HISTORY, jobCounters));
    }

    List<TaskProfile> tasks =
      db.execute(FindByQueryCall.newInstance(TASK, QueryUtils.is("jobId", job.getId()))).getEntities();
    transaction.addCall(DeleteByQueryCall.newInstance(TASK, QueryUtils.is("jobId", job.getId())));
    transaction.addCall(StoreAllCall.newInstance(TASK_HISTORY, tasks));
    for (TaskProfile task : tasks) {
      CountersProxy taskCounters = db.execute(FindByIdCall.newInstance(COUNTER, task.getId())).getEntity();
      if (taskCounters != null) {
        transaction.addCall(DeleteByIdCall.newInstance(COUNTER, task.getId()));
        transaction.addCall(StoreCall.newInstance(COUNTER_HISTORY, taskCounters));
      }
    }

    db.execute(transaction);
    LOG.debug(MessageFormat.format("App finished: {0}", appId));
  }

  private void applicationSubmitted(String oldAppIdString, ApplicationId appId) {
    LOG.debug("Doing application submitted for " + oldAppIdString);
    String appIdString = appId.toString();

    TransactionCall transaction = TransactionCall.newInstance();
    AppProfile app = sourceDb.execute(FindByIdCall.newInstance(APP, oldAppIdString)).getEntity();
    if (app != null) {
      app.setId(appIdString);
      transaction.addCall(StoreCall.newInstance(APP, app));
    }
    ApplicationId oldAppId = parseApplicationId(oldAppIdString);

    // calculate job id in case the app was not registered in the database (e.g. for an old trace)
    JobId oldJobId = MRBuilderUtils.newJobId(oldAppId, oldAppId.getId());
    String oldJobIdString = oldJobId.toString();
    JobProfile job = sourceDb.execute(FindByIdCall.newInstance(JOB, oldJobIdString)).getEntity();
    job.setAppId(appIdString);
    job.setStartTime(simulationContext.getCurrentTime());
    transaction.addCall(StoreCall.newInstance(JOB, job));

    JobConfProxy jobConf = sourceDb.execute(FindByIdCall.newInstance(JOB_CONF, oldJobIdString)).getEntity();
    if (jobConf != null) {
      transaction.addCall(StoreCall.newInstance(JOB_CONF, jobConf));
    }

    CountersProxy jobCounters = sourceDb.execute(FindByIdCall.newInstance(COUNTER, oldJobIdString)).getEntity();
    if (jobCounters != null) {
      transaction.addCall(StoreCall.newInstance(COUNTER, jobCounters));
    }

    List<TaskProfile> tasks =
      sourceDb.execute(FindByQueryCall.newInstance(TASK, QueryUtils.is("jobId", oldJobIdString))).getEntities();
    transaction.addCall(StoreAllCall.newInstance(TASK, tasks));
    for (TaskProfile task : tasks) {
      task.setAppId(appIdString);
      CountersProxy taskCounters = sourceDb.execute(FindByIdCall.newInstance(COUNTER, task.getId())).getEntity();
      if (taskCounters != null) {
        transaction.addCall(StoreCall.newInstance(COUNTER, taskCounters));
      }
    }

    db.execute(transaction);
    LOG.debug(MessageFormat.format("App submitted. {0} becomes {1}", oldAppId, appId));
  }
}
