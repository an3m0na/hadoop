package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
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
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.tools.posum.common.util.Utils.parseApplicationId;

public class ApplicationMonitor implements EventHandler<ApplicationEvent> {
  private static final Log LOG = LogFactory.getLog(ApplicationMonitor.class);
  private SimulationContext simulationContext;
  private final Database db;
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
    }
  }

  private void applicationSubmitted(String oldAppIdString, ApplicationId appId) {
    LOG.trace(MessageFormat.format("Sim={0} T={1}: Handling application submitted for {2}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), oldAppIdString));
    String appIdString = appId.toString();

    TransactionCall transaction = TransactionCall.newInstance();
    AppProfile app = sourceDb.execute(FindByIdCall.newInstance(APP, oldAppIdString)).getEntity();
    if (app != null) {
      app.setId(appIdString);
      app.setStartTime(simulationContext.getCurrentTime());
      transaction.addCall(StoreCall.newInstance(APP, app));
    }
    ApplicationId oldAppId = parseApplicationId(oldAppIdString);

    // calculate job id in case the app was not registered in the database (e.g. for an old trace)
    JobId oldJobId = MRBuilderUtils.newJobId(oldAppId, oldAppId.getId());
    String oldJobIdString = oldJobId.toString();
    JobProfile job = sourceDb.execute(FindByIdCall.newInstance(JOB, oldJobIdString)).getEntity();
    job.setAppId(appIdString);
    if (simulationContext.isOnlineSimulation() && job.getHostName() != null)
      job.setStartTime(job.getStartTime() - simulationContext.getClusterTimeAtStart());
    else {
      job.setStartTime(null);
      job.setFinishTime(null);
      job.setHostName(null);
    }
    if (orZero(job.getDeadline()) != 0) {
      long newDeadline = job.getDeadline() - simulationContext.getClusterTimeAtStart();
      if (newDeadline == 0)
        newDeadline = 1; // to avoid considering this job a batch job due to deadline=0
      job.setDeadline(newDeadline);
    }

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
      if (simulationContext.isOnlineSimulation() && task.getHostName() != null) {
        task.setStartTime(task.getStartTime() - simulationContext.getClusterTimeAtStart());
        if (task.getFinishTime() != null)
          task.setFinishTime(task.getFinishTime() - simulationContext.getClusterTimeAtStart());
      } else {
        task.setStartTime(null);
        task.setFinishTime(null);
        task.setHostName(null);
      }
      CountersProxy taskCounters = sourceDb.execute(FindByIdCall.newInstance(COUNTER, task.getId())).getEntity();
      if (taskCounters != null) {
        transaction.addCall(StoreCall.newInstance(COUNTER, taskCounters));
      }
    }
    synchronized (db) {
      db.execute(transaction);
    }
    db.notifyUpdate();
    LOG.trace(MessageFormat.format("Sim={0} T={1}: App submitted. {1} becomes {2}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), oldAppId, appId));
  }
}
