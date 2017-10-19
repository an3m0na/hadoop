package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
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
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
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
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer.AM_TYPE;

public class ContainerMonitor implements EventHandler<ContainerEvent> {
  private static final Log LOG = LogFactory.getLog(ContainerMonitor.class);
  private SimulationContext simulationContext;
  private final Database db;

  public ContainerMonitor(SimulationContext simulationContext, Database db) {
    this.simulationContext = simulationContext;
    this.db = db;
  }

  @Override
  public void handle(ContainerEvent event) {
    switch (event.getType()) {
      case CONTAINER_STARTED:
        containerStarted(event.getContainer());
        break;
      case CONTAINER_FINISHED:
        containerFinished(event.getContainer());
        break;
    }
  }

  private void containerStarted(SimulatedContainer container) {
    if (AM_TYPE.equals(container.getType())) {
      ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();
      JobProfile job = db.execute(JobForAppCall.newInstance(appId.toString())).getEntity();
      if (job.getStartTime() == null)
        job.setStartTime(simulationContext.getCurrentTime());
      db.execute(UpdateOrStoreCall.newInstance(JOB, job));
      return;
    }
    LOG.trace(MessageFormat.format("Sim={0} T={1}: Container started for {2}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), container.getTaskId()));

    synchronized (db) {
      TaskProfile task = db.execute(FindByIdCall.newInstance(TASK, container.getTaskId())).getEntity();
      if (task == null) {
        throw new PosumException(MessageFormat.format("Sim={0} T={1}: Task {2} could not be found for container {3}",
          simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(),
          container.getTaskId(), container.getId()));
      }
      if (task.getStartTime() == null) {
        // only mark start time if it was not already running
        task.setStartTime(simulationContext.getCurrentTime());
        task.setHttpAddress(container.getNodeId().getHost());
      }
      db.execute(UpdateOrStoreCall.newInstance(TASK, task));
    }
  }

  private void containerFinished(SimulatedContainer container) {
    if (AM_TYPE.equals(container.getType())) {
      ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();
      applicationFinished(appId);
      return;
    }

    LOG.trace(MessageFormat.format("Sim={0} T={1}: Container finished for {2}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), container.getTaskId()));
    synchronized (db) {
      boolean jobFinished = false;
      TaskProfile task = db.execute(FindByIdCall.newInstance(TASK, container.getTaskId())).getEntity();
      if (task == null) {
        jobFinished = true;
        task = db.execute(FindByIdCall.newInstance(TASK_HISTORY, container.getTaskId())).getEntity();
      }
      if (task == null) {
        throw new PosumException(MessageFormat.format("Sim={0} T={1}: Task {2} could not be found for container {3}",
          simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(),
          container.getTaskId(), container.getId()));
      }
      task.setFinishTime(simulationContext.getCurrentTime());
      JobProfile job =
        db.execute(FindByIdCall.newInstance(jobFinished ? JOB_HISTORY : JOB, task.getJobId())).getEntity();
      if (task.getType() == TaskType.MAP) {
        int completedMaps = orZero(job.getCompletedMaps());
        job.setCompletedMaps(completedMaps + 1);
      } else {
        int completedReduces = orZero(job.getCompletedReduces());
        job.setCompletedReduces(completedReduces + 1);
      }
      TransactionCall transaction = TransactionCall.newInstance()
        .addCall(UpdateOrStoreCall.newInstance(jobFinished ? TASK_HISTORY : TASK, task))
        .addCall(UpdateOrStoreCall.newInstance(jobFinished ? JOB_HISTORY : JOB, job));
      db.execute(transaction);
    }
  }

  private void applicationFinished(ApplicationId appId) {
    String appIdString = appId.toString();
    LOG.trace(MessageFormat.format("Sim={0} T={1}: App finished: {1}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), appIdString));

    TransactionCall transaction = TransactionCall.newInstance();
    AppProfile app = db.execute(FindByIdCall.newInstance(APP, appIdString)).getEntity();
    if (app != null) {
      transaction.addCall(DeleteByIdCall.newInstance(APP, appIdString));
      transaction.addCall(StoreCall.newInstance(APP_HISTORY, app));
    }
    JobProfile job = db.execute(JobForAppCall.newInstance(appIdString)).getEntity();
    if (job.getFinishTime() == null) {
      job.setFinishTime(simulationContext.getCurrentTime());
      LOG.debug("Finish time for " + job.getId() + " is " + job.getFinishTime());
    }
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
    synchronized (db) {
      db.execute(transaction);
    }
  }
}
