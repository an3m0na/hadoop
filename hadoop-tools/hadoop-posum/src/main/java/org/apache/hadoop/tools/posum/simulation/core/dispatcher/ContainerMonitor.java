package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.event.EventHandler;

import java.text.MessageFormat;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer.AM_TYPE;

public class ContainerMonitor implements EventHandler<ContainerEvent> {
  private static final Log LOG = LogFactory.getLog(ContainerMonitor.class);
  private SimulationContext simulationContext;
  private Database db;

  public ContainerMonitor(SimulationContext simulationContext, Database db) {
    this.simulationContext = simulationContext;
    this.db = db;
  }

  @Override
  public void handle(ContainerEvent event) {
    if (AM_TYPE.equals(event.getContainer().getType()))
      return;
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
    TaskProfile task = db.execute(FindByIdCall.newInstance(TASK, container.getTaskId())).getEntity();
    task.setStartTime(simulationContext.getCurrentTime());
    task.setHttpAddress(container.getNodeId().getHost());
    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(UpdateOrStoreCall.newInstance(TASK, task));
    db.execute(transaction);
    LOG.debug(MessageFormat.format("Container started: {0}", container.getId()));
  }

  private void containerFinished(SimulatedContainer container) {
    TaskProfile task = db.execute(FindByIdCall.newInstance(TASK, container.getTaskId())).getEntity();
    task.setStartTime(simulationContext.getCurrentTime());
    JobProfile job = db.execute(FindByIdCall.newInstance(JOB, task.getJobId())).getEntity();
    if (task.getType() == TaskType.MAP) {
      int completedMaps = orZero(job.getCompletedMaps());
      job.setCompletedMaps(completedMaps + 1);
    } else {
      int completedReduces = orZero(job.getCompletedReduces());
      job.setCompletedReduces(completedReduces + 1);
    }
    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(UpdateOrStoreCall.newInstance(TASK, task))
      .addCall(UpdateOrStoreCall.newInstance(JOB, job));
    db.execute(transaction);
    LOG.debug(MessageFormat.format("Container finished: {0}", container.getId()));
  }
}
