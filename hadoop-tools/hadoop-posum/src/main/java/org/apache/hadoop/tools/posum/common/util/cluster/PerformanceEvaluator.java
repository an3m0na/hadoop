package org.apache.hadoop.tools.posum.common.util.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;

import java.text.MessageFormat;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;

public class PerformanceEvaluator {
  private static final Log LOG = LogFactory.getLog(PerformanceEvaluator.class);
  private static final Long MIN_EXECUTION_TIME = 10000L;

  private Double slowdown = 0.0;
  private Double penalty = 0.0;
  private Double cost = 0.0; // TODO this when cluster resizing is implemented
  private DatabaseProvider dbProvider;
  private int dcNum = 0;

  public PerformanceEvaluator(DatabaseProvider dbProvider) {
    this.dbProvider = dbProvider;
  }

  public CompoundScorePayload evaluate() {
    Database db = dbProvider.getDatabase();
    List<JobProfile> finishedJobs = db.execute(FindByQueryCall.newInstance(DataEntityCollection.JOB_HISTORY, null)).getEntities();
    for (JobProfile job : finishedJobs) {
      if (orZero(job.getDeadline()) == 0) {
        // BC job; calculate slowdown
        long runtime = getDuration(job);
        FindByQueryCall findTasks = FindByQueryCall.newInstance(DataEntityCollection.TASK_HISTORY, QueryUtils.is("jobId", job.getId()));
        List<TaskProfile> tasks = db.execute(findTasks).getEntities();
        long executionTime = 0;
        for (TaskProfile task : tasks) {
          executionTime += getDuration(task);
        }
        if(LOG.isTraceEnabled()){
          if(dbProvider instanceof SimulationContext){
            String simulation = ((SimulationContext) dbProvider).getSchedulerClass().getSimpleName();
            LOG.trace(MessageFormat.format("Sim={0}: Performance for job {1}: runtime={2} executionTime={3}", simulation, job.getId(), runtime, executionTime));
          } else{
            LOG.trace(MessageFormat.format("Online: Performance for job {0}: runtime={1} executionTime={2}", job.getId(), runtime, executionTime));
          }
        }
        slowdown += 1.0 * runtime / Math.max(executionTime, MIN_EXECUTION_TIME);
      } else {
        // DC job; calculate deadline violation
        if(LOG.isTraceEnabled()){
          if(dbProvider instanceof SimulationContext){
            String simulation = ((SimulationContext) dbProvider).getSchedulerClass().getSimpleName();
            LOG.trace(MessageFormat.format("Sim={0}: Performance for job {1}: deadlineViolation={2}", simulation, job.getId(), job.getFinishTime() - job.getDeadline()));
          } else{
            LOG.trace(MessageFormat.format("Online: Performance for job {0}: deadlineViolation={1}", job.getId(), job.getFinishTime() - job.getDeadline()));
          }
        }
        penalty += Math.pow((Math.max(job.getFinishTime() - job.getDeadline(), 0)), 2);
        dcNum++;
      }
    }
    return CompoundScorePayload.newInstance(slowdown, dcNum > 0 ? Math.sqrt(penalty / dcNum) : 0.0, cost);
  }
}
