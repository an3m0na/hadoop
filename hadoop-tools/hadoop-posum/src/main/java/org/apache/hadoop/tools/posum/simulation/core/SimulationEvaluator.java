package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.Utils.getDuration;

public class SimulationEvaluator {
  private static final Long MIN_EXECUTION_TIME = 10000L;
  private Double slowdown = 0.0;
  private Double penalty = 0.0;
  private Double cost = 0.0; // TODO this when cluster resizing is implemented
  private Database db;
  private int dcNum = 0;

  public SimulationEvaluator(Database db) {
    this.db = db;
  }

  public CompoundScorePayload evaluate() {
    List<JobProfile> finishedJobs = db.execute(FindByQueryCall.newInstance(DataEntityCollection.JOB_HISTORY, null)).getEntities();
    List<JobConfProxy> finishedJobConfs = db.execute(FindByQueryCall.newInstance(DataEntityCollection.JOB_CONF_HISTORY, null)).getEntities();
    Map<String, Long> deadlinesById = new HashMap<>();
    for (JobConfProxy jobConf : finishedJobConfs) {
      String deadlineString = jobConf.getEntry(PosumConfiguration.APP_DEADLINE);
      if (deadlineString != null && deadlineString.length() > 0)
        deadlinesById.put(jobConf.getId(), Long.valueOf(deadlineString));
    }
    for (JobProfile job : finishedJobs) {
      Long deadline = deadlinesById.get(job.getId());
      if (deadline == null) {
        // BC job; calculate slowdown
        long runtime = getDuration(job);
        FindByQueryCall findTasks = FindByQueryCall.newInstance(DataEntityCollection.TASK_HISTORY, QueryUtils.is("jobId", job.getId()));
        List<TaskProfile> tasks = db.execute(findTasks).getEntities();
        long executionTime = 0;
        for (TaskProfile task : tasks) {
          executionTime += getDuration(task);
        }
        slowdown += 1.0 * runtime / Math.max(executionTime, MIN_EXECUTION_TIME);
      } else {
        // DC job; calculate deadline violation
        penalty += Math.pow((Math.max(job.getFinishTime() - deadline, 0)), 2);
        dcNum++;
      }
    }
    return CompoundScorePayload.newInstance(slowdown, dcNum > 0 ? penalty / dcNum : 0.0, cost);
  }
}
