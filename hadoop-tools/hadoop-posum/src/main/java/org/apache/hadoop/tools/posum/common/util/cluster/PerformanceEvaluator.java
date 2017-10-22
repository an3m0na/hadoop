package org.apache.hadoop.tools.posum.common.util.cluster;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.Utils.getDuration;

public class PerformanceEvaluator {
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
      if (job.getDeadline() == null) {
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
        penalty += Math.pow((Math.max(job.getFinishTime() - job.getDeadline(), 0)), 2);
        dcNum++;
      }
    }
    return CompoundScorePayload.newInstance(slowdown, dcNum > 0 ? penalty / dcNum : 0.0, cost);
  }
}
