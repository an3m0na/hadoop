package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionModel;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public abstract class SimpleRateBasedPredictor<M extends PredictionModel> extends JobBehaviorPredictor<M> {

  private static final Log logger = LogFactory.getLog(SimpleRateBasedPredictor.class);

  public SimpleRateBasedPredictor(Configuration conf) {
    super(conf);
  }

  protected List<TaskProfile> getJobTasks(String jobId, boolean fromHistory) {
    FindByQueryCall getTasks = FindByQueryCall.newInstance(fromHistory ? TASK_HISTORY : TASK,
      QueryUtils.is("jobId", jobId));
    return getDatabase().execute(getTasks).getEntities();
  }

  protected static Long getSplitSize(TaskProfile task, JobProfile job) {
    if (task != null && task.getSplitSize() != null)
      return task.getSplitSize();
    return getAvgSplitSize(job);
  }

  protected static Long getAvgSplitSize(JobProfile job) {
    if (job.getTotalInputBytes() == null || job.getTotalMapTasks() < 1)
      return null;
    // consider equal sizes; restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    return Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
  }

  protected static Long getReduceDuration(JobProfile job, Double avgSelectivity, Double avgRate) {
    if (job.getTotalInputBytes() == null || avgSelectivity == null || avgRate == null)
      return null;
    // calculate how much input the task has
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double inputPerTask = Math.max(orZero(job.getTotalInputBytes()) * avgSelectivity / job.getTotalReduceTasks(), 1);
    Double duration = inputPerTask / avgRate;
    return duration.longValue();
  }

  protected Double getMapTaskSelectivity(JobProfile job, SimpleMapPredictionStats mapStats, String selectivityKey) {
    // we try to get the selectivity from the map history
    if (mapStats == null || mapStats.getRelevance() > 1 || mapStats.getAvgSelectivity() == null) {
      // there is no history, or it is not relevant for selectivity, so get current selectivity
      String selectivityString = job.getFlexField(selectivityKey);
      logger.debug("Using own selectivity for " + job.getId() + ": " + selectivityString);
      return selectivityString != null ? Double.valueOf(selectivityString) : null;
    }
    return mapStats.getAvgSelectivity();
  }

  protected TaskPredictionOutput handleNoMapInfo(JobProfile job) {
    logger.debug("Insufficient map data for " + job.getId() + ". Using default");
    // return the default; there is nothing we can do
    return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
  }

  protected TaskPredictionOutput handleNoReduceInfo(JobProfile job, Double avgSelectivity, Double mapRate) {
    // assume the reduce processing rate is the same as the map processing rate to calculate reduce duration
    Long duration = getReduceDuration(job, avgSelectivity, mapRate);
    if (duration == null) {
      // our selectivity or map rate data is unreliable; just return default duration
      logger.debug("Insufficient reduce data for " + job.getName() + ". Using default");
      return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
    }
    logger.debug("Reduce duration computed based on map data for " + job.getId() + " as " + duration + "from (remote) mapRate=" + mapRate + " and selectivity=" + avgSelectivity);
    return new TaskPredictionOutput(duration);
  }
}
