package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionModel;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardPredictor;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;

public abstract class SimpleRateBasedPredictor<
  M extends PredictionModel<P>,
  P extends PredictionProfile> extends JobBehaviorPredictor<M, P> {

  private static final Log logger = LogFactory.getLog(StandardPredictor.class);

  public SimpleRateBasedPredictor(Configuration conf) {
    super(conf);
  }

  protected <T extends PredictionStats> TaskPredictionOutput handleNoMapRateInfo(JobProfile job, T historicalStats, T jobStats) {
    Double avgDuration = getAnyStat(MAP_DURATION, historicalStats, jobStats);
    logger.trace("Incomplete map rate info for " + job.getId() + ". Trying average duration " + avgDuration);
    if (avgDuration != null)
      return new TaskPredictionOutput(avgDuration.longValue());
    return handleNoMapInfo(job);
  }

  protected static Long predictMapByRate(JobProfile job, Long taskInput, Double avgRate) {
    if (taskInput == null || avgRate == null)
      return null;
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double duration = Math.max(taskInput, 1.0) / avgRate;
    logger.trace("Map duration for " + job.getId() + " should be " + taskInput + " / " + avgRate + " = " + duration);
    return duration.longValue();
  }

  protected static Long predictReduceByRate(JobProfile job, Double avgSelectivity, Double avgRate) {
    if (job.getTotalSplitSize() == null || avgSelectivity == null || avgRate == null)
      return null;
    Double inputPerTask = calculateInputPerReduce(job, avgSelectivity);
    Double duration = inputPerTask / avgRate;
    logger.trace("Reduce duration for " + job.getId() + " should be " + inputPerTask + " / " + avgRate + " = " + duration);
    return duration.longValue();
  }

  protected static Double calculateInputPerReduce(JobProfile job, Double avgSelectivity) {
    // calculate how much reduce input remains
    Double inputLeft = orZero(job.getTotalSplitSize()) * avgSelectivity - orZero(job.getReduceInputBytes());
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    return Math.max(inputLeft / (job.getTotalReduceTasks() - job.getCompletedReduces()), 1.0);
  }
}
