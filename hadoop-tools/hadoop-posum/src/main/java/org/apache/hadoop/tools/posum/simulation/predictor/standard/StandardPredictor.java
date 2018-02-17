package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleRateBasedPredictor;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.REDUCE_RATE;

public class StandardPredictor extends SimpleRateBasedPredictor<StandardPredictionModel, StandardPredictionProfile> {

  public StandardPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected StandardPredictionModel initializeModel() {
    return new StandardPredictionModel(historyBuffer);
  }

  @Override
  protected StandardPredictionProfile buildPredictionProfile(JobProfile job) {
    StandardMapPredictionStats mapStats = new StandardMapPredictionStats(0);
    StandardReducePredictionStats reduceStats = new StandardReducePredictionStats(0);
    StandardPredictionProfile predictionProfile = new StandardPredictionProfile(job, mapStats, reduceStats);
    predictionProfile.deserialize();

    if (mapStats.getSampleSize(MAP_DURATION) != job.getCompletedMaps()) { // new information is available
      List<TaskProfile> tasks = getJobTasks(job.getId(), job.getFinishTime() != null);
      mapStats.addSamples(job, tasks);
      predictionProfile.markUpdated();
    }

    if (reduceStats.getSampleSize(REDUCE_DURATION) != job.getCompletedReduces()) { // new information is available
      reduceStats.addSamples(job);
      predictionProfile.markUpdated();
    }

    return predictionProfile;
  }

  @Override
  protected TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input, StandardPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    StandardMapPredictionStats historicalStats = model.getRelevantMapStats(job);
    StandardMapPredictionStats jobStats = predictionProfile.getMapStats();

    Long taskInput = getSplitSize(input.getTask(), job);
    Double avgRate = getAnyStat(MAP_RATE, historicalStats, jobStats);

    Long duration = predictMapByRate(job, taskInput, avgRate);
    if (duration == null)
      return handleNoMapRateInfo(job, historicalStats, jobStats);
    return new TaskPredictionOutput(duration);
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input, StandardPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    StandardReducePredictionStats historicalStats = model.getRelevantReduceStats(job);
    StandardReducePredictionStats jobStats = predictionProfile.getReduceStats();

    StandardMapPredictionStats historicalMapStats = model.getRelevantMapStats(job);
    StandardMapPredictionStats jobMapStats = predictionProfile.getMapStats();

    Double avgSelectivity = getRelevantStat(MAP_SELECTIVITY, historicalMapStats, jobMapStats);
    Double avgRate = getRelevantStat(REDUCE_RATE, historicalStats, jobStats);
    if (avgRate == null || avgSelectivity == null) {
      Double avgReduceDuration = getRelevantStat(REDUCE_DURATION, historicalStats, jobStats);
      if (avgReduceDuration != null)
        return new TaskPredictionOutput(avgReduceDuration.longValue());
      // get any reduce rate
      avgRate = getAnyStat(REDUCE_RATE, historicalStats, jobStats);
      if (avgRate == null) {
        // assume it is the same as any map rate
        avgRate = getAnyStat(MAP_RATE, historicalMapStats, jobMapStats);
      }
      // get any selectivity
      avgSelectivity = getAnyStat(MAP_SELECTIVITY, historicalMapStats, jobMapStats);
    }

    Long duration = predictReduceByRate(job, avgSelectivity, avgRate);
    if (duration == null)
      return handleNoReduceInfo(job);
    return new TaskPredictionOutput(duration);
  }
}
