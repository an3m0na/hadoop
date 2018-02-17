package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;

import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

public class BasicPredictor extends JobBehaviorPredictor<BasicPredictionModel, BasicPredictionProfile> {

  public BasicPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected BasicPredictionModel initializeModel() {
    return new BasicPredictionModel(historyBuffer);
  }

  @Override
  protected BasicPredictionProfile buildPredictionProfile(JobProfile job) {
    BasicPredictionStats jobStats = new BasicPredictionStats(0);
    BasicPredictionProfile predictionProfile = new BasicPredictionProfile(job, jobStats);
    predictionProfile.deserialize();

    if (jobStats.getSampleSize(MAP_DURATION) + jobStats.getSampleSize(REDUCE_DURATION) !=
      job.getCompletedMaps() + job.getCompletedReduces()) {
      // new information is available
      jobStats.addSamples(job);
      predictionProfile.markUpdated();
    }

    return predictionProfile;
  }

  @Override
  protected TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input, BasicPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    if (job.getAvgMapDuration() != null)
      return new TaskPredictionOutput(job.getAvgMapDuration());
    BasicPredictionStats stats = model.getRelevantStats(job);
    if (stats == null)
      return handleNoMapInfo(job);
    Double avgDuration = stats.getAverage(MAP_DURATION);
    if (avgDuration != null)
      return new TaskPredictionOutput(avgDuration.longValue());
    return handleNoMapInfo(job);
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input, BasicPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    if (job.getAvgReduceDuration() != null)
      return new TaskPredictionOutput(job.getAvgReduceDuration());
    BasicPredictionStats stats = model.getRelevantStats(job);
    if (stats == null)
      return handleNoReduceInfo(job);
    Double avgDuration = stats.getAverage(REDUCE_DURATION);
    if (avgDuration != null)
      return new TaskPredictionOutput(avgDuration.longValue());
    return handleNoReduceInfo(job);
  }
}
