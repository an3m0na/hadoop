package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;

import java.util.Map;

public class BasicPredictor extends JobBehaviorPredictor<BasicPredictionModel> {

  public BasicPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected BasicPredictionModel initializeModel() {
    return new BasicPredictionModel(historyBuffer);
  }

  @Override
  protected Map<String, String> getPredictionProfileUpdates(JobProfile job, boolean fromHistory) {
    return null;
  }

  @Override
  protected TaskPredictionOutput predictMapTaskDuration(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    if (job.getAvgMapDuration() != null)
      return new TaskPredictionOutput(job.getAvgMapDuration());
    BasicPredictionStats stats = model.getRelevantStats(job);
    if (stats != null && stats.getAvgMapDuration() != null) {
      new TaskPredictionOutput(stats.getAvgMapDuration().longValue());
    }
    return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskDuration(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    if (job.getAvgReduceDuration() != null)
      return new TaskPredictionOutput(job.getAvgReduceDuration());
    BasicPredictionStats stats = model.getRelevantStats(job);
    if (stats != null && stats.getAvgReduceDuration() != null) {
      new TaskPredictionOutput(stats.getAvgReduceDuration().longValue());
    }
    return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
  }
}
