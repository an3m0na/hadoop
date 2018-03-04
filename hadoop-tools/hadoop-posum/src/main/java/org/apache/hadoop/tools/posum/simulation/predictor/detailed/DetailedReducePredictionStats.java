package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.PredictionStats;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.RegressionWithFallbackStatEntry;

import java.util.List;

import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MERGE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.REDUCE_ONLY_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_FIRST_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_TYPICAL_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

class DetailedReducePredictionStats extends PredictionStats<RegressionWithFallbackStatEntry> {

  DetailedReducePredictionStats(int relevance) {
    super(relevance, REDUCE_DURATION, REDUCE_ONLY_DURATION, MERGE_DURATION, SHUFFLE_TYPICAL_DURATION, SHUFFLE_FIRST_DURATION);
  }

  public void addSamples(JobProfile job, List<TaskProfile> tasks) {
    int sampleNo = job.getCompletedReduces();
    Long avgDuration = job.getAvgReduceDuration();

    if (sampleNo > 0 && avgDuration != null) {
      addEntry(REDUCE_DURATION, new RegressionWithFallbackStatEntry(avgDuration.doubleValue(), sampleNo));

      RegressionWithFallbackStatEntry reduceOnlyDurationEntry = new RegressionWithFallbackStatEntry();
      RegressionWithFallbackStatEntry mergeDurationEntry = new RegressionWithFallbackStatEntry();
      RegressionWithFallbackStatEntry shuffleTypicalDurationEntry = new RegressionWithFallbackStatEntry();
      RegressionWithFallbackStatEntry shuffleFirstDurationEntry = new RegressionWithFallbackStatEntry();

      long mapFinish = -1; // keeps track of the finish time of the last map task
      // calculate when the last map task finished
      for (TaskProfile task : tasks) {
        if (getDuration(task) > 0 && task.getType() == MAP && mapFinish < task.getFinishTime())
          mapFinish = task.getFinishTime();
      }
      // parse reduce stats
      for (TaskProfile task : tasks) {
        double duration = getDuration(task);
        if (duration <= 0 || task.getType() != REDUCE)
          continue;
        Long inputBytes = task.getInputBytes();
        if (inputBytes != null) {
          // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
          long boundedInputSize = Math.max(inputBytes, 1);
          reduceOnlyDurationEntry.addSample(boundedInputSize, duration);
          if (task.getReduceTime() != null)
            reduceOnlyDurationEntry.addSample(boundedInputSize, task.getReduceTime());
          if (task.getMergeTime() != null)
            mergeDurationEntry.addSample(boundedInputSize, task.getMergeTime());
          if (task.getShuffleTime() != null && task.getStartTime() >= mapFinish)
            shuffleTypicalDurationEntry.addSample(boundedInputSize, task.getShuffleTime());

        }
        if (task.getShuffleTime() != null && task.getStartTime() < mapFinish) {
          // first shuffle
          duration = task.getStartTime().doubleValue() - mapFinish + task.getShuffleTime();
          shuffleFirstDurationEntry.addSample(duration);
        }
      }
      addEntry(REDUCE_ONLY_DURATION, reduceOnlyDurationEntry);
      addEntry(MERGE_DURATION, mergeDurationEntry);
      addEntry(SHUFFLE_FIRST_DURATION, shuffleFirstDurationEntry);
      addEntry(SHUFFLE_TYPICAL_DURATION, shuffleTypicalDurationEntry);
    }
  }

  @Override
  protected RegressionWithFallbackStatEntry emptyEntry() {
    return new RegressionWithFallbackStatEntry();
  }
}
