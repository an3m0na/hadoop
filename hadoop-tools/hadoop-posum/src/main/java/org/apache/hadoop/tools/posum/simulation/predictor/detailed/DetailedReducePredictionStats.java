package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.AveragingStatEntry;

import java.util.List;

import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MERGE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.REDUCE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_FIRST_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_TYPICAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

class DetailedReducePredictionStats extends PredictionStats<AveragingStatEntry> {

  DetailedReducePredictionStats(int relevance) {
    super(relevance, REDUCE_DURATION, REDUCE_RATE, MERGE_RATE, SHUFFLE_TYPICAL_RATE, SHUFFLE_FIRST_DURATION);
  }

  public void addSamples(JobProfile job, List<TaskProfile> tasks) {
    int sampleNo = job.getCompletedReduces();
    Long avgDuration = job.getAvgReduceDuration();

    if (sampleNo > 0 && avgDuration != null) {
      addEntry(REDUCE_DURATION, new AveragingStatEntry(avgDuration, sampleNo));

      long shuffleRate = 0, shuffleFirst = 0;
      long totalInputSize = 0;
      int shuffleTypicalNo = 0, shuffleFirstNo = 0;

      long mapFinish = -1; // keeps track of the finish time of the last map task
      // calculate when the last map task finished
      for (TaskProfile task : tasks) {
        if (getDuration(task) > 0 && task.getType() == MAP && mapFinish < task.getFinishTime())
          mapFinish = task.getFinishTime();
      }
      // parse reduce stats
      for (TaskProfile task : tasks) {
        if (getDuration(task) <= 0 || task.getType() != REDUCE)
          continue;
        if (task.getInputBytes() != null) {
          totalInputSize += task.getInputBytes();
        }
        if (task.getShuffleTime() != null) {
          if (task.getStartTime() < mapFinish) {
            // first shuffle
            Double duration = task.getStartTime().doubleValue() - mapFinish + task.getShuffleTime();
            shuffleFirst += duration;
            shuffleFirstNo++;
          } else if (task.getInputBytes() != null) { // typical shuffle; calculate rate
            // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
            Double inputSize = Math.max(task.getInputBytes(), 1.0);
            shuffleRate += inputSize / task.getShuffleTime();
            shuffleTypicalNo++;
          }
        }
      }

      // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
      Double avgInputSize = Math.max(1.0 * totalInputSize / sampleNo, 1.0);
      if (job.getAvgReduceTime() != null)
        addEntry(REDUCE_RATE, new AveragingStatEntry(avgInputSize / job.getAvgReduceTime(), sampleNo));
      if (job.getAvgMergeTime() != null)
        addEntry(MERGE_RATE, new AveragingStatEntry(avgInputSize / job.getAvgMergeTime(), sampleNo));
      if (shuffleFirstNo > 0)
        addEntry(SHUFFLE_FIRST_DURATION, new AveragingStatEntry(shuffleFirst / shuffleFirstNo, shuffleFirstNo));
      if (shuffleTypicalNo > 0)
        addEntry(SHUFFLE_TYPICAL_RATE, new AveragingStatEntry(shuffleRate / shuffleTypicalNo, shuffleTypicalNo));
    }
  }

  @Override
  protected AveragingStatEntry emptyEntry() {
    return new AveragingStatEntry();
  }
}
