package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleReducePredictionStats;

import java.util.List;

import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MERGE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.REDUCE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_FIRST_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_TYPICAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

class DetailedReducePredictionStats extends SimpleReducePredictionStats {

  DetailedReducePredictionStats(int relevance) {
    super(relevance, REDUCE_DURATION, REDUCE_RATE, MERGE_RATE, SHUFFLE_TYPICAL_RATE, SHUFFLE_FIRST_DURATION);
  }

  @Override
  public void addSamples(JobProfile job, List<TaskProfile> tasks) {
    long mapFinish = -1; // keeps track of the finish time of the last map task
    // calculate when the last map task finished
    for (TaskProfile task : tasks) {
      if (getDuration(task) > 0 && task.getType() == MAP && mapFinish < task.getFinishTime())
        mapFinish = task.getFinishTime();
    }
    for (TaskProfile task : tasks) {
      if (getDuration(task) <= 0 || task.getType() != REDUCE)
        continue;
      addSample(job, task);
      if (task.getStartTime() < mapFinish && task.getShuffleTime() != 0) {
        // first shuffle
        Double duration = task.getStartTime().doubleValue() - mapFinish + task.getShuffleTime();
        addSample(SHUFFLE_FIRST_DURATION, duration);
      } else if (task.getInputBytes() != null) { // typical shuffle; calculate rate
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Double inputSize = Math.max(task.getInputBytes(), 1.0);
        addSample(SHUFFLE_TYPICAL_RATE, inputSize / task.getShuffleTime());
      }
    }
  }

  @Override
  public void addSample(JobProfile job, TaskProfile task) {
    Double duration = getDuration(task).doubleValue();
    addSample(REDUCE_DURATION, duration);
    if (task.getInputBytes() != null) {
      // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
      Double inputSize = Math.max(task.getInputBytes(), 1.0);
      if (orZero(task.getReduceTime()) > 0)
        addSample(REDUCE_RATE, inputSize / task.getReduceTime());
      if (orZero(task.getMergeTime()) > 0)
        addSample(MERGE_RATE, inputSize / task.getMergeTime());
    }
  }
}
