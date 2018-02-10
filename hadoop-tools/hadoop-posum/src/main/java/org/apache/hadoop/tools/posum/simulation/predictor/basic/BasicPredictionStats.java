package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

class BasicPredictionStats extends PredictionStats {
  BasicPredictionStats(int relevance) {
    super(relevance, SimpleStatKeys.MAP_DURATION, REDUCE_DURATION);
  }

  void addSamples(List<TaskProfile> tasks) {
    for (TaskProfile task : tasks) {
      if (getDuration(task) <= 0)
        continue;
      // this is a finished map task; calculate processing rate
      addSample(task);
    }
  }

  private void addSample(TaskProfile task) {
    Double duration = getDuration(task).doubleValue();
    addSample(task.getType() == TaskType.MAP ? MAP_DURATION : REDUCE_DURATION, duration);
  }
}
