package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMapPredictionStats;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_LOCAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_REMOTE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;

class DetailedMapPredictionStats extends SimpleMapPredictionStats {

  DetailedMapPredictionStats(int relevance) {
    super(relevance, MAP_DURATION, MAP_RATE, MAP_LOCAL_RATE, MAP_REMOTE_RATE, MAP_SELECTIVITY);
  }

  public void addSample(JobProfile job, TaskProfile task) {
    Double duration = getDuration(task).doubleValue();
    addSample(MAP_DURATION, duration);

    Long splitSize = getSplitSize(task, job);
    if (splitSize == null)
      return;

    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double boundedSplitSize = Math.max(splitSize, 1.0);
    addSample(MAP_SELECTIVITY, orZero(task.getOutputBytes()) / boundedSplitSize);

    Double rate = boundedSplitSize / duration;
    addSample(MAP_RATE, rate);
    if (task.isLocal() != null)
      addSample(task.isLocal() ? MAP_LOCAL_RATE : MAP_REMOTE_RATE, rate);
  }
}
