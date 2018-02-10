package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMapPredictionStats;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_SELECTIVITY;

class StandardMapPredictionStats extends SimpleMapPredictionStats {

  StandardMapPredictionStats(int relevance) {
    super(relevance, MAP_DURATION, MAP_RATE, MAP_SELECTIVITY);
  }

  public void addSample(JobProfile job, TaskProfile task) {
    Double duration = getDuration(task).doubleValue();
    addSample(MAP_DURATION, duration);

    Long splitSize = getSplitSize(task, job);
    if (splitSize == null)
      return;

    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double boundedSplitSize = Math.max(splitSize, 1.0);
    addSample(MAP_RATE, boundedSplitSize / duration);
    addSample(MAP_SELECTIVITY, orZero(task.getOutputBytes()) / boundedSplitSize);
  }
}
