package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleReducePredictionStats;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.REDUCE_RATE;

class StandardReducePredictionStats extends SimpleReducePredictionStats {

  StandardReducePredictionStats(int relevance) {
    super(relevance, REDUCE_DURATION, REDUCE_RATE);
  }

  public void addSample(JobProfile job, TaskProfile task) {
    Double duration = getDuration(task).doubleValue();
    addSample(REDUCE_DURATION, duration);
    if (task.getInputBytes() != null) {
      // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
      addSample(REDUCE_RATE, Math.max(task.getInputBytes(), 1.0) / duration);
    }
  }
}
