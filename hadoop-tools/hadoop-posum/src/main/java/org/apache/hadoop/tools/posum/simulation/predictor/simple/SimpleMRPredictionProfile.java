package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.HashMap;
import java.util.Map;

public class SimpleMRPredictionProfile<M extends PredictionStats, R extends PredictionStats> extends PredictionProfile {
  private M mapStats;
  private R reduceStats;

  public SimpleMRPredictionProfile(JobProfile job, M newMapStats, R newReduceStats) {
    super(job);
    this.mapStats = newMapStats;
    this.reduceStats = newReduceStats;
  }

  @Override
  public Map<String, String> serialize() {
    Map<String, String> fields = new HashMap<>();
    if (mapStats != null)
      fields.putAll(mapStats.serialize());
    if (reduceStats != null)
      fields.putAll(reduceStats.serialize());
    return fields;
  }

  @Override
  public void deserialize() {
    mapStats.deserialize(getJob().getFlexFields());
    reduceStats.deserialize(getJob().getFlexFields());
  }

  public M getMapStats() {
    return mapStats;
  }

  public R getReduceStats() {
    return reduceStats;
  }
}