package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionProfile;

import java.util.HashMap;
import java.util.Map;

public class BasicPredictionProfile extends PredictionProfile {
  private BasicPredictionStats stats;

  public BasicPredictionProfile(JobProfile job, BasicPredictionStats newStats) {
    super(job);
    this.stats = newStats;
  }

  @Override
  public Map<String, String> serialize() {
    Map<String, String> fields = new HashMap<>();
    if (stats != null)
      fields.putAll(stats.serialize());
    return fields;
  }

  @Override
  public void deserialize() {
    stats.deserialize(getJob().getFlexFields());
  }

  public BasicPredictionStats getStats() {
    return stats;
  }
}