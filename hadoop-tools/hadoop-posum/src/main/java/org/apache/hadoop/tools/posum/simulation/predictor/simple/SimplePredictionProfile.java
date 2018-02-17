package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStatEntry;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.HashMap;
import java.util.Map;

public class SimplePredictionProfile<S extends PredictionStats<E>, E extends PredictionStatEntry<E>> extends PredictionProfile {
  private S stats;
  private boolean updated = false;

  public SimplePredictionProfile(JobProfile job, S stats) {
    super(job);
    this.stats = stats;
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

  @Override
  public boolean isUpdated() {
    return updated;
  }

  @Override
  public void markUpdated() {
    updated = true;
  }

  public S getStats() {
    return stats;
  }
}