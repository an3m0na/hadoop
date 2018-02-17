package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStatEntry;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;

public class SimpleMRPredictionProfile<
  M extends PredictionStats<E>,
  R extends PredictionStats<E>,
  E extends PredictionStatEntry<E>> extends SimplePredictionProfile<M, E> {
  private R reduceStats;

  public SimpleMRPredictionProfile(JobProfile job, M newMapStats, R newReduceStats) {
    super(job, newMapStats);
    this.reduceStats = newReduceStats;
  }

  @Override
  public Map<String, String> serialize() {
    Map<String, String> fields = super.serialize();
    if (reduceStats != null)
      fields.putAll(reduceStats.serialize());
    return fields;
  }

  @Override
  public void deserialize() {
    super.deserialize();
    reduceStats.deserialize(getJob().getFlexFields());
  }

  public M getMapStats() {
    return super.getStats();
  }

  public R getReduceStats() {
    return reduceStats;
  }

  @Override
  public M getStats() {
    throw new UnsupportedOperationException();
  }
}