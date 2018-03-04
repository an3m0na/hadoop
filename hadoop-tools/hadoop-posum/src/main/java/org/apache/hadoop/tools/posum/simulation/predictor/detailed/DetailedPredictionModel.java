package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionModel;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.RegressionWithFallbackStatEntry;

class DetailedPredictionModel extends SimpleMRPredictionModel<
  DetailedMapPredictionStats,
  DetailedReducePredictionStats,
    RegressionWithFallbackStatEntry,
  DetailedPredictionProfile> {

  DetailedPredictionModel(int historyBuffer) {
    super(historyBuffer);
  }

  @Override
  protected DetailedMapPredictionStats newMapStats(int relevance) {
    return new DetailedMapPredictionStats(relevance);
  }

  @Override
  protected DetailedReducePredictionStats newReduceStats(int relevance) {
    return new DetailedReducePredictionStats(relevance);
  }
}