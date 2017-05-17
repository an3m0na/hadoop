package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionModel;

class DetailedPredictionModel extends SimpleMRPredictionModel<DetailedMapPredictionStats, DetailedReducePredictionStats> {

  DetailedPredictionModel(int historyBuffer) {
    super(DetailedMapPredictionStats.class, DetailedReducePredictionStats.class, historyBuffer);
  }

  @Override
  protected DetailedMapPredictionStats newMapStats(int historyBuffer, int relevance) {
    return new DetailedMapPredictionStats(historyBuffer, relevance);
  }

  @Override
  protected DetailedReducePredictionStats newReduceStats(int historyBuffer, int relevance) {
    return new DetailedReducePredictionStats(historyBuffer, relevance);
  }
}
