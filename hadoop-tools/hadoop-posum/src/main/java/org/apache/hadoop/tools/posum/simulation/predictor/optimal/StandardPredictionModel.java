package org.apache.hadoop.tools.posum.simulation.predictor.optimal;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionModel;

class OptimalPredictionModel extends SimpleMRPredictionModel<OptimalMapPredictionStats, OptimalReducePredictionStats> {

  OptimalPredictionModel(int historyBuffer) {
    super(OptimalMapPredictionStats.class, OptimalReducePredictionStats.class, historyBuffer);
  }

  @Override
  protected OptimalMapPredictionStats newMapStats(int historyBuffer, int relevance) {
    return new OptimalMapPredictionStats(historyBuffer, relevance);
  }

  @Override
  protected OptimalReducePredictionStats newReduceStats(int historyBuffer, int relevance) {
    return new OptimalReducePredictionStats(historyBuffer, relevance);
  }
}
