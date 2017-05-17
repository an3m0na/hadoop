package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionModel;

class StandardPredictionModel extends SimpleMRPredictionModel<StandardMapPredictionStats, StandardReducePredictionStats> {

  StandardPredictionModel(int historyBuffer) {
    super(StandardMapPredictionStats.class, StandardReducePredictionStats.class, historyBuffer);
  }

  @Override
  protected StandardMapPredictionStats newMapStats(int historyBuffer, int relevance) {
    return new StandardMapPredictionStats(historyBuffer, relevance);
  }

  @Override
  protected StandardReducePredictionStats newReduceStats(int historyBuffer, int relevance) {
    return new StandardReducePredictionStats(historyBuffer, relevance);
  }
}
