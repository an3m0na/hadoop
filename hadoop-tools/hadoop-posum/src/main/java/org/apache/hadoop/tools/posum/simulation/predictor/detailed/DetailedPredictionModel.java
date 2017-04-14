package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.simulation.predictor.SimpleMRPredictionModel;

class DetailedPredictionModel extends SimpleMRPredictionModel<DetailedMapPredictionStats, DetailedReducePredictionStats> {

  DetailedPredictionModel(int historyBuffer) {
    super(DetailedMapPredictionStats.class, DetailedReducePredictionStats.class, historyBuffer);
  }

}
