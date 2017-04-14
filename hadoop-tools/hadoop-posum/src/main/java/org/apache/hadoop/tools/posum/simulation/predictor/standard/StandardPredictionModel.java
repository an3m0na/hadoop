package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.simulation.predictor.SimpleMRPredictionModel;

class StandardPredictionModel extends SimpleMRPredictionModel<StandardMapPredictionStats, StandardReducePredictionStats> {

  StandardPredictionModel(int historyBuffer) {
    super(StandardMapPredictionStats.class, StandardReducePredictionStats.class, historyBuffer);
  }

}
