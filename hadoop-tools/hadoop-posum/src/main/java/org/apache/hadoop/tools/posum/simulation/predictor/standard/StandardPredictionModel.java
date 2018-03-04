package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionModel;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.AveragingStatEntryImpl;

class StandardPredictionModel extends SimpleMRPredictionModel<
  StandardMapPredictionStats,
  StandardReducePredictionStats,
    AveragingStatEntryImpl,
  StandardPredictionProfile> {

  StandardPredictionModel(int historyBuffer) {
    super(historyBuffer);
  }

  @Override
  protected StandardMapPredictionStats newMapStats(int relevance) {
    return new StandardMapPredictionStats(relevance);
  }

  @Override
  protected StandardReducePredictionStats newReduceStats(int relevance) {
    return new StandardReducePredictionStats(relevance);
  }
}
