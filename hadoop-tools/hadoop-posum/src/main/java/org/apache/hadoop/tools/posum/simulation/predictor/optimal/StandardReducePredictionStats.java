package org.apache.hadoop.tools.posum.simulation.predictor.optimal;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleReducePredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.REDUCE_RATE;


class OptimalReducePredictionStats extends SimpleReducePredictionStats {

  OptimalReducePredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance, REDUCE_RATE.getKey());
  }

}
