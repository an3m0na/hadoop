package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleReducePredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.REDUCE_RATE;

class StandardReducePredictionStats extends SimpleReducePredictionStats {

  StandardReducePredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance, REDUCE_RATE.getKey());
  }

}
