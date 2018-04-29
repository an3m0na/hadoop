package org.apache.hadoop.tools.posum.simulation.predictor.optimal;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMapPredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.MAP_SELECTIVITY;


class OptimalMapPredictionStats extends SimpleMapPredictionStats {

  OptimalMapPredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance, MAP_RATE.getKey(), MAP_SELECTIVITY.getKey());
  }

}
