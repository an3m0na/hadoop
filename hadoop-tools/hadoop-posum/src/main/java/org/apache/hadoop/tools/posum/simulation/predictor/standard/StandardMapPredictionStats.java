package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMapPredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.MAP_SELECTIVITY;

class StandardMapPredictionStats extends SimpleMapPredictionStats {

  StandardMapPredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance, MAP_RATE.getKey(), MAP_SELECTIVITY.getKey());
  }

}
