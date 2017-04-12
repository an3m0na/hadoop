package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

public class Common {

  enum FlexKeys {
    PROFILED,
    PROFILED_MAPS,
    PROFILED_REDUCES,
    MAP_GENERAL,
    MAP_REMOTE,
    MAP_LOCAL,
    MAP_SELECTIVITY,
    SHUFFLE_FIRST,
    SHUFFLE_TYPICAL,
    MERGE, REDUCE
  }

  static final String FLEX_KEY_PREFIX = DetailedPredictor.class.getSimpleName() + "::";

}
