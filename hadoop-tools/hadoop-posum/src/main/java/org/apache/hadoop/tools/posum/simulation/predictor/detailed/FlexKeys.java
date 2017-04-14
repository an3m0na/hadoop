package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

enum FlexKeys {
  PROFILED_MAPS,
  PROFILED_REDUCES,
  MAP_GENERAL,
  MAP_REMOTE,
  MAP_LOCAL,
  MAP_SELECTIVITY,
  SHUFFLE_FIRST,
  SHUFFLE_TYPICAL,
  MERGE, REDUCE;

  public String getKey() {
    return DetailedPredictor.class.getSimpleName() + "::" + this.name();
  }
}
