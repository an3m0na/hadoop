package org.apache.hadoop.tools.posum.simulation.predictor.standard;

enum FlexKeys {
  PROFILED_MAPS,
  PROFILED_REDUCES,
  MAP_RATE,
  MAP_SELECTIVITY,
  REDUCE_RATE;

  public String getKey() {
    return StandardPredictor.class.getSimpleName() + "::" + this.name();
  }
}
