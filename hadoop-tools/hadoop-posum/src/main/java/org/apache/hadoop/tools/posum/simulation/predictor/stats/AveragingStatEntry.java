package org.apache.hadoop.tools.posum.simulation.predictor.stats;


public interface AveragingStatEntry<T extends PredictionStatEntry<T>> extends PredictionStatEntry<T> {
  Double getAverage();

  void addSample(double sample);

}
