package org.apache.hadoop.tools.posum.simulation.predictor.stats;

public interface PredictionStatEntry<T extends PredictionStatEntry> {
  long getSampleSize();

  T copy();

  T merge(T otherEntry);

  String serialize();

  T deserialize(String s);
}
