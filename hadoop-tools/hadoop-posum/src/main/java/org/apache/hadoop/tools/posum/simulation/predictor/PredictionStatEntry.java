package org.apache.hadoop.tools.posum.simulation.predictor;

public interface PredictionStatEntry<T extends PredictionStatEntry> {
  int getSampleSize();

  T copy();

  T merge(T otherEntry);

  String serialize();

  T deserialize(String s);
}
