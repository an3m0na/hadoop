package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class RegressionWithAverageStatEntry implements PredictionStatEntry<RegressionWithAverageStatEntry> {
  private AveragingStatEntry averagingStatEntry;
  private RegressionStatEntry regressionStatEntry;

  public RegressionWithAverageStatEntry(Double average, long sampleSize) {
    regressionStatEntry = new RegressionStatEntry();
    averagingStatEntry = new AveragingStatEntry(average, sampleSize);
  }

  public RegressionWithAverageStatEntry() {
    regressionStatEntry = new RegressionStatEntry();
    averagingStatEntry = new AveragingStatEntry();
  }

  public Double getAverage() {
    return averagingStatEntry.getAverage();
  }

  public void addData(double y) {
    averagingStatEntry.addSample(y);
  }

  public void addData(double x, double y) {
    regressionStatEntry.addData(x, y);
    addData(y);
  }

  public Double predict(double x) {
    Double prediction = regressionStatEntry.predict(x);
    return prediction.isNaN() ? null : prediction;
  }

  public SimpleRegression getRegression() {
    return regressionStatEntry;
  }

  @Override
  public long getSampleSize() {
    return averagingStatEntry.getSampleSize();
  }

  @Override
  public RegressionWithAverageStatEntry copy() {
    RegressionWithAverageStatEntry newEntry = new RegressionWithAverageStatEntry(averagingStatEntry.getAverage(), averagingStatEntry.getSampleSize());
    newEntry.regressionStatEntry.append(regressionStatEntry);
    return newEntry;
  }

  @Override
  public RegressionWithAverageStatEntry merge(RegressionWithAverageStatEntry otherEntry) {
    averagingStatEntry.merge(otherEntry.averagingStatEntry);
    regressionStatEntry.merge(otherEntry.regressionStatEntry);
    return this;
  }

  @Override
  public String serialize() {
    return averagingStatEntry.serialize() + ";" + regressionStatEntry.serialize();
  }

  @Override
  public RegressionWithAverageStatEntry deserialize(String serialzedForm) {
    String[] parts = serialzedForm.split(";");
    averagingStatEntry.deserialize(parts[0]);
    regressionStatEntry.deserialize(parts[1]);
    return this;
  }
}
