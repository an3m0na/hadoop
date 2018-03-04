package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class RegressionWithFallbackStatEntry implements AveragingStatEntry<RegressionWithFallbackStatEntry> {
  private AveragingStatEntryImpl average;
  private AveragingStatEntryImpl simpleSlope;
  private RegressionStatEntry regression;

  public RegressionWithFallbackStatEntry(Double average, long sampleSize) {
    this.average = new AveragingStatEntryImpl(average, sampleSize);
    this.regression = new RegressionStatEntry();
    this.simpleSlope = new AveragingStatEntryImpl();
  }

  public RegressionWithFallbackStatEntry() {
    regression = new RegressionStatEntry();
    average = new AveragingStatEntryImpl();
    simpleSlope = new AveragingStatEntryImpl();
  }

  public Double getAverage() {
    return average.getAverage();
  }

  public Double getSimpleSlope() {
    return simpleSlope.getAverage();
  }

  @Override
  public void addSample(double y) {
    average.addSample(y);
  }

  public void addSample(double x, double y) {
    regression.addData(x, y);
    simpleSlope.addSample(x / y);
    addSample(y);
  }

  public Double predict(double x) {
    Double prediction = regression.predict(x);
    if (!prediction.isNaN())
      return prediction;
    Double averageSlope = simpleSlope.getAverage();
    return averageSlope == null ? null : x / averageSlope;
  }

  public SimpleRegression getRegression() {
    return regression;
  }

  @Override
  public long getSampleSize() {
    return average.getSampleSize();
  }

  @Override
  public RegressionWithFallbackStatEntry copy() {
    RegressionWithFallbackStatEntry newEntry = new RegressionWithFallbackStatEntry();
    newEntry.average = this.average.copy();
    newEntry.simpleSlope = this.simpleSlope.copy();
    newEntry.regression = this.regression.copy();
    return newEntry;
  }

  @Override
  public RegressionWithFallbackStatEntry merge(RegressionWithFallbackStatEntry otherEntry) {
    average.merge(otherEntry.average);
    simpleSlope.merge(otherEntry.simpleSlope);
    regression.merge(otherEntry.regression);
    return this;
  }

  @Override
  public String serialize() {
    return average.serialize() + ";" + simpleSlope.serialize() + ";" + regression.serialize();
  }

  @Override
  public RegressionWithFallbackStatEntry deserialize(String serialzedForm) {
    String[] parts = serialzedForm.split(";");
    average.deserialize(parts[0]);
    simpleSlope.deserialize(parts[1]);
    regression.deserialize(parts[2]);
    return this;
  }
}
