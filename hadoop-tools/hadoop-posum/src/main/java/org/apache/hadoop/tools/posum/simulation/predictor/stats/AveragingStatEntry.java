package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;

public class AveragingStatEntry implements PredictionStatEntry<AveragingStatEntry> {
  private Double average;
  private int sampleSize;

  public AveragingStatEntry() {
  }

  public AveragingStatEntry(Double average, int sampleSize) {
    this.average = average;
    this.sampleSize = sampleSize;
  }

  public AveragingStatEntry(Long average, int sampleSize) {
    if (average != null && sampleSize > 0) {
      this.average = average.doubleValue();
      this.sampleSize = sampleSize;
    }
  }

  public Double getAverage() {
    return average;
  }

  public void addSample(Double sample) {
    average = (orZero(average) * sampleSize + sample) / (++sampleSize);
  }

  @Override
  public long getSampleSize() {
    return sampleSize;
  }

  @Override
  public AveragingStatEntry copy() {
    return new AveragingStatEntry(average, sampleSize);
  }

  @Override
  public AveragingStatEntry merge(AveragingStatEntry otherEntry) {
    int newSampleSize = sampleSize + otherEntry.sampleSize;
    average = (average * sampleSize + otherEntry.average * otherEntry.sampleSize) / newSampleSize;
    sampleSize = newSampleSize;
    return this;
  }

  @Override
  public String serialize() {
    return sampleSize + "=" + average;
  }

  @Override
  public AveragingStatEntry deserialize(String serializedForm) {
    if (serializedForm == null)
      return null;
    String[] parts = serializedForm.trim().split("=");
    sampleSize = Integer.valueOf(parts[0]);
    average = parts.length < 2 || parts[1].equals("null") ? null : Double.valueOf(parts[1]);
    return this;
  }
}
