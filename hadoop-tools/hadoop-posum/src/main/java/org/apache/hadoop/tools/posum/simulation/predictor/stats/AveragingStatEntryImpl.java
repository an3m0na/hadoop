package org.apache.hadoop.tools.posum.simulation.predictor.stats;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;

public class AveragingStatEntryImpl implements AveragingStatEntry<AveragingStatEntryImpl> {
  private Double average;
  private long sampleSize;

  public AveragingStatEntryImpl() {
  }

  public AveragingStatEntryImpl(Double average, long sampleSize) {
    this.average = average;
    this.sampleSize = sampleSize;
  }

  public AveragingStatEntryImpl(Long average, long sampleSize) {
    if (average != null && sampleSize > 0) {
      this.average = average.doubleValue();
      this.sampleSize = sampleSize;
    }
  }

  public Double getAverage() {
    return average;
  }

  public void addSample(double sample) {
    average = (orZero(average) * sampleSize + sample) / (++sampleSize);
  }

  @Override
  public long getSampleSize() {
    return sampleSize;
  }

  @Override
  public AveragingStatEntryImpl copy() {
    return new AveragingStatEntryImpl(average, sampleSize);
  }

  @Override
  public AveragingStatEntryImpl merge(AveragingStatEntryImpl otherEntry) {
    long newSampleSize = sampleSize + otherEntry.sampleSize;
    average = (orZero(average) * sampleSize + orZero(otherEntry.average) * otherEntry.sampleSize) / newSampleSize;
    sampleSize = newSampleSize;
    return this;
  }

  @Override
  public String serialize() {
    return sampleSize + "=" + average;
  }

  @Override
  public AveragingStatEntryImpl deserialize(String serializedForm) {
    if (serializedForm == null)
      return null;
    String[] parts = serializedForm.trim().split("=");
    sampleSize = Integer.valueOf(parts[0]);
    average = parts.length < 2 || parts[1].equals("null") ? null : Double.valueOf(parts[1]);
    return this;
  }
}
