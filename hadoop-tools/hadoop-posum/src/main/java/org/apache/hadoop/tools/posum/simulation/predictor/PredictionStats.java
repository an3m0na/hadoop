package org.apache.hadoop.tools.posum.simulation.predictor;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDouble;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getInteger;

public abstract class PredictionStats {
  class StatEntry {
    private Double average;
    private int sampleSize;

    StatEntry(Double average, int sampleSize) {
      this.average = average;
      this.sampleSize = sampleSize;
    }

    int getSampleSize() {
      return sampleSize;
    }

    Double getAverage() {
      return average;
    }

    void addSample(Double sample) {
      average = (average * sampleSize + sample) / (++sampleSize);
    }
  }

  private int relevance;
  private String flexPrefix;
  private Enum[] statKeys;
  protected Map<Enum, StatEntry> averages = new HashMap<>();

  public PredictionStats(int relevance, Enum... statKeys) {
    this.relevance = relevance;
    this.statKeys = statKeys;
    this.flexPrefix = getClass().getSimpleName() + "::";
  }

  public int getSampleSize(Enum key) {
    StatEntry entry = averages.get(key);
    return entry == null ? 0 : entry.getSampleSize();
  }

  public Double getAverage(Enum key) {
    StatEntry entry = averages.get(key);
    return entry == null ? null : entry.getAverage();
  }

  public int getRelevance() {
    return relevance;
  }

  public void setRelevance(int relevance) {
    this.relevance = relevance;
  }

  protected void addSample(Enum key, Double value) {
    if (value == null)
      return;
    StatEntry entry = averages.get(key);
    if (entry == null) {
      entry = new StatEntry(value, 1);
      averages.put(key, entry);
    } else {
      entry.addSample(value);
    }
  }

  public void merge(PredictionStats otherStats) {
    if (otherStats == null)
      return;
    if (!getClass().isAssignableFrom(otherStats.getClass()))
      throw new UnsupportedOperationException();
    for (Enum statKey : statKeys) {
      StatEntry thisEntry = averages.get(statKey);
      StatEntry otherEntry = otherStats.averages.get(statKey);
      if (otherEntry != null && otherEntry.getAverage() != null) {
        if (thisEntry == null || thisEntry.getAverage() == null) {
          averages.put(statKey, new StatEntry(otherEntry.getAverage(), otherEntry.getSampleSize()));
        } else {
          int newSampleSize = thisEntry.getSampleSize() + otherEntry.getSampleSize();
          Double newAverage = (thisEntry.getAverage() * thisEntry.getSampleSize() +
            otherEntry.getAverage() * otherEntry.getSampleSize()) / newSampleSize;
          averages.put(statKey, new StatEntry(newAverage, newSampleSize));
        }
      }
    }
  }

  public Map<String, String> serialize() {
    Map<String, String> propertyMap = new HashMap<>();
    for (Map.Entry<Enum, StatEntry> entry : averages.entrySet()) {
      propertyMap.put(asFlexKey(entry.getKey()), String.valueOf(entry.getValue().getAverage()));
      propertyMap.put(asSamplesFlexKey(entry.getKey()), String.valueOf(entry.getValue().getSampleSize()));
    }
    return propertyMap;
  }

  public void deserialize(Map<String, String> flexFields) {
    for (Enum statKey : statKeys) {
      Double average = getDouble(flexFields, asFlexKey(statKey), null);
      int sampleSize = getInteger(flexFields, asSamplesFlexKey(statKey), 0);
      if (average != null)
        averages.put(statKey, new StatEntry(average, sampleSize));
    }
  }

  protected String asFlexKey(Enum statKey) {
    return flexPrefix + statKey.name();
  }

  protected String asSamplesFlexKey(Enum statKey) {
    return asFlexKey(statKey) + "::samples";
  }
}
