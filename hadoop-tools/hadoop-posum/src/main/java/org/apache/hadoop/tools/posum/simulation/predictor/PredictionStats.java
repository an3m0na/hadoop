package org.apache.hadoop.tools.posum.simulation.predictor;

import java.util.HashMap;
import java.util.Map;

public abstract class PredictionStats<E extends PredictionStatEntry<E>> {

  private int relevance;
  private String flexPrefix;
  private Enum[] statKeys;
  protected Map<Enum, E> entries = new HashMap<>();

  public PredictionStats(int relevance, Enum... statKeys) {
    this.relevance = relevance;
    this.statKeys = statKeys;
    this.flexPrefix = getClass().getSimpleName() + "::";
  }

  public int getSampleSize(Enum key) {
    E entry = entries.get(key);
    return entry == null ? 0 : entry.getSampleSize();
  }

  public E getEntry(Enum key) {
    return entries.get(key);
  }

  public int getRelevance() {
    return relevance;
  }

  public void setRelevance(int relevance) {
    this.relevance = relevance;
  }

  protected void addEntry(Enum key, E entry) {
    if (entry == null || entry.getSampleSize() == 0)
      return;
    E oldEntry = entries.get(key);
    entries.put(key, oldEntry == null ? entry.copy() : oldEntry.copy().merge(entry));
  }

  public void merge(PredictionStats<E> otherStats) {
    if (otherStats == null)
      return;
    if (!getClass().isAssignableFrom(otherStats.getClass()))
      throw new UnsupportedOperationException();
    for (Enum statKey : statKeys) {
      addEntry(statKey, otherStats.getEntry(statKey));
    }
  }

  public Map<String, String> serialize() {
    Map<String, String> propertyMap = new HashMap<>();
    for (Map.Entry<Enum, E> entry : entries.entrySet()) {
      propertyMap.put(asFlexKey(entry.getKey()), entry.getValue().serialize());
    }
    return propertyMap;
  }

  public void deserialize(Map<String, String> flexFields) {
    for (Enum statKey : statKeys) {
      entries.put(statKey, emptyEntry().deserialize(flexFields.get(asFlexKey(statKey))));
    }
  }

  protected abstract E emptyEntry();

  private String asFlexKey(Enum statKey) {
    return flexPrefix + statKey.name();
  }
}
