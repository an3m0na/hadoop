package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.Map;
import java.util.Queue;

abstract class DetailedPredictionStats {
  protected int maxHistory;
  protected int sampleSize = 0;
  protected int relevance;

  public DetailedPredictionStats(int maxHistory, int relevance) {
    this.maxHistory = maxHistory;
    this.relevance = relevance;
  }

  public int getSampleSize() {
    return sampleSize;
  }

  public int getRelevance() {
    return relevance;
  }

  public void setRelevance(int relevance) {
    this.relevance = relevance;
  }

  protected Double addValue(String valueString, Double previousAverage, Queue<Double> values) {
    if (valueString == null)
      return previousAverage;
    double value = Double.valueOf(valueString);
    return addValue(value, previousAverage, values);
  }

  protected Double addValue(Double value, Double previousAverage, Queue<Double> values) {
    double total = previousAverage == null ? 0 : previousAverage;
    total += value;
    if (values.size() == maxHistory) {
      total -= values.remove();
    }
    values.add(value);
    return total / values.size();
  }

  protected abstract void updateStatsFromFlexFields(Map<String, String> flexFields);

  protected abstract void addSource(JobProfile job);

  protected abstract boolean isIncomplete();
}
