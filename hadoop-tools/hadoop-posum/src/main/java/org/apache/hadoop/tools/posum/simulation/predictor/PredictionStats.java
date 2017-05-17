package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.Map;
import java.util.Queue;

public abstract class PredictionStats {
  private int maxHistory;
  private int sampleSize = 0;
  private int relevance;

  public PredictionStats(int maxHistory, int relevance) {
    this.maxHistory = maxHistory;
    this.relevance = relevance;
  }

  public int getSampleSize() {
    return sampleSize;
  }

  public void incrementSampleSize() {
    if (sampleSize < maxHistory)
      sampleSize++;
  }

  public int getRelevance() {
    return relevance;
  }

  public void setRelevance(int relevance) {
    this.relevance = relevance;
  }

  public Double addValue(String valueString, Double previousAverage, Queue<Double> values) {
    if (valueString == null)
      return previousAverage;
    double value = Double.valueOf(valueString);
    return addValue(value, previousAverage, values);
  }

  public Double addValue(Double value, Double previousAverage, Queue<Double> values) {
    if(value == null)
      return previousAverage;
    double total = previousAverage == null ? 0 : previousAverage * values.size();
    total += value;
    if (values.size() >= maxHistory) {
      total -= values.remove();
    }
    values.add(value);
    return total / values.size();
  }

  public abstract void updateStatsFromFlexFields(Map<String, String> flexFields);

  public abstract void addSource(JobProfile job);

}
