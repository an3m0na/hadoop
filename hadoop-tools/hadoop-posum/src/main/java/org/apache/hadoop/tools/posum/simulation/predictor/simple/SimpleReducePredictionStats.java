package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class SimpleReducePredictionStats extends PredictionStats {
  private Double avgReduceDuration;
  private Queue<Double> reduceDurations;
  private Double avgReduceRate;
  private Queue<Double> reduceRates;
  private final String rateKey;

  public SimpleReducePredictionStats(int maxHistory, int relevance, String rateKey) {
    super(maxHistory, relevance);
    this.reduceDurations = new ArrayBlockingQueue<>(maxHistory);
    this.reduceRates = new ArrayBlockingQueue<>(maxHistory);
    this.rateKey = rateKey;
  }

  public Double getAvgReduceRate() {
    return avgReduceRate;
  }

  public void setAvgReduceRate(Double avgReduceRates) {
    this.avgReduceRate = avgReduceRates;
  }

  public Queue<Double> getReduceRates() {
    return reduceRates;
  }

  public Double getAvgReduceDuration() {
    return avgReduceDuration;
  }

  public void setAvgReduceDuration(Double avgReduceDuration) {
    this.avgReduceDuration = avgReduceDuration;
  }

  public Queue<Double> getReduceDurations() {
    return reduceDurations;
  }

  public void updateStatsFromFlexFields(Map<String, String> flexFields) {
    avgReduceRate = addValue(
      flexFields.get(rateKey),
      avgReduceRate,
      reduceRates
    );
  }

  public void addSource(JobProfile job) {
    updateStatsFromFlexFields(job.getFlexFields());
    job.getAvgReduceDuration();
    Long avgDuration = job.getAvgReduceDuration();
    if (avgDuration != null)
      avgReduceDuration = addValue(avgDuration.doubleValue(), avgReduceDuration, reduceDurations);
    incrementSampleSize();
  }

}
